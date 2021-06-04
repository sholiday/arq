package arq

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"

	"golang.org/x/crypto/pbkdf2"
)

var arqEncryptionV3Header = []byte("ENCRYPTIONV2")

func Unlock(ctx context.Context, reader io.ReadCloser, passphrase string) (*encryptionV3, error) {
	defer reader.Close()
	r := bufio.NewReader(reader)
	e := encryptionV3{}

	// Decode header.
	err := binary.Read(r, binary.BigEndian, &e.header)
	if err != nil {
		return nil, err
	}
	if len(e.header) != len(arqEncryptionV3Header) {
		return nil, errors.New("unexpected encryption header size")
	}
	if !bytes.Equal(e.header[:], arqEncryptionV3Header) {
		return nil, fmt.Errorf("invalid encryption header '% x', expected '% x'", e.header, arqEncryptionV3Header)
	}

	err = binary.Read(r, binary.BigEndian, &e.salt)
	if err != nil {
		return nil, err
	}

	err = binary.Read(r, binary.BigEndian, &e.hmac)
	if err != nil {
		return nil, err
	}

	err = binary.Read(r, binary.BigEndian, &e.iv)
	if err != nil {
		return nil, err
	}

	e.encKeys, err = io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	derived := pbkdf2.Key([]byte(passphrase), e.salt[:], 200000, 64, sha1.New)
	if len(derived) != 64 {
		return nil, fmt.Errorf("failed to derive key, unexpected length %d", len(derived))
	}
	copy(e.derivedKey[:], derived)

	v, err := e.verifyHmac()
	if err != nil {
		return nil, err
	}
	if !v {
		return nil, fmt.Errorf("invalid password")
	}

	err = e.decryptKeys()
	if err != nil {
		return nil, err
	}

	return &e, nil
}

func writeAll(b []byte, w io.Writer) error {
	for i := 0; i < len(b); {
		n, err := w.Write(b[i:])
		if err != nil {
			return err
		}
		i += n
	}
	return nil
}

func (e *encryptionV3) verifyHmac() (bool, error) {
	mac := hmac.New(sha256.New, e.derivedKey[32:])
	err := writeAll(e.iv[:], mac)
	if err != nil {
		return false, err
	}
	err = writeAll(e.encKeys, mac)
	if err != nil {
		return false, err
	}
	return hmac.Equal(mac.Sum(nil), e.hmac[:]), nil
}

type encryptionV3 struct {
	// Unmodified data from the encryption file.
	header  [12]byte
	salt    [8]byte
	hmac    [32]byte
	iv      [16]byte
	encKeys []byte

	// Created using the pbkdf2 from user password and iv.
	derivedKey [64]byte
	decKeys    []byte

	key1 [32]byte
	key2 [32]byte
	key3 [32]byte
}

func (e *encryptionV3) decryptKeys() error {
	block, err := aes.NewCipher(e.derivedKey[:32])
	if err != nil {
		return err
	}
	mode := cipher.NewCBCDecrypter(block, e.iv[:])
	e.decKeys = make([]byte, len(e.encKeys))
	mode.CryptBlocks(e.decKeys, e.encKeys)

	copy(e.key1[:], e.decKeys[:32])
	copy(e.key2[:], e.decKeys[32:64])
	copy(e.key3[:], e.decKeys[64:96])
	if e.decKeys[96] != 16 {
		log.Fatal("doesn't match")
	}
	return nil
}

type eObjectReader struct {
	// Underlying reader for the encrypted object.
	ur       io.Reader
	e        *encryptionV3
	unlocked bool

	tr  io.Reader
	mac hash.Hash

	hmacValue [32]byte

	crypter cipher.BlockMode

	buf      []byte
	bufStart int
	// Number of bytes in the buffer.
	bufCount int
	eofHit   bool
}

func NewEObjectReader(ur io.Reader, e *encryptionV3) io.Reader {
	r := eObjectReader{
		ur:       ur,
		e:        e,
		unlocked: false,
	}
	return NewPaddedReader(&r, 16)
}

func consumeHeader(r io.Reader) error {
	header1 := []byte("encrypted")
	header2 := []byte("ARQO")
	var header [9]byte
	err := binary.Read(r, binary.BigEndian, header[:4])
	if err != nil {
		return err
	}
	if !bytes.Equal(header[:4], header2) {
		err = binary.Read(r, binary.BigEndian, header[4:])
		if err != nil {
			return err
		}
		if !bytes.Equal(header[:], header1) {
			return fmt.Errorf("invalid header in ARQ encrypted object '% x', not % X", header, header1)
		}
		err = binary.Read(r, binary.BigEndian, header[:4])
		if err != nil {
			return err
		}
	}
	if !bytes.Equal(header[:4], header2) {
		return fmt.Errorf("invalid header in ARQ encrypted object '% x'", header[:4])
	}
	return nil
}

func (er *eObjectReader) decryptIVAndSessionKey() error {
	// IV used to decrypt the data IV and session key.
	var masterIV [16]byte
	err := binary.Read(er.tr, binary.BigEndian, masterIV[:])
	if err != nil {
		return err
	}
	// The encrypted data IV and session key.
	// TODO(sholiday): I'm skeptical about the 64 bytes here, we actually only need 48.
	var encDataIVSessionKey [64]byte
	err = binary.Read(er.tr, binary.BigEndian, encDataIVSessionKey[:])
	if err != nil {
		return err
	}
	block, err := aes.NewCipher(er.e.key1[:])
	if err != nil {
		return fmt.Errorf("failed to use key1: %w", err)
	}
	mode := cipher.NewCBCDecrypter(block, masterIV[:])
	decDataIVSessionKey := make([]byte, len(encDataIVSessionKey))
	mode.CryptBlocks(decDataIVSessionKey, encDataIVSessionKey[:])

	dataIV := decDataIVSessionKey[:16]
	sessionKey := decDataIVSessionKey[16:48]

	block2, err := aes.NewCipher(sessionKey)
	if err != nil {
		return fmt.Errorf("failed to use session key: %w", err)
	}
	er.crypter = cipher.NewCBCDecrypter(block2, dataIV)

	return nil
}

func (er *eObjectReader) unlock() error {
	err := consumeHeader(er.ur)
	if err != nil {
		return err
	}
	// HMAC is done on everything after the header and the checksum itself.
	// From now on we shouldn't read from the underlying reader, but the tee-ing
	// reader which will also calculate the HMAC.
	err = binary.Read(er.ur, binary.BigEndian, er.hmacValue[:])
	if err != nil {
		return err
	}
	er.mac = hmac.New(sha256.New, er.e.key2[:])
	er.tr = io.TeeReader(er.ur, er.mac)

	err = er.decryptIVAndSessionKey()
	if err != nil {
		return nil
	}
	er.buf = make([]byte, er.crypter.BlockSize())
	er.bufStart = 0
	er.bufCount = 0
	er.eofHit = false
	er.unlocked = true

	return nil
}

func (er *eObjectReader) verifyHmac() bool {
	return hmac.Equal(er.mac.Sum(nil), er.hmacValue[:])
}

func (er *eObjectReader) fillBuf() error {
	if er.eofHit {
		return io.EOF
	}
	blockSize := er.crypter.BlockSize()
	_, err := io.ReadFull(er.tr, er.buf)
	if err != nil {
		return err
	}
	er.bufStart = 0
	er.bufCount = blockSize
	er.crypter.CryptBlocks(er.buf, er.buf)
	return nil
}

func (er *eObjectReader) Read(p []byte) (n int, err error) {
	if !er.unlocked {
		err := er.unlock()
		if err != nil {
			return 0, err
		}
	}

	if er.bufCount <= 0 {
		// There's nothing left in the buffer, we need to fill it and decrypt the block.
		err := er.fillBuf()
		if err == io.EOF && !er.verifyHmac() {
			return 0, fmt.Errorf("HMAC for encrypted object did not match")
		}
		if err != nil {
			return 0, err
		}
	}

	var toCopy int
	if er.bufCount > len(p) {
		toCopy = len(p)
	} else {
		toCopy = er.bufCount
	}

	copy(p, er.buf[er.bufStart:er.bufStart+er.bufCount])
	er.bufStart += toCopy
	er.bufCount -= toCopy
	return toCopy, nil
}

type PaddedReader struct {
	r          io.Reader
	bs         int
	buf        []byte
	currentBuf []byte
	nextBuf    []byte

	current []byte
	next    []byte

	eofHit bool
}

func NewPaddedReader(r io.Reader, blockSize int) *PaddedReader {
	buf := make([]byte, blockSize*2)
	return &PaddedReader{
		r:          r,
		bs:         blockSize,
		buf:        buf,
		currentBuf: buf[:blockSize],
		nextBuf:    buf[blockSize:],
		current:    make([]byte, 0),
		next:       make([]byte, 0),
		eofHit:     false,
	}
}

func (pr *PaddedReader) fillBuffer() error {
	pr.current, pr.next = pr.next, pr.current
	pr.currentBuf, pr.nextBuf = pr.nextBuf, pr.currentBuf

	if pr.eofHit {
		return nil
	}

	n, err := io.ReadFull(pr.r, pr.nextBuf)
	if err == io.EOF {
		pr.eofHit = true
	} else if err != nil {
		return err
	} else if n != pr.bs {
		return fmt.Errorf("expected a blocksize of %d, but was only able to read, %d", pr.bs, n)
	}
	pr.next = pr.nextBuf[:n]

	if !pr.eofHit {
		return nil
	}

	lastByte := pr.current[len(pr.current)-1]
	for i := 2; i <= len(pr.current) && i <= int(lastByte); i++ {
		curByte := pr.current[len(pr.current)-i]
		if curByte != lastByte {
			return nil
		}
	}
	pr.current = pr.current[:len(pr.current)-int(lastByte)]
	return nil
}

func (pr *PaddedReader) Read(dst []byte) (n int, err error) {
	for len(pr.current) == 0 && !pr.eofHit {
		err := pr.fillBuffer()
		if err != nil {
			return 0, err
		}
	}
	if len(pr.current) == 0 && pr.eofHit {
		return 0, io.EOF
	}

	copied := copy(dst, pr.current)
	pr.current = pr.current[copied:]
	return copied, nil
}
