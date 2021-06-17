package arq

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

type CompressionType int32

func (ct CompressionType) String() string {
	switch ct {
	case 0:
		return "None"
	case 1:
		return "Gzip"
	case 2:
		return "LZ4"
	default:
		return "INVALID"
	}
}

const (
	NoneCompression CompressionType = 0
	GzipCompression CompressionType = 1
	Lz4Compression  CompressionType = 2
)

type ShaHash struct {
	Contents [20]byte
}

func (sh ShaHash) String() string {
	return hex.EncodeToString(sh.Contents[:])
}

func DecodeShaHashString(in string) (ShaHash, error) {
	var sh ShaHash
	if l := hex.DecodedLen(len(in)); l != 20 {
		return sh, fmt.Errorf("invalid ShaHash length %d, expected 20", l)
	}
	buf, err := hex.DecodeString(in)
	if err != nil {
		return sh, err
	}
	copy(sh.Contents[:], buf[:20])
	return sh, nil
}

func DecodeShaHash(in []byte) (ShaHash, error) {
	var sh ShaHash
	n, err := hex.Decode(sh.Contents[:], in[:40])
	if err != nil {
		return sh, err
	}
	if n != len(sh.Contents) {
		return sh, fmt.Errorf("didn't decode enough bytes")
	}
	return sh, err
}

func WrapShaHash(in *[20]byte) ShaHash {
	var sh ShaHash
	copy(sh.Contents[:], in[:])
	return sh
}

func (sh *ShaHash) UnmarshalArq(input io.Reader) error {
	var s string
	if err := DecodeArq(input, &s); err != nil {
		return err
	}
	if len(s) == 0 {
		for i := range sh.Contents {
			sh.Contents[i] = 0
		}
		return nil
	}
	d, err := DecodeShaHashString(s)
	if err != nil {
		return err
	}
	*sh = d
	return nil
}

type ArqTree struct {
	// 54 72 65 65 56 30 32 32             "TreeV022"
	Header                [8]byte
	XattrsCompressionType CompressionType
	AclCompressionType    CompressionType
	XattrsBlobKey         ArqBlobKey
	XattrsSize            uint64
	AclBlobKey            ArqBlobKey
	Uid                   int32
	Gid                   int32
	Mode                  int32
	Mtime                 time.Time `arq:"nsec"`
	Flags                 int64
	FinderFlags           int32
	ExtendedFinderFlags   int32
	StDev                 int32
	StIno                 int32
	StNlink               uint32
	StRdev                int32
	Ctime                 time.Time `arq:"nsec"`
	StBlocks              int64
	StBlkSize             uint32
	CreateTimeSec         int64
	CreateTimeNsec        int64
	MissingNodes          []string      `arq:"len-uint32"`
	Nodes                 []ArqTreeNode `arq:"len-uint32"`
}

type ArqBlobKey struct {
	Hash                   ShaHash
	EncryptionKeyStretched bool
	StorageType            int32
	ArchiveId              string
	ArchiveSize            uint64
	ArchiveUploadDate      time.Time
}

type ArqTreeNode struct {
	FileName string
	Node     ArqNode
}

type ArqNode struct {
	IsTree                   bool
	TreeContainsMissingItems bool
	DataCompressionType      CompressionType
	XattrsCompressionType    CompressionType
	AclCompressionType       CompressionType
	DataBlobKeys             []ArqBlobKey `arq:"len-uint32"`
	DataSize                 uint64
	XattrsBlobKey            ArqBlobKey
	XattrsSize               uint64
	AclBlobKey               ArqBlobKey
	Uid                      int32
	Gid                      int32
	Mode                     int32
	Mtime                    time.Time `arq:"nsec"`
	Flags                    int64
	FinderFlags              int32
	ExtendedFinderFlags      int32
	FinderFileType           string
	FinderFileCreator        string
	IsFileExtensionHidden    bool
	StDev                    int32
	StIno                    int32
	StNlink                  uint32
	StRdev                   int32
	Ctime                    time.Time `arq:"nsec"`
	CreateTime               time.Time `arq:"nsec"`
	StBlocks                 int64
	StBlkSize                uint32
}

type ArqPackIndex struct {
	Header  [4]byte
	Version uint32
	Fanout  [256]uint32
	Objects []ArqPackIndexObject
	SHA1    [20]byte
	// TODO(sholiday): Support Glacier metadata.
}

func (o *ArqPackIndex) UnmarshalArq(input io.Reader) error {
	h := sha1.New()
	r := io.TeeReader(input, h)

	err := DecodeArq(r, &o.Header)
	if err != nil {
		return err
	}
	if !bytes.Equal(o.Header[:], []byte{0xff, 0x74, 0x4f, 0x63}) {
		return fmt.Errorf("magic bytes '% x' are incorrect for ArqPackIndex", o.Header)
	}
	err = DecodeArq(r, &o.Version)
	if err != nil {
		return err
	}
	err = DecodeArq(r, &o.Fanout)
	if err != nil {
		return err
	}
	numObjects := int(o.Fanout[255])
	o.Objects = make([]ArqPackIndexObject, numObjects)
	for i := range o.Objects {
		err = DecodeArq(r, &o.Objects[i])
		if err != nil {
			return err
		}
		if !bytes.Equal(o.Objects[i].Alignment[:], []byte{0, 0, 0, 0}) {
			return fmt.Errorf("invalid alignment for ArqPackIndexObject")
		}
	}
	calculated := h.Sum(nil)
	err = DecodeArq(r, &o.SHA1)
	if err != nil {
		return err
	}
	if !bytes.Equal(calculated, o.SHA1[:]) {
		return fmt.Errorf("ArqPackIndex checksum '%x' doesn't match calculated '%x'", o.SHA1[:], calculated)
	}
	return nil
}

type ArqPackIndexObject struct {
	Offset    uint64
	Length    uint64
	SHA1      [20]byte
	Alignment [4]byte
}

func (o ArqPackIndexObject) String() string {
	return fmt.Sprintf("ArqPackIndexObject[%x, off=%d, len=%d]", o.SHA1, o.Offset, o.Length)
}

type ArqPack struct {
	Magic       [4]byte
	Version     uint32
	ObjectCount uint64
	Objects     []ArqPackObject
	SHA1        [20]byte
}

func (p *ArqPack) UnmarshalArq(input io.Reader) error {
	h := sha1.New()
	r := io.TeeReader(input, h)

	if err := DecodeArq(r, &p.Magic); err != nil {
		return err
	}
	if !bytes.Equal(p.Magic[:], []byte("PACK")) {
		return fmt.Errorf("magic bytes '% x' are incorrect for ArqPack", p.Magic)
	}
	if err := DecodeArq(r, &p.Version); err != nil {
		return err
	}
	if p.Version != 2 {
		return fmt.Errorf("invalid version '%d' for ArqPack", p.Version)
	}
	if err := DecodeArq(r, &p.ObjectCount); err != nil {
		return err
	}

	p.Objects = make([]ArqPackObject, p.ObjectCount)
	for i := range p.Objects {
		err := DecodeArq(r, &p.Objects[i])
		if err != nil {
			return err
		}
	}
	calculated := h.Sum(nil)
	if err := DecodeArq(r, &p.SHA1); err != nil {
		return err
	}
	if !bytes.Equal(calculated, p.SHA1[:]) {
		return fmt.Errorf("ArqPack checksum '%x' doesn't match calculated '%x'", p.SHA1[:], calculated)
	}

	return nil
}

type ArqPackObject struct {
	Mimetype string
	Name     string
	Data     []byte `arq:"len-uint64"`
}
