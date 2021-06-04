package arq_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/sholiday/arq"
	"github.com/stretchr/testify/assert"
)

func TestDecryptObject(t *testing.T) {
	const passphrase = "hunter2"
	ctx := context.Background()
	file, err := os.Open("testdata/crypt/encryptionv3.dat.bin")
	if !assert.Nil(t, err) {
		return
	}
	enc, err := arq.Unlock(ctx, file, passphrase)
	if !assert.Nil(t, err) {
		return
	}
	t.Run("Object0", func(t *testing.T) {
		by, err := ioutil.ReadFile("testdata/crypt/object.0.bin")
		if !assert.Nil(t, err) {
			return
		}
		assert.Equal(t, 1316, len(by))

		br := bytes.NewReader(by)
		eor := arq.NewEObjectReader(br, enc)
		read, err := io.ReadAll(eor)
		assert.Nil(t, err)
		assert.Equal(t, 1198, len(read))
		assert.Equal(t, []byte("CommitV012"), read[:10])
		assert.Equal(t, []byte("</plist>\000"), read[len(read)-9:])
	})
	t.Run("Object1", func(t *testing.T) {
		by, err := ioutil.ReadFile("testdata/crypt/object.1.bin")
		if !assert.Nil(t, err) {
			return
		}
		assert.Equal(t, 372, len(by))

		br := bytes.NewReader(by)
		eor := arq.NewEObjectReader(br, enc)
		read, err := io.ReadAll(eor)
		assert.Nil(t, err)
		assert.Equal(t, 256-15, len(read))
	})
}

func TestPaddedReader(t *testing.T) {
	testCases := []struct {
		name      string
		in        []byte
		exptected []byte
	}{
		{"2x1",
			[]byte{0xAA, 0xAB, 0xAC, 1},
			[]byte{0xAA, 0xAB, 0xAC}},
		{"2x2",
			[]byte{0xAA, 0xAB, 2, 2},
			[]byte{0xAA, 0xAB}},
		{"2x0",
			[]byte{0xAA, 0xAB, 1, 2},
			[]byte{0xAA, 0xAB, 1, 2}},
		{"4x1",
			[]byte{0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xBA, 1},
			[]byte{0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xBA}},
		{"4x0",
			[]byte{0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 1, 2},
			[]byte{0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 1, 2}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := arq.NewPaddedReader(bytes.NewReader(tc.in), 2)
			out, err := io.ReadAll(r)
			assert.Nil(t, err)
			assert.Equal(t, tc.exptected, out)
		})
	}
}
