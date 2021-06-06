package arq_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"testing"

	"github.com/sholiday/arq"
	"github.com/stretchr/testify/assert"
)

func TestDecodeSha(t *testing.T) {
	hexStr := "2d48a782b4db79027b408ef3d0276ac2d4a8b79b"
	expected, err := hex.DecodeString(hexStr)
	assert.Nil(t, err)
	buf := new(bytes.Buffer)
	assert.Nil(t, binary.Write(buf, binary.BigEndian, byte(1)))
	assert.Nil(t, binary.Write(buf, binary.BigEndian, uint64(len(hexStr))))
	assert.Nil(t, binary.Write(buf, binary.BigEndian, []byte(hexStr)))

	type testStruct struct {
		H arq.ShaHash
	}
	var actual testStruct
	assert.Nil(t, arq.DecodeArq(buf, &actual))
	assert.Equal(t, expected, actual.H.Contents[:])
}

func TestDecodeTree(t *testing.T) {
	by, err := ioutil.ReadFile("testdata/types/1.tree")
	assert.Nil(t, err)

	r := bytes.NewReader(by)

	aT := arq.ArqTree{}
	err = arq.DecodeArq(r, &aT)
	if !assert.Nil(t, err, aT) {
		return
	}
	assert.Equal(t, []byte("TreeV022"), aT.Header[:])
	assert.Equal(t, int32(501), aT.Uid)
	assert.Equal(t, int32(20), aT.Gid)
	assert.Equal(t, int32(16877), aT.Mode)

	if !assert.Equal(t, 1, len(aT.Nodes)) {
		return
	}
	assert.Equal(t, "one.txt", aT.Nodes[0].FileName)
	node := aT.Nodes[0].Node
	assert.Equal(t, false, node.IsTree)
	assert.Equal(t, 26, int(node.DataSize))
	assert.Equal(t, arq.Lz4Compression, node.DataCompressionType)
	if !assert.Equal(t, 1, len(node.DataBlobKeys)) {
		return
	}
	bk := node.DataBlobKeys[0]
	assert.Equal(t, "92a1aaa5506fafc27548eb324dc3b885fe0968ac", bk.Hash.String())
}

func TestDecodePackIndex(t *testing.T) {
	by, err := ioutil.ReadFile("testdata/types/1.index")
	assert.Nil(t, err)

	r := bytes.NewReader(by)

	pi := arq.ArqPackIndex{}
	err = arq.DecodeArq(r, &pi)
	if !assert.Nil(t, err, pi) {
		return
	}
	assert.Equal(t, []byte{0xff, 0x74, 0x4f, 0x63}, pi.Header[:])
	assert.Equal(t, 2, int(pi.Version))
	assert.Equal(t, len(pi.Objects), int(pi.Fanout[255]), "number of objects decoded doesn't match the parsed fanout")
	for i, obj := range pi.Objects {
		assert.Equalf(t, []byte{0x00, 0x00, 0x00, 0x00}, obj.Alignment[:], "non zero alignment for object %d, %+v", i, obj)
	}
	if !assert.Equal(t, 2, len(pi.Objects)) {
		return
	}
	{
		o := pi.Objects[0]
		d, _ := hex.DecodeString("0ed92a2ab71b2fe75a28fcd785e1c9ec51e040f2")
		assert.Equal(t, d, o.SHA1[:])
		assert.Equal(t, 16, int(o.Offset))
		assert.Equal(t, 1316, int(o.Length))
	}
	{
		o := pi.Objects[1]
		d, _ := hex.DecodeString("5d2d2b62a1b11b2e5977c5ea65cb4708e5f41887")
		assert.Equal(t, d, o.SHA1[:])
		assert.Equal(t, 1342, int(o.Offset))
		assert.Equal(t, 372, int(o.Length))
	}
}
