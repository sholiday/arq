package arq

import (
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
