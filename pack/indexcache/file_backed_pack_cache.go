package indexcache

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"

	"github.com/sholiday/arq"
)

var (
	ErrTooManyPacksets = errors.New("ErrTooManyPacksets")
)

const (
	packLocationFname = "cache_packlocation"
	indexFname        = "cache_index"
	packListFname     = "cache_packlist"
)

type FileBuilder struct {
	workdir string
	mp      *MapBackedCache
}

func NewFileBuilder(workdir string) *FileBuilder {
	return &FileBuilder{
		workdir: workdir,
		mp:      NewMapBackedCache(),
	}
}

func (f *FileBuilder) HasPackIndex(ctx context.Context, h arq.ShaHash) (bool, error) {
	return f.mp.HasPackIndex(ctx, h)
}

func (f *FileBuilder) AddPackIndex(ctx context.Context, h arq.ShaHash, pi arq.ArqPackIndex) error {
	return f.mp.AddPackIndex(ctx, h, pi)
}

func (fb *FileBuilder) Build(ctx context.Context) error {
	pList, pIndex, err := fb.computePacklist(ctx)
	if err != nil {
		return err
	}
	if err := fb.writePacklist(ctx, pList); err != nil {
		return err
	}
	if err := fb.write(ctx, pIndex); err != nil {
		return err
	}
	return nil
}

func (fb *FileBuilder) computePacklist(ctx context.Context) ([]arq.ShaHash, map[arq.ShaHash]uint16, error) {
	if len(fb.mp.packsets) > math.MaxUint16 {
		return nil, nil, ErrTooManyPacksets
	}
	hIndex := make(map[arq.ShaHash]uint16)
	packs := make([]arq.ShaHash, 0, len(fb.mp.packsets))
	for k := range fb.mp.packsets {
		packs = append(packs, k)
	}
	sort.Slice(packs, func(i, j int) bool {
		return bytes.Compare(packs[i].Contents[:], packs[j].Contents[:]) == -1
	})

	for i, h := range packs {
		hIndex[h] = uint16(i)
	}

	return packs, hIndex, nil
}

func (fb *FileBuilder) writePacklist(ctx context.Context, pList []arq.ShaHash) error {
	w, err := os.OpenFile(path.Join(fb.workdir, packListFname), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer w.Close()
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	for _, e := range pList {
		if err := binary.Write(w, binary.BigEndian, e.Contents); err != nil {
			return err
		}
	}
	return nil
}

type packLocationEntry struct {
	Hash      arq.ShaHash
	PackIndex uint16
	Offset    uint64
	Length    uint64
}

func (e *packLocationEntry) MarshalLength() int {
	return len(e.Hash.Contents) + 2 + 8 + 8
}

func (e *packLocationEntry) MarshalArq(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.BigEndian, e.Hash.Contents); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, e.PackIndex); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, e.Offset); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, e.Length); err != nil {
		return 0, err
	}
	return e.MarshalLength(), nil
}

func (e *packLocationEntry) UnmarshalArq(r io.Reader) error {
	if err := arq.DecodeArq(r, &e.Hash.Contents); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &e.PackIndex); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &e.Offset); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &e.Length); err != nil {
		return err
	}
	return nil
}

func (fb *FileBuilder) write(ctx context.Context, pIndex map[arq.ShaHash]uint16) error {
	hashes := make([]arq.ShaHash, 0, len(fb.mp.index))
	for k := range fb.mp.index {
		hashes = append(hashes, k)
	}
	sort.Slice(hashes, func(i, j int) bool {
		return bytes.Compare(hashes[i].Contents[:], hashes[j].Contents[:]) == -1
	})

	plW, err := os.OpenFile(path.Join(fb.workdir, packLocationFname), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer plW.Close()
	bplW := bufio.NewWriter(plW)
	defer bplW.Flush()

	iW, err := os.OpenFile(path.Join(fb.workdir, indexFname), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer iW.Close()
	biW := bufio.NewWriter(iW)
	defer biW.Flush()

	cur := uint16(0)
	loc := uint32(0)
	for _, h := range hashes {
		pl := fb.mp.index[h]
		pi, foundPi := pIndex[pl.PackHash]
		if !foundPi {
			return fmt.Errorf("couldn't find pack has in pIndex")
		}
		plE := packLocationEntry{
			Hash:      h,
			PackIndex: pi,
			Offset:    pl.Offset,
			Length:    pl.Length,
		}

		hPrefix := binary.BigEndian.Uint16(h.Contents[:2])
		for ; hPrefix >= cur; cur += 1 {
			if err := binary.Write(biW, binary.BigEndian, loc); err != nil {
				return err
			}
		}

		if _, err := plE.MarshalArq(bplW); err != nil {
			return err
		}
		loc += uint32(plE.MarshalLength())
	}
	// Now write out the remaining indexes.
	for ; math.MaxUint16 > cur; cur += 1 {
		if err := binary.Write(biW, binary.BigEndian, loc); err != nil {
			return err
		}
	}

	return nil
}

func NewFileSearcher(workdir string) *FileSearcher {
	return &FileSearcher{
		workdir: workdir,
	}
}

type FileSearcher struct {
	workdir string

	packListF     *os.File
	packLocationF *os.File
	indexF        *os.File
}

func (fs *FileSearcher) Open(ctx context.Context) error {
	var err error
	fs.packListF, err = os.Open(path.Join(fs.workdir, packListFname))
	if err != nil {
		fs.Close()
		return err
	}
	fs.packLocationF, err = os.Open(path.Join(fs.workdir, packLocationFname))
	if err != nil {
		fs.Close()
		return err
	}
	fs.indexF, err = os.Open(path.Join(fs.workdir, indexFname))
	if err != nil {
		fs.Close()
		return err
	}
	return nil
}

func (fs *FileSearcher) Close() {
	if fs.indexF != nil {
		fs.indexF.Close()
	}
	if fs.packListF != nil {
		fs.packListF.Close()
	}
	if fs.packLocationF != nil {
		fs.packLocationF.Close()
	}
}

// We know that the result will be at the file location [start, offset)
// If start and offset are the same, we know no hashes with the prefix exist.
type packLocationOffset struct {
	start uint32
	limit uint32
}

func (fs *FileSearcher) findInIndex(ctx context.Context, h arq.ShaHash) (packLocationOffset, error) {
	var lo packLocationOffset

	prefix := binary.BigEndian.Uint16(h.Contents[:2])

	// Read two uint32s.
	by := make([]byte, 4+4)
	// TODO(sholiday): Handle when we're accessing the last possible prefix (like 0xFF).
	// In that case there will be no offset to follow it.
	if _, err := fs.indexF.ReadAt(by, int64(prefix)*4); err != nil {
		return lo, fmt.Errorf("findInIndex %w", err)
	}

	lo.start = binary.BigEndian.Uint32(by[:4])
	lo.limit = binary.BigEndian.Uint32(by[4:])
	return lo, nil
}

func (fs *FileSearcher) findInPackLocation(ctx context.Context, h arq.ShaHash, lo packLocationOffset) (packLocationEntry, error) {
	var le packLocationEntry
	r := io.NewSectionReader(fs.packLocationF, int64(lo.start), int64(lo.limit)+int64(le.MarshalLength()))
	var err error
	for err == nil {
		err := le.UnmarshalArq(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return le, ErrNotFound
			}
			return le, fmt.Errorf("unmarshal %w", err)
		}
		switch cmp := bytes.Compare(h.Contents[:], le.Hash.Contents[:]); cmp {
		case 0:
			return le, nil
		case 1:
			// We haven't reached our hash yet.
			continue
		case -1:
			// We've overshot it.
			return le, ErrNotFound
		}
	}
	return le, err
}

func (fs *FileSearcher) findPackList(ctx context.Context, le packLocationEntry) (arq.ShaHash, error) {
	var h arq.ShaHash

	fOffset := int64(int(le.PackIndex) * len(h.Contents))
	if _, err := fs.packListF.ReadAt(h.Contents[:], fOffset); err != nil {
		return h, fmt.Errorf("findPackList %w", err)
	}

	return h, nil
}

func (fs *FileSearcher) Find(ctx context.Context, h arq.ShaHash) (PackLocation, error) {
	var l PackLocation

	lo, err := fs.findInIndex(ctx, h)
	if err != nil {
		return l, err
	}

	le, err := fs.findInPackLocation(ctx, h, lo)
	if err != nil {
		return l, err
	}
	l.Offset = le.Offset
	l.Length = le.Length

	pl, err := fs.findPackList(ctx, le)
	if err != nil {
		return l, err
	}
	l.PackHash = pl

	return l, nil
}

// Verify interfaces are satisfied.
var (
	_ Builder  = &FileBuilder{}
	_ Searcher = &FileSearcher{}
)
