package indexcache

import (
	"context"

	"github.com/sholiday/arq"
)

type MapBackedCache struct {
	index map[arq.ShaHash]PackLocation
	// List of pack indexes we've seen.
	packsets map[arq.ShaHash]bool
}

func NewMapBackedCache() *MapBackedCache {
	return &MapBackedCache{
		index:    make(map[arq.ShaHash]PackLocation),
		packsets: make(map[arq.ShaHash]bool),
	}
}

func (m *MapBackedCache) HasPackIndex(ctx context.Context, h arq.ShaHash) (bool, error) {
	_, ok := m.packsets[h]
	return ok, nil
}

func (m *MapBackedCache) AddPackIndex(ctx context.Context, h arq.ShaHash, pi arq.ArqPackIndex) error {
	if _, ok := m.packsets[h]; ok {
		return ErrAlreadyIndexedPack
	}
	for _, o := range pi.Objects {
		var oH arq.ShaHash
		copy(oH.Contents[:], o.SHA1[:])
		m.index[oH] = PackLocation{
			Offset:   o.Offset,
			Length:   o.Length,
			PackHash: h,
		}
	}
	m.packsets[h] = true
	return nil
}

func (m *MapBackedCache) Build(ctx context.Context) error {
	// A MapPackCache is always ready for searching.
	return nil
}

func (m *MapBackedCache) Find(ctx context.Context, oH arq.ShaHash) (PackLocation, error) {
	var l PackLocation
	l, ok := m.index[oH]
	if !ok {
		return l, ErrNotFound
	}
	return l, nil
}

var (
	_ Builder  = &MapBackedCache{}
	_ Searcher = &MapBackedCache{}
)
