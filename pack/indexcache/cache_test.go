package indexcache_test

import (
	"context"
	"os"
	"testing"

	"github.com/sholiday/arq"
	"github.com/sholiday/arq/pack/indexcache"
	"github.com/stretchr/testify/assert"
)

type cacheTester interface {
	Builder() indexcache.Builder
	Searcher() indexcache.Searcher
	Close()
}

type NewCacheTester func(t *testing.T) cacheTester

func loadPackIndex(t *testing.T, fname string) *arq.ArqPackIndex {
	f, err := os.Open(fname)
	if !assert.Nil(t, err) {
		return nil
	}
	defer f.Close()
	pi := arq.ArqPackIndex{}
	err = arq.DecodeArq(f, &pi)
	if !assert.Nil(t, err, pi) {
		return nil
	}
	return &pi
}

func decodeSha(in string) arq.ShaHash {
	h, err := arq.DecodeShaHashString(in)
	if err != nil {
		panic(err)
	}
	return h
}

type pack struct {
	h arq.ShaHash
	i arq.ArqPackIndex
}

func testWithPacks(t *testing.T, nct NewCacheTester, packs []pack) {
	ctx := context.Background()
	ct := nct(t)
	defer ct.Close()
	b := ct.Builder()

	// Index all of the given packs.
	for _, p := range packs {
		has, err := b.HasPackIndex(ctx, p.h)
		if !assert.Nil(t, err) {
			return
		}
		assert.False(t, has)
		err = b.AddPackIndex(ctx, p.h, p.i)
		if !assert.Nil(t, err) {
			return
		}
	}

	// Build the cache.
	if !assert.Nil(t, b.Build(ctx)) {
		return
	}

	// Now search for every element we indexed.
	s := ct.Searcher()
	if s == nil {
		return
	}
	for _, p := range packs {
		for _, o := range p.i.Objects {
			l, err := s.Find(ctx, arq.WrapShaHash(&o.SHA1))
			if !assert.Nil(t, err) {
				return
			}
			assert.Equal(t, p.h, l.PackHash)
			assert.Equal(t, o.Offset, l.Offset)
			assert.Equal(t, o.Length, l.Length)
		}
	}

	// And an element we know not to exist.
	_, err := s.Find(ctx, decodeSha("2d48a782b4db79027b408ef3d0276ac2d4a8b79b"))
	assert.ErrorIs(t, err, indexcache.ErrNotFound)
}

func Run(t *testing.T, nct NewCacheTester) {
	t.Run("1", func(t *testing.T) {
		var pi *arq.ArqPackIndex
		if pi = loadPackIndex(t, "../../testdata/types/1.index"); pi == nil {
			return
		}
		piH := arq.WrapShaHash(&pi.SHA1)
		testWithPacks(t, nct, []pack{{piH, *pi}})
	})

	t.Run("Empty", func(t *testing.T) {
		var pi arq.ArqPackIndex
		piH := decodeSha("2d48a782b4db79027b408ef3d0276ac2d4a8b79b")
		testWithPacks(t, nct, []pack{{piH, pi}})
	})

	t.Run("AdjacentObjects", func(t *testing.T) {
		var pi arq.ArqPackIndex
		pi.Objects = []arq.ArqPackIndexObject{
			{
				Offset: 1000,
				Length: 100,
				SHA1:   decodeSha("aa00000000000000000000000000000000000001").Contents,
			},
			// This one follows the previous object, within the same prefix.
			{
				Offset: 2000,
				Length: 100,
				SHA1:   decodeSha("aa00000000000000000000000000000000000002").Contents,
			},
			{
				Offset: 3000,
				Length: 100,
				SHA1:   decodeSha("aa00000000000000000000000000000000000003").Contents,
			},
			// This is the prefix after the previous object.
			{
				Offset: 4000,
				Length: 100,
				SHA1:   decodeSha("ab00000000000000000000000000000000000001").Contents,
			},
		}
		piH := decodeSha("2d48a782b4db79027b408ef3d0276ac2d4a8b79b")
		testWithPacks(t, nct, []pack{{piH, pi}})
	})

	// This test verifies the first and last prefixes possible.
	t.Run("FirstAndLast", func(t *testing.T) {
		var pi arq.ArqPackIndex
		pi.Objects = []arq.ArqPackIndexObject{
			{
				Offset: 1000,
				Length: 100,
				SHA1:   decodeSha("0000000000000000000000000000000000000001").Contents,
			},
			{
				Offset: 3000,
				Length: 100,
				SHA1:   decodeSha("ff00000000000000000000000000000000000001").Contents,
			},
		}
		piH := decodeSha("2d48a782b4db79027b408ef3d0276ac2d4a8b79b")
		testWithPacks(t, nct, []pack{{piH, pi}})
	})
}
