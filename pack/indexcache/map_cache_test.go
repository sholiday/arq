package indexcache_test

import (
	"testing"

	"github.com/sholiday/arq/pack/indexcache"
)

type mCacheTester struct {
	c *indexcache.MapBackedCache
}

func (ct mCacheTester) Builder() indexcache.Builder {
	return ct.c
}

func (ct mCacheTester) Searcher() indexcache.Searcher {
	return ct.c
}

func (ct mCacheTester) Close() {
}

func newMCacheTester(t *testing.T) cacheTester {
	c := indexcache.NewMapBackedCache()
	return &mCacheTester{c}
}

func TestMapBackedCache(t *testing.T) {
	Run(t, newMCacheTester)
}
