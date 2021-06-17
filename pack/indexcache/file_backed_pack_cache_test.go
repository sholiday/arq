package indexcache_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/sholiday/arq/pack/indexcache"
	"github.com/stretchr/testify/assert"
)

func fbNewCacheTester(t *testing.T) cacheTester {
	tdir, err := ioutil.TempDir("", "arqfilecache")
	if !assert.Nil(t, err) {
		return nil
	}
	return &fbCacheTester{
		tdir: tdir,
	}
}

type fbCacheTester struct {
	tdir     string
	searcher *indexcache.FileSearcher
}

func (ct fbCacheTester) Builder() indexcache.Builder {
	return indexcache.NewFileBuilder(ct.tdir)
}

func (ct fbCacheTester) Searcher() indexcache.Searcher {
	ct.searcher = indexcache.NewFileSearcher(ct.tdir)
	if err := ct.searcher.Open(context.Background()); err != nil {
		return nil
	}
	return ct.searcher
}

func (ct fbCacheTester) Close() {
	if ct.searcher != nil {
		ct.searcher.Close()
	}
	os.RemoveAll(ct.tdir)
}

func TestFileBackedCache(t *testing.T) {
	Run(t, fbNewCacheTester)
}
