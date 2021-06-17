package indexcache

import (
	"context"
	"errors"

	"github.com/sholiday/arq"
)

type PackLocation struct {
	Offset   uint64
	Length   uint64
	PackHash arq.ShaHash
}

var (
	ErrAlreadyIndexedPack = errors.New("already indexed this pack index")
	ErrNotFound           = errors.New("ErrNotFound")
)

type Builder interface {
	HasPackIndex(ctx context.Context, h arq.ShaHash) (bool, error)
	AddPackIndex(ctx context.Context, h arq.ShaHash, pi arq.ArqPackIndex) error
	Build(ctx context.Context) error
}

type Searcher interface {
	Find(ctx context.Context, h arq.ShaHash) (PackLocation, error)
}
