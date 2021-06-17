package arq

import (
	"context"
	"fmt"
	"io"
	"path"
)

type Folder struct {
	uuid     string
	computer *Computer
	fInfo    *FolderInfo
}

func (f *Folder) FindMaster(ctx context.Context) (ShaHash, error) {
	obj, err := f.computer.NewObject(ctx, path.Join("bucketdata", f.uuid, "refs", "heads", "master"))
	var sh ShaHash
	if err != nil {
		return sh, err
	}
	reader, err := obj.Open(ctx)
	if err != nil {
		return sh, err
	}
	defer reader.Close()

	b := make([]byte, 40)
	if _, err = io.ReadAtLeast(reader, b, 40); err != nil {
		return sh, fmt.Errorf("failed to read hash: %s", err)
	}
	return DecodeShaHash(b)
}
