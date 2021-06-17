package arq

import (
	"context"
	"fmt"
	"io"
	"log"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/rclone/rclone/fs"
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

type RefListEntry struct {
	Name int
	o    fs.Object
}

var refRegex = regexp.MustCompile("[0-9]+")

// ListRefs returns a sorted list of RefListEntry, corresponding to all the
// commits in `refs/logs/master`.
//
// The list is sorted in reverse chronological order (the most recent commit is
// first).
func (f *Folder) ListRefs(ctx context.Context) ([]RefListEntry, error) {
	dirEntries, err := f.computer.List(ctx, path.Join("bucketdata", f.uuid, "refs", "logs", "master"))
	if err != nil {
		return nil, err
	}
	refs := make([]RefListEntry, 0, len(dirEntries))
	for _, entry := range dirEntries {
		fName := path.Base(entry.String())
		if !refRegex.MatchString(fName) {
			continue
		}
		var name int
		if name, err = strconv.Atoi(fName); err != nil {
			log.Println(err)
			continue
		}
		o, ok := entry.(fs.Object)
		if !ok {
			continue
		}
		refs = append(refs, RefListEntry{
			Name: name,
			o:    o,
		})
	}

	sort.Slice(refs, func(i, j int) bool {
		return refs[i].Name > refs[j].Name
	})
	return refs, nil
}

type RefEntry struct {
	OldHeadStretchKey bool   `plist:"oldHeadStretchKey"`
	NewHeadSha1       string `plist:"newHeadSHA1"`
	NewHeadStretchKey bool   `plist:"newHeadStretchKey"`
	PackSha1          string `plist:"packSHA1"`
}

func (f *Folder) RefEntry(ctx context.Context, name int) (RefEntry, error) {
	var re RefEntry
	fName := path.Join("bucketdata", f.uuid, "refs", "logs", "master", strconv.Itoa(name))
	obj, err := f.computer.NewObject(ctx, fName)
	if err != nil {
		return re, err
	}
	err = unmarshalPlist(ctx, obj, &re)
	return re, err
}
