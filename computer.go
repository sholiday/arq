package arq

import (
	"context"
	"io"
	"path"
	"regexp"

	"github.com/rclone/rclone/fs"
	"howett.net/plist"
)

func unmarshalPlist(ctx context.Context, o fs.Object, out interface{}) error {
	rc, err := o.Open(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()
	by, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	_, err = plist.Unmarshal(by, out)
	return err
}

func parseComputerInfo(ctx context.Context, f fs.Fs, cPath string) (ComputerInfo, error) {
	var info ComputerInfo
	o, err := f.NewObject(ctx, path.Join(cPath, "computerinfo"))
	if err != nil {
		return info, err
	}
	err = unmarshalPlist(ctx, o, &info)
	return info, err
}

func ListComputers(ctx context.Context, f fs.Fs, base string) ([]Computer, error) {
	entries, err := f.List(ctx, base)
	if err != nil {
		return nil, err
	}

	computers := make([]Computer, 0, len(entries))
	for _, entry := range entries {
		if !uuidRegex.MatchString(entry.String()) {
			continue
		}
		d, ok := entry.(fs.Directory)
		if !ok {
			continue
		}
		c := Computer{
			Uuid:   path.Base(d.String()),
			opened: false,
			base:   base,
			fs:     f,
		}
		cInfo, err := parseComputerInfo(ctx, f, d.String())
		if err != nil {
			return nil, err
		}
		c.Info = cInfo
		computers = append(computers, c)
	}
	return computers, nil
}

func NewComputer(fs fs.Fs, base string) *Computer {
	return &Computer{
		opened: false,
		base:   base,
		fs:     fs,
	}
}

type Computer struct {
	Uuid string
	Info ComputerInfo

	opened bool
	base   string
	fs     fs.Fs
	enc    *encryptionV3
}

func (c *Computer) Open(ctx context.Context, passphrase string) error {
	if err := c.unlock(ctx, passphrase); err != nil {
		return err
	}
	return nil
}

func (c *Computer) NewObject(ctx context.Context, p string) (fs.Object, error) {
	return c.fs.NewObject(ctx, path.Join(c.base, p))
}

func (c *Computer) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	return c.fs.List(ctx, path.Join(c.base, dir))
}

func (c *Computer) unlock(ctx context.Context, passphrase string) error {
	obj, err := c.fs.NewObject(ctx, path.Join(c.base, "encryptionv3.dat"))
	if err != nil {
		return err
	}
	rc, err := obj.Open(ctx)
	if err != nil {
		return err
	}
	c.enc, err = Unlock(ctx, rc, passphrase)
	return err
}

type ComputerInfo struct {
	UserName     string `plist:"userName"`
	ComputerName string `plist:"computerName"`
}

var uuidRegex = regexp.MustCompile("[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}")

func (c *Computer) ListFolders(ctx context.Context) ([]FolderInfo, error) {
	entries, err := c.fs.List(ctx, path.Join(c.base, "buckets"))
	if err != nil {
		return nil, err
	}

	folders := make([]FolderInfo, 0, len(entries))
	for _, entry := range entries {
		if !uuidRegex.MatchString(entry.String()) {
			continue
		}
		o, ok := entry.(fs.Object)
		if !ok {
			continue
		}
		rc, err := o.Open(ctx)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		eor := NewEObjectReader(rc, c.enc)
		by, err := io.ReadAll(eor)
		if err != nil {
			return nil, err
		}
		var folder FolderInfo
		if _, err := plist.Unmarshal(by, &folder); err != nil {
			return nil, err
		}
		folder.computer = c
		folders = append(folders, folder)
	}
	return folders, nil
}

func (fi *FolderInfo) Folder() *Folder {
	return &Folder{
		uuid:     fi.BucketUuid,
		computer: fi.computer,
		fInfo:    fi,
	}
}

type FolderInfo struct {
	Endpoint                                       string `plist:"Endpoint"`
	BucketUuid                                     string `plist:"BucketUUID"`
	BucketName                                     string `plist:"BucketName"`
	ComputerUuid                                   string `plist:"ComputerUUID"`
	LocalPath                                      string `plist:"LocalPath"`
	LocalMountPoint                                string `plist:"LocalMountPoint"`
	StorageType                                    int    `plist:"StorageType"`
	SkipDuringBackup                               bool   `plist:"SkipDuringBackup"`
	ExcludeItemsWithTimeMachineExcludeMetadataFlag bool   `plist:"ExcludeItemsWithTimeMachineExcludeMetadataFlag"`

	computer *Computer
}
