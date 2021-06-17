package arq_test

import (
	"context"
	"log"
	"testing"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/sholiday/arq"
	"github.com/stretchr/testify/assert"
)

func TestFolder(t *testing.T) {
	const (
		computerUuid = "8C10C697-7DCA-4747-B92B-6900CC64CCE7"
		bucketUuid   = "9084C9D4-B59E-4F94-A577-CF5FCFF23056"
	)
	ctx := context.Background()
	cfg := configmap.New()
	localFs, err := local.NewFs(ctx, "localfs", "testdata/t1/local", cfg)
	if err != nil {
		log.Println(err)
	}
	c := arq.NewComputer(localFs, computerUuid)
	if !assert.Nil(t, c.Open(ctx, "hunter2")) {
		return
	}

	folders, err := c.ListFolders(ctx)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.Equal(t, 1, len(folders)) {
		return
	}
	f := folders[0].Folder()

	master, err := f.FindMaster(ctx)
	if !assert.Nil(t, err) {
		return
	}
	assert.Equal(t, "917ba67b0748ebbf02f12cdf2b49f536e5ddb20e", master.String())
}
