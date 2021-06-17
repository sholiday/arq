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

func TestComputer(t *testing.T) {
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

	t.Run("ListComputers", func(t *testing.T) {
		computers, err := arq.ListComputers(ctx, localFs, "")
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, 1, len(computers)) {
			return
		}
		assert.Equal(t, computerUuid, computers[0].Uuid)
		assert.Equal(t, "narrator", computers[0].Info.ComputerName)
		assert.Equal(t, "sholiday", computers[0].Info.UserName)
	})

	c := arq.NewComputer(localFs, computerUuid)
	if !assert.Nil(t, c.Open(ctx, "hunter2")) {
		return
	}

	t.Run("ListFolders", func(t *testing.T) {
		folders, err := c.ListFolders(ctx)
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, 1, len(folders)) {
			return
		}
		assert.Contains(t, folders[0].Endpoint, "t1/local")
		assert.Equal(t, bucketUuid, folders[0].BucketUuid)
		assert.Equal(t, "src", folders[0].BucketName)
		assert.Equal(t, computerUuid, folders[0].ComputerUuid)
	})
}
