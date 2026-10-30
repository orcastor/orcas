package s3

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	batchutil "github.com/orcastor/orcas/util"
)

type batchPersistenceHandler struct {
	core.Handler
	fail      atomic.Bool
	persisted atomic.Int32
	empty     atomic.Int32
}

func (*batchPersistenceHandler) Ref(core.Ctx, int64, []*core.DataInfo) ([]int64, error) {
	return []int64{0}, nil
}
func (*batchPersistenceHandler) PutData(_ core.Ctx, _, id int64, _ int, _ []byte) (int64, error) {
	return id, nil
}
func (*batchPersistenceHandler) GetBktInfo(core.Ctx, int64) (*core.BucketInfo, error) {
	return &core.BucketInfo{}, nil
}
func (h *batchPersistenceHandler) Put(_ core.Ctx, _ int64, objects []*core.ObjectInfo) ([]int64, error) {
	if len(objects) != 1 || objects[0].Size != 0 {
		return nil, errors.New("unexpected standalone metadata")
	}
	h.empty.Add(1)
	return []int64{objects[0].ID}, nil
}
func (h *batchPersistenceHandler) PutDataInfoAndObj(_ core.Ctx, _ int64, data []*core.DataInfo, objects []*core.ObjectInfo) error {
	if h.fail.Load() {
		return errors.New("injected persistence error")
	}
	if len(data) == 0 || len(data) != len(objects) || data[0].PkgID == 0 {
		return errors.New("missing packaged metadata")
	}
	h.persisted.Add(int32(len(objects)))
	return nil
}

func TestBatchPersistenceBeforeS3Success(t *testing.T) {
	t.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	t.Setenv("ORCAS_ADMIN_PASSWORD", "batch-test-password")
	path := t.TempDir()
	t.Chdir(path)
	t.Cleanup(func() { core.GetDBPool().Close() })
	if err := core.InitDB(".", ""); err != nil {
		t.Fatal(err)
	}
	ma := core.NewDefaultMetadataAdapter()
	ma.SetBasePath(".")
	ma.SetDataPath(".")
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			uid, bucket := core.NewID(), core.NewID()
			if err := ma.PutBkt(context.Background(), []*core.BucketInfo{{ID: bucket, Name: "batch-test"}}); err != nil {
				t.Fatal(err)
			}
			if err := ma.PutACL(context.Background(), bucket, uid, core.ALL); err != nil {
				t.Fatal(err)
			}
			h := &batchPersistenceHandler{}
			h.fail.Store(fail)
			previous := handler
			handler = h

			t.Cleanup(func() {
				h.fail.Store(false)
				if err := batchutil.FlushBatchWriterForBucket(context.Background(), bucket); err != nil {
					t.Error(err)
				}
				handler = previous
				bucketListCache.Del(uid)
				bucketCache.Del(bucket)
			})
			request := func(body string) *httptest.ResponseRecorder {
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPut, "/batch-test/file", bytes.NewBufferString(body))
				c.Params = gin.Params{{Key: "bucket", Value: "batch-test"}, {Key: "key", Value: "/file"}}
				c.Set("uid", uid)
				PutObject(c)
				return recorder
			}
			response := request("payload")
			if fail {
				if response.Code != http.StatusInternalServerError || h.persisted.Load() != 0 {
					t.Fatalf("failed persistence returned %d: %s", response.Code, response.Body.String())
				}
			} else {
				if response.Code != http.StatusOK || h.persisted.Load() != 1 {
					t.Fatalf("success before persistence: status=%d saved=%d body=%s", response.Code, h.persisted.Load(), response.Body.String())
				}
				// Empty files must take the metadata path, without a pending package.
				bucketCache.Del(bucket) // Avoid the separate on-disk bucket validation path in this fixture.
				response = request("")
				if response.Code != http.StatusOK || h.empty.Load() != 1 {
					t.Fatalf("empty file was not persisted: %d %s", response.Code, response.Body.String())
				}
			}
		})
	}
}
