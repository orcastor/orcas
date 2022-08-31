package sdk

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

var bktID int64

func init() {
	core.Init(&core.CoreConfig{
		Path: "/tmp/test/",
	})
	bktID, _ = idgen.NewIDGen(nil, 0).New()
	core.InitDB()
	core.InitBucketDB(bktID)
}

var path = "/home/semaphore/"

func TestUpload(t *testing.T) {
	Convey("normal", t, func() {
		Convey("upload dir", func() {

			// bktID = 18885407408128

			c := context.TODO()
			h := core.NewRWHandler(bktID)
			defer h.Close()

			So(h.PutBkt(c, []*core.BucketInfo{&core.BucketInfo{ID: bktID, Name: "zhangwei", UID: 9999, Type: 1}}), ShouldBeNil)

			sdk := New(h)
			sdk.SetConfig(Config{
				DataSync: true,
				RefLevel: OFF,
				WiseCmpr: core.DATA_CMPR_ZSTD,
				EndecWay: core.DATA_ENDEC_AES256,
				EndecKey: "1234567890abcdef12345678",
			})
			fmt.Println(sdk.Upload(c, core.ROOT_OID, path))

			id, _ := sdk.Path2ID(c, core.ROOT_OID, filepath.Base(path))
			fmt.Println(id)
			fmt.Println(sdk.ID2Path(c, id))

			id2, _ := sdk.Path2ID(c, id, "go")
			fmt.Println(id2)
			fmt.Println(sdk.ID2Path(c, id2))

			sdk.Download(c, id, "/tmp/test")

			var out bytes.Buffer
			cmd := exec.Command("diff", "-urNa", path, "/tmp/test/"+filepath.Base(path))
			cmd.Stdout = &out
			So(cmd.Run(), ShouldBeNil)
			So(out.String(), ShouldBeEmpty)
		})
	})
}
