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
var mntPath = "/tmp/test/"
var path = "/home/semaphore/go/"
var cfg = Config{
	DataSync: true,
	RefLevel: OFF,
	WiseCmpr: core.DATA_CMPR_ZSTD,
	EndecWay: core.DATA_ENDEC_AES256,
	EndecKey: "1234567890abcdef12345678",
	DontSync: ".*",
}

func init() {
	core.Init(&core.CoreConfig{
		Path: mntPath,
	})
	bktID, _ = idgen.NewIDGen(nil, 0).New()
	core.InitDB()
	core.InitBucketDB(bktID)
}

func TestUpload(t *testing.T) {
	Convey("normal", t, func() {
		Convey("upload dir", func() {

			// bktID = 18885407408128

			c := context.TODO()
			h := core.NewRWHandler()
			defer h.Close()

			So(h.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "zhangwei", UID: 9999, Type: 1}}), ShouldBeNil)

			sdk := New(h)
			sdk.SetConfig(cfg)
			fmt.Println(sdk.Upload(c, bktID, core.ROOT_OID, path))
		})
	})
}

func TestDownload(t *testing.T) {
	Convey("normal", t, func() {
		Convey("upload dir", func() {
			c := context.TODO()
			h := core.NewRWHandler()
			defer h.Close()

			sdk := New(h)
			sdk.SetConfig(cfg)
			id, _ := sdk.Path2ID(c, bktID, core.ROOT_OID, filepath.Base(path))
			fmt.Println(id)
			fmt.Println(sdk.ID2Path(c, bktID, id))

			id2, _ := sdk.Path2ID(c, bktID, id, "go")
			fmt.Println(id2)
			fmt.Println(sdk.ID2Path(c, bktID, id2))

			sdk.Download(c, bktID, id, mntPath)

			var out bytes.Buffer
			cmd := exec.Command("diff", "-urNa", "-x.*", path, filepath.Join(mntPath, filepath.Base(path)))
			cmd.Stdout = &out
			So(cmd.Run(), ShouldBeNil)
			So(out.String(), ShouldBeEmpty)
		})
	})
}
