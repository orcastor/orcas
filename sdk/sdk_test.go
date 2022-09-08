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
	RefLevel: FULL,
	WiseCmpr: core.DATA_CMPR_ZSTD,
	EndecWay: core.DATA_ENDEC_AES256,
	EndecKey: "1234567890abcdef12345678",
	DontSync: ".*",
	WorkersN: 16,
}

func init() {
	core.Init(&core.CoreConfig{
		Path: mntPath,
	})
	bktID, _ = idgen.NewIDGen(nil, 0).New()
	core.InitDB()
	core.InitBucketDB(bktID)
	core.NewRWHandler().PutBkt(context.TODO(), []*core.BucketInfo{{ID: bktID, Name: "zhangwei", UID: 9999, Type: 1}})
	// bktID = 30403939270656
}

func TestUpload(t *testing.T) {
	Convey("upload dir", t, func() {
		c := context.TODO()
		sdk := New(core.NewRWHandler())
		defer sdk.Close()

		sdk.SetConfig(cfg)
		So(sdk.Upload(c, bktID, core.ROOT_OID, path), ShouldBeNil)
	})
}

func TestDownload(t *testing.T) {
	Convey("download dir", t, func() {
		c := context.TODO()
		sdk := New(core.NewRWHandler())
		defer sdk.Close()

		sdk.SetConfig(cfg)
		id, _ := sdk.Path2ID(c, bktID, core.ROOT_OID, filepath.Base(path))
		fmt.Println(id)
		fmt.Println(sdk.ID2Path(c, bktID, id))

		So(sdk.Download(c, bktID, id, mntPath), ShouldBeNil)
	})
}

func TestCheck(t *testing.T) {
	Convey("normal", t, func() {
		var out bytes.Buffer
		cmd := exec.Command("diff", "-urNa", "-x.*", path, filepath.Join(mntPath, filepath.Base(path)))
		cmd.Stdout = &out
		So(cmd.Run(), ShouldBeNil)
		So(out.String(), ShouldBeEmpty)
	})
}
