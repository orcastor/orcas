package sdk

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// ORCAS_BASE=/tmp/test ORCAS_DATA=/tmp/test ORCAS_SECRET=xxxxxxxx go test . -run=TestUpload -v
var mntPath = "/tmp/test/"
var path = "/home/semaphore/go/"
var cfg = Config{
	UserName: "orcas",
	Password: "orcas",
	DataSync: true,
	RefLevel: FULL,
	EndecWay: core.DATA_ENDEC_AES256,
	EndecKey: "1234567890abcdef12345678",
	WiseCmpr: core.DATA_CMPR_GZIP,
	CmprQlty: 5,
	DontSync: ".*",
}

func init() {
	core.InitDB()

	sdk := New(core.NewLocalHandler())
	defer sdk.Close()

	c, _, b, _ := sdk.Login(cfg)
	if len(b) <= 0 {
		bktID, _ := idgen.NewIDGen(nil, 0).New()
		core.InitBucketDB(c, bktID)
		core.NewLocalAdmin().PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "备份测试", UID: 1, Type: 1}})
	}
}

func TestUpload(t *testing.T) {
	Convey("upload dir", t, func() {
		sdk := New(core.NewLocalHandler())
		defer sdk.Close()

		c, _, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		So(sdk.Upload(c, b[0].ID, core.ROOT_OID, path), ShouldBeNil)
	})
}

func TestDownload(t *testing.T) {
	Convey("download dir", t, func() {
		sdk := New(core.NewLocalHandler())
		defer sdk.Close()

		c, _, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		id, _ := sdk.Path2ID(c, b[0].ID, core.ROOT_OID, filepath.Base(path))
		So(sdk.Download(c, b[0].ID, id, mntPath), ShouldBeNil)

		fmt.Println(sdk.ID2Path(c, b[0].ID, id))
	})
}

func TestCheck(t *testing.T) {
	Convey("normal", t, func() {
		var out bytes.Buffer
		cmd := exec.Command("diff", "-urNa", "-x.*", path, filepath.Join(mntPath, filepath.Base(path)))
		cmd.Stdout = &out
		err := cmd.Run()
		So(out.String(), ShouldBeEmpty)
		So(err, ShouldBeNil)
	})
}
