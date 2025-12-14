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

// ORCAS_BASE=/opt/orcas ORCAS_DATA=/opt/orcas_disk ORCAS_SECRET=xxxxxxxx go test . -run=TestUpload -v
var (
	mntPath = "/opt/orcas_disk/"
	path    = "/mnt/e/Download/media_new/"
	cfg     = Config{
		UserName: "orcas",
		Password: "orcas",
		RefLevel: FULL,
		//	EndecWay: core.DATA_ENDEC_AES256,
		//	EndecKey: "1234567890abcdef12345678",
		WiseCmpr: core.DATA_CMPR_GZIP,
		CmprQlty: 5,
		DontSync: ".*",
	}
)

func init() {
	core.InitDB("")

	sdk := New(core.NewLocalHandler())
	defer sdk.Close()

	c, _, b, _ := sdk.Login(cfg)
	if len(b) <= 0 {
		bktID, _ := idgen.NewIDGen(nil, 0).New()
		core.InitBucketDB(c, bktID)
		core.NewLocalAdmin().PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "下载", UID: 1, Type: 1}})
	}
}

func TestUpload(t *testing.T) {
	Convey("upload dir", t, func() {
		sdk := New(core.NewLocalHandler())
		defer sdk.Close()

		c, u, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(c, bktID)
			admin := core.NewLocalAdmin()
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "下载", UID: u.ID, Type: 1}})
			So(err, ShouldBeNil)
		} else {
			bktID = b[0].ID
		}

		So(sdk.Upload(c, bktID, core.ROOT_OID, path), ShouldBeNil)
	})
}

func TestDownload(t *testing.T) {
	Convey("download dir", t, func() {
		sdk := New(core.NewLocalHandler())
		defer sdk.Close()

		c, u, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(c, bktID)
			admin := core.NewLocalAdmin()
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "下载", UID: u.ID, Type: 1}})
			So(err, ShouldBeNil)
		} else {
			bktID = b[0].ID
		}

		id, _ := sdk.Path2ID(c, bktID, core.ROOT_OID, filepath.Base(path))
		So(sdk.Download(c, bktID, id, mntPath), ShouldBeNil)

		fmt.Println(sdk.ID2Path(c, bktID, id))
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
