package sdk

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// ORCAS_BASE=/opt/orcas ORCAS_DATA=/opt/orcas_disk ORCAS_SECRET=xxxxxxxx go test . -run=TestUpload -v
var (
	cfg = Config{
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
		// Create temporary test directory with test files
		tmpDir, err := os.MkdirTemp("", "orcas_upload_test_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		// Create test files
		testFile1 := filepath.Join(tmpDir, "test1.txt")
		err = os.WriteFile(testFile1, []byte("test content 1"), 0644)
		So(err, ShouldBeNil)

		testFile2 := filepath.Join(tmpDir, "subdir", "test2.txt")
		err = os.MkdirAll(filepath.Dir(testFile2), 0755)
		So(err, ShouldBeNil)
		err = os.WriteFile(testFile2, []byte("test content 2"), 0644)
		So(err, ShouldBeNil)

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
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "test-bucket", UID: u.ID, Type: 1}})
			So(err, ShouldBeNil)
		} else {
			bktID = b[0].ID
		}

		So(sdk.Upload(c, bktID, core.ROOT_OID, tmpDir), ShouldBeNil)
	})
}

func TestDownload(t *testing.T) {
	Convey("download dir", t, func() {
		// Create temporary test directory with test files
		tmpDir, err := os.MkdirTemp("", "orcas_download_test_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		// Create test files
		testFile1 := filepath.Join(tmpDir, "test1.txt")
		err = os.WriteFile(testFile1, []byte("test content 1"), 0644)
		So(err, ShouldBeNil)

		testFile2 := filepath.Join(tmpDir, "subdir", "test2.txt")
		err = os.MkdirAll(filepath.Dir(testFile2), 0755)
		So(err, ShouldBeNil)
		err = os.WriteFile(testFile2, []byte("test content 2"), 0644)
		So(err, ShouldBeNil)

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
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "test-bucket", UID: u.ID, Type: 1}})
			So(err, ShouldBeNil)
		} else {
			bktID = b[0].ID
		}

		// Upload first
		err = sdk.Upload(c, bktID, core.ROOT_OID, tmpDir)
		So(err, ShouldBeNil)

		// Download to temporary directory
		downloadDir, err := os.MkdirTemp("", "orcas_download_dest_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(downloadDir)

		id, err := sdk.Path2ID(c, bktID, core.ROOT_OID, filepath.Base(tmpDir))
		So(err, ShouldBeNil)
		So(id, ShouldBeGreaterThan, 0)

		err = sdk.Download(c, bktID, id, downloadDir)
		So(err, ShouldBeNil)

		fmt.Println(sdk.ID2Path(c, bktID, id))
	})
}

func TestCheck(t *testing.T) {
	Convey("normal", t, func() {
		// Create temporary test directory with test files
		tmpDir, err := os.MkdirTemp("", "orcas_check_test_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		// Create test files
		testFile1 := filepath.Join(tmpDir, "test1.txt")
		err = os.WriteFile(testFile1, []byte("test content 1"), 0644)
		So(err, ShouldBeNil)

		testFile2 := filepath.Join(tmpDir, "subdir", "test2.txt")
		err = os.MkdirAll(filepath.Dir(testFile2), 0755)
		So(err, ShouldBeNil)
		err = os.WriteFile(testFile2, []byte("test content 2"), 0644)
		So(err, ShouldBeNil)

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
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "test-bucket", UID: u.ID, Type: 1}})
			So(err, ShouldBeNil)
		} else {
			bktID = b[0].ID
		}

		// Upload
		err = sdk.Upload(c, bktID, core.ROOT_OID, tmpDir)
		So(err, ShouldBeNil)

		// Download to temporary directory
		downloadDir, err := os.MkdirTemp("", "orcas_check_dest_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(downloadDir)

		id, err := sdk.Path2ID(c, bktID, core.ROOT_OID, filepath.Base(tmpDir))
		So(err, ShouldBeNil)
		So(id, ShouldBeGreaterThan, 0)

		err = sdk.Download(c, bktID, id, downloadDir)
		So(err, ShouldBeNil)

		// Compare using diff command
		var out bytes.Buffer
		cmd := exec.Command("diff", "-urNa", "-x.*", tmpDir, filepath.Join(downloadDir, filepath.Base(tmpDir)))
		cmd.Stdout = &out
		err = cmd.Run()
		So(out.String(), ShouldBeEmpty)
		So(err, ShouldBeNil)
	})
}
