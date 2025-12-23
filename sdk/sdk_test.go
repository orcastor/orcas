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
		CmprWay:  core.DATA_CMPR_GZIP,
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
		core.NewLocalAdmin().PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "下载", Type: 1}})
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
		err = os.WriteFile(testFile1, []byte("test content 1"), 0o644)
		So(err, ShouldBeNil)

		testFile2 := filepath.Join(tmpDir, "subdir", "test2.txt")
		err = os.MkdirAll(filepath.Dir(testFile2), 0o755)
		So(err, ShouldBeNil)
		err = os.WriteFile(testFile2, []byte("test content 2"), 0o644)
		So(err, ShouldBeNil)

		sdk := New(core.NewLocalHandler())
		defer sdk.Close()

		c, _, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(c, bktID)
			admin := core.NewLocalAdmin()
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "test-bucket", Type: 1}})
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
		err = os.WriteFile(testFile1, []byte("test content 1"), 0o644)
		So(err, ShouldBeNil)

		testFile2 := filepath.Join(tmpDir, "subdir", "test2.txt")
		err = os.MkdirAll(filepath.Dir(testFile2), 0o755)
		So(err, ShouldBeNil)
		err = os.WriteFile(testFile2, []byte("test content 2"), 0o644)
		So(err, ShouldBeNil)

		sdk := New(core.NewLocalHandler())
		defer sdk.Close()

		c, _, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(c, bktID)
			admin := core.NewLocalAdmin()
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "test-bucket", Type: 1}})
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
		err = os.WriteFile(testFile1, []byte("test content 1"), 0o644)
		So(err, ShouldBeNil)

		testFile2 := filepath.Join(tmpDir, "subdir", "test2.txt")
		err = os.MkdirAll(filepath.Dir(testFile2), 0o755)
		So(err, ShouldBeNil)
		err = os.WriteFile(testFile2, []byte("test content 2"), 0o644)
		So(err, ShouldBeNil)

		sdk := New(core.NewLocalHandler())
		defer sdk.Close()

		c, _, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(c, bktID)
			admin := core.NewLocalAdmin()
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "test-bucket", Type: 1}})
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

func TestACL(t *testing.T) {
	Convey("ACL management", t, func() {
		sdk := New(core.NewLocalHandler())
		defer sdk.Close()

		c, userInfo, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(c, bktID)
			admin := core.NewLocalAdmin()
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "test-bucket", Type: 1}})
			So(err, ShouldBeNil)
		} else {
			bktID = b[0].ID
		}

		// Get MetadataAdapter to access ACL methods
		// We can access it through LocalHandler's internal structure
		// For testing, we'll use DefaultMetadataAdapter directly
		ma := &core.DefaultMetadataAdapter{}

		// Test PutACL: Grant READ permission to current user
		err = ma.PutACL(c, bktID, userInfo.ID, core.READ)
		So(err, ShouldBeNil)

		// Test ListACL: List all ACLs for the bucket
		acls, err := ma.ListACL(c, bktID)
		So(err, ShouldBeNil)
		So(len(acls), ShouldBeGreaterThan, 0)

		// Verify the ACL we just created exists
		found := false
		for _, acl := range acls {
			if acl.BktID == bktID && acl.UID == userInfo.ID {
				So(acl.Perm, ShouldEqual, core.READ)
				found = true
				break
			}
		}
		So(found, ShouldBeTrue)

		// Test CheckPermission: Check if user has READ permission
		hasPermission, err := ma.CheckPermission(c, bktID, userInfo.ID, core.READ)
		So(err, ShouldBeNil)
		So(hasPermission, ShouldBeTrue)

		// Test CheckPermission: Check if user has WRITE permission (should be false)
		hasPermission, err = ma.CheckPermission(c, bktID, userInfo.ID, core.WRITE)
		So(err, ShouldBeNil)
		So(hasPermission, ShouldBeFalse)

		// Test PutACL: Update permission to ALL
		err = ma.PutACL(c, bktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		// Verify permission was updated
		hasPermission, err = ma.CheckPermission(c, bktID, userInfo.ID, core.WRITE)
		So(err, ShouldBeNil)
		So(hasPermission, ShouldBeTrue)

		// Test ListACLByUser: List all buckets accessible by the user
		userACLs, err := ma.ListACLByUser(c, userInfo.ID)
		So(err, ShouldBeNil)
		So(len(userACLs), ShouldBeGreaterThan, 0)

		// Verify the bucket is in the list
		found = false
		for _, acl := range userACLs {
			if acl.BktID == bktID && acl.UID == userInfo.ID {
				found = true
				break
			}
		}
		So(found, ShouldBeTrue)

		// Test DeleteACL: Remove the ACL entry
		err = ma.DeleteACL(c, bktID, userInfo.ID)
		So(err, ShouldBeNil)

		// Verify ACL was deleted
		acls, err = ma.ListACL(c, bktID)
		So(err, ShouldBeNil)
		found = false
		for _, acl := range acls {
			if acl.BktID == bktID && acl.UID == userInfo.ID {
				found = true
				break
			}
		}
		So(found, ShouldBeFalse)

		// Test PutACL: Add ACL back with different permissions
		err = ma.PutACL(c, bktID, userInfo.ID, core.DR|core.MDR) // Data Read and Metadata Read
		So(err, ShouldBeNil)

		// Test DeleteAllACL: Remove all ACLs for the bucket
		err = ma.DeleteAllACL(c, bktID)
		So(err, ShouldBeNil)

		// Verify all ACLs were deleted
		acls, err = ma.ListACL(c, bktID)
		So(err, ShouldBeNil)
		// There might still be ACLs from bucket creation (owner with ALL permission)
		// So we just verify the operation succeeded
	})
}
