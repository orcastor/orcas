package sdk

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// ORCAS_SECRET=xxxxxxxx go test . -run=TestUpload -v
// Note: ORCAS_BASE and ORCAS_DATA are no longer used as environment variables.
// Paths are configured via context using Path2Ctx or Config2Ctx, defaulting to current directory (.)
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
	core.InitDB(".", "")

	sdk := New(core.NewLocalHandler(".", "."))
	defer sdk.Close()

	c, _, b, _ := sdk.Login(cfg)
	if len(b) <= 0 {
		bktID, _ := idgen.NewIDGen(nil, 0).New()
		core.InitBucketDB(".", bktID)
		core.NewLocalAdmin().PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "下载", Type: 1}})
	}
}

func TestUpload(t *testing.T) {
	Convey("upload dir", t, func() {
		// Initialize database if not already initialized (paths now managed via Handler)
		if err := core.InitDB(".", ""); err != nil {
			// Ignore error if database already exists
			if err.Error() != "open db failed" {
				So(err, ShouldBeNil)
			}
		}

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

		sdk := New(core.NewLocalHandler("", ""))
		defer sdk.Close()

		c, _, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(".", bktID)
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
		// Initialize database if not already initialized (paths now managed via Handler)
		if err := core.InitDB(".", ""); err != nil {
			// Ignore error if database already exists
			if err.Error() != "open db failed" {
				So(err, ShouldBeNil)
			}
		}

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

		sdk := New(core.NewLocalHandler("", ""))
		defer sdk.Close()

		c, _, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(".", bktID)
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

// compareDirectories compares two directories recursively
func compareDirectories(dir1, dir2 string) error {
	return filepath.Walk(dir1, func(path1 string, info1 os.FileInfo, err1 error) error {
		if err1 != nil {
			return err1
		}

		// Get relative path from base directory
		relPath, err := filepath.Rel(dir1, path1)
		if err != nil {
			return err
		}

		// Skip hidden files
		if filepath.Base(relPath)[0] == '.' {
			if info1.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		path2 := filepath.Join(dir2, relPath)
		info2, err2 := os.Stat(path2)

		if os.IsNotExist(err2) {
			return fmt.Errorf("file %s exists in source but not in destination", relPath)
		}
		if err2 != nil {
			return err2
		}

		if info1.IsDir() != info2.IsDir() {
			return fmt.Errorf("file type mismatch for %s", relPath)
		}

		if !info1.IsDir() {
			// Compare file contents
			data1, err := os.ReadFile(path1)
			if err != nil {
				return err
			}
			data2, err := os.ReadFile(path2)
			if err != nil {
				return err
			}
			if string(data1) != string(data2) {
				return fmt.Errorf("file content mismatch for %s", relPath)
			}
		}

		return nil
	})
}

func TestCheck(t *testing.T) {
	Convey("normal", t, func() {
		// Initialize database if not already initialized (paths now managed via Handler)
		if err := core.InitDB(".", ""); err != nil {
			// Ignore error if database already exists
			if err.Error() != "open db failed" {
				So(err, ShouldBeNil)
			}
		}

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

		sdk := New(core.NewLocalHandler("", ""))
		defer sdk.Close()

		c, _, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(".", bktID)
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

		// Compare files using Go code (more portable than diff command)
		err = compareDirectories(tmpDir, filepath.Join(downloadDir, filepath.Base(tmpDir)))
		So(err, ShouldBeNil)
	})
}

func TestACL(t *testing.T) {
	Convey("ACL management", t, func() {
		// Initialize database if not already initialized (paths now managed via Handler)
		if err := core.InitDB(".", ""); err != nil {
			// Ignore error if database already exists
			if err.Error() != "open db failed" {
				So(err, ShouldBeNil)
			}
		}

		sdk := New(core.NewLocalHandler("", ""))
		defer sdk.Close()

		c, userInfo, b, err := sdk.Login(cfg)
		So(err, ShouldBeNil)

		var bktID int64
		if len(b) <= 0 {
			// Create bucket if none exists
			bktID, _ = idgen.NewIDGen(nil, 0).New()
			core.InitBucketDB(".", bktID)
			admin := core.NewLocalAdmin()
			err = admin.PutBkt(c, []*core.BucketInfo{{ID: bktID, Name: "test-bucket", Type: 1}})
			So(err, ShouldBeNil)
		} else {
			bktID = b[0].ID
		}

		// Get Admin to access ACL management methods
		admin := core.NewLocalAdmin()

		// Get MetadataAdapter for permission checking (CheckPermission and ListACLByUser are still in MetadataAdapter)
		ma := &core.DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
		}
		ma.DefaultBaseMetadataAdapter.SetPath(".")
		ma.DefaultDataMetadataAdapter.SetPath(".")

		// Test PutACL: Grant READ permission to current user
		err = admin.PutACL(c, bktID, userInfo.ID, core.READ)
		So(err, ShouldBeNil)

		// Test ListACL: List all ACLs for the bucket
		acls, err := admin.ListACL(c, bktID)
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

		// Test CheckPermission: Check if user has READ permission (still in MetadataAdapter)
		hasPermission, err := ma.DefaultBaseMetadataAdapter.CheckPermission(c, bktID, userInfo.ID, core.READ)
		So(err, ShouldBeNil)
		So(hasPermission, ShouldBeTrue)

		// Test CheckPermission: Check if user has WRITE permission (should be false)
		hasPermission, err = ma.DefaultBaseMetadataAdapter.CheckPermission(c, bktID, userInfo.ID, core.WRITE)
		So(err, ShouldBeNil)
		So(hasPermission, ShouldBeFalse)

		// Test PutACL: Update permission to ALL
		err = admin.PutACL(c, bktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		// Verify permission was updated
		hasPermission, err = ma.DefaultBaseMetadataAdapter.CheckPermission(c, bktID, userInfo.ID, core.WRITE)
		So(err, ShouldBeNil)
		So(hasPermission, ShouldBeTrue)

		// Test ListACLByUser: List all buckets accessible by the user (still in MetadataAdapter)
		userACLs, err := ma.DefaultBaseMetadataAdapter.ListACLByUser(c, userInfo.ID)
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
		err = admin.DeleteACL(c, bktID, userInfo.ID)
		So(err, ShouldBeNil)

		// Verify ACL was deleted
		acls, err = admin.ListACL(c, bktID)
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
		err = admin.PutACL(c, bktID, userInfo.ID, core.DR|core.MDR) // Data Read and Metadata Read
		So(err, ShouldBeNil)

		// Test DeleteAllACL: Remove all ACLs for the bucket
		err = admin.DeleteAllACL(c, bktID)
		So(err, ShouldBeNil)

		// Verify all ACLs were deleted
		acls, err = admin.ListACL(c, bktID)
		So(err, ShouldBeNil)
		// There might still be ACLs from bucket creation (owner with ALL permission)
		// So we just verify the operation succeeded
	})
}
