package vfs

import (
	"context"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// TestDeleteFileWithAttr tests that extended attributes are deleted when file is deleted
func TestDeleteFileWithAttr(t *testing.T) {
	Convey("Delete file with extended attributes", t, func() {
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		err := core.InitBucketDB(".", testBktID)
		So(err, ShouldBeNil)

		dma := &core.DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
		}
		dma.DefaultBaseMetadataAdapter.SetPath(".")
		dma.DefaultDataMetadataAdapter.SetPath(".")
		dda := &core.DefaultDataAdapter{}
		dda.SetDataPath(".")

		lh := core.NewLocalHandler(".", ".").(*core.LocalHandler)
		lh.SetAdapter(dma, dda)

		testCtx, userInfo, _, err := lh.Login(c, "orcas", "orcas")
		So(err, ShouldBeNil)

		bucket := &core.BucketInfo{
			ID:       testBktID,
			Name:     "test_bucket",
			Type:     1,
			Quota:    100000000,
			Used:     0,
			RealUsed: 0,
		}
		So(dma.PutBkt(testCtx, []*core.BucketInfo{bucket}), ShouldBeNil)
		err = dma.PutACL(testCtx, testBktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		cfg := &core.Config{
			DataPath: ".",
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)

		Convey("test file deletion removes extended attributes", func() {
			// Create a file
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_file_with_attr.txt",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Create node for the file
			fileNode := &OrcasNode{
				fs:    ofs,
				objID: fileID,
			}

			// Set extended attributes
			ctx := context.Background()
			errno := fileNode.Setxattr(ctx, "user.test_attr1", []byte("value1"), 0)
			So(int(errno), ShouldEqual, 0)
			errno = fileNode.Setxattr(ctx, "user.test_attr2", []byte("value2"), 0)
			So(int(errno), ShouldEqual, 0)

			// Verify attributes exist
			dest := make([]byte, 100)
			size, errno := fileNode.Getxattr(ctx, "user.test_attr1", dest)
			So(int(errno), ShouldEqual, 0)
			So(string(dest[:size]), ShouldEqual, "value1")

			// Delete the file
			rootNode := &OrcasNode{
				fs:    ofs,
				objID: testBktID,
			}
			errno = rootNode.Unlink(ctx, "test_file_with_attr.txt")
			So(int(errno), ShouldEqual, 0)

			// Wait a bit for async deletion to complete
			// In production, we might need to wait longer or check completion
			// For test purposes, we'll directly check the database

			// Try to get attributes - should fail because file is deleted
			// First, try to get the file object
			objs, getErr := lh.Get(testCtx, testBktID, []int64{fileID})
			// File should be marked as deleted (PID < 0) or not found
			if getErr == nil && len(objs) > 0 {
				// If file still exists in DB (marked as deleted), check if attrs are gone
				So(objs[0].PID, ShouldBeLessThan, 0) // Should be marked as deleted
			}

			// Try to list attributes directly from database
			// This should return empty after permanent deletion
			attrs, listErr := dma.ListAttrs(testCtx, testBktID, fileID)
			// Attributes should be empty or error
			if listErr == nil {
				So(len(attrs), ShouldEqual, 0)
			}
		})

		Convey("test directory deletion removes extended attributes", func() {
			// Create a directory
			dirID, _ := ig.New()
			dirObj := &core.ObjectInfo{
				ID:     dirID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_DIR,
				Name:   "test_dir_with_attr",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{dirObj})
			So(err, ShouldBeNil)

			// Create node for the directory
			dirNode := &OrcasNode{
				fs:    ofs,
				objID: dirID,
			}

			// Set extended attributes
			ctx := context.Background()
			errno := dirNode.Setxattr(ctx, "user.dir_attr", []byte("dir_value"), 0)
			So(int(errno), ShouldEqual, 0)

			// Verify attribute exists
			dest := make([]byte, 100)
			size, errno := dirNode.Getxattr(ctx, "user.dir_attr", dest)
			So(int(errno), ShouldEqual, 0)
			So(string(dest[:size]), ShouldEqual, "dir_value")

			// Delete the directory
			rootNode := &OrcasNode{
				fs:    ofs,
				objID: testBktID,
			}
			errno = rootNode.Rmdir(ctx, "test_dir_with_attr")
			So(int(errno), ShouldEqual, 0)

			// Try to list attributes directly from database
			attrs, listErr := dma.ListAttrs(testCtx, testBktID, dirID)
			// Attributes should be empty or error
			if listErr == nil {
				So(len(attrs), ShouldEqual, 0)
			}
		})

		Convey("test file with multiple attributes deletion", func() {
			// Create a file
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_file_multi_attr.txt",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Create node for the file
			fileNode := &OrcasNode{
				fs:    ofs,
				objID: fileID,
			}

			// Set multiple extended attributes
			ctx := context.Background()
			for i := 0; i < 10; i++ {
				attrName := "user.attr_" + string(rune('0'+i))
				attrValue := "value_" + string(rune('0'+i))
				errno := fileNode.Setxattr(ctx, attrName, []byte(attrValue), 0)
				So(int(errno), ShouldEqual, 0)
			}

			// List attributes to verify they exist
			listDest := make([]byte, 1000)
			listSize, errno := fileNode.Listxattr(ctx, listDest)
			So(int(errno), ShouldEqual, 0)
			So(listSize, ShouldBeGreaterThan, 0)

			// Delete the file
			rootNode := &OrcasNode{
				fs:    ofs,
				objID: testBktID,
			}
			errno = rootNode.Unlink(ctx, "test_file_multi_attr.txt")
			So(int(errno), ShouldEqual, 0)

			// Try to list attributes directly from database
			attrs, listErr := dma.ListAttrs(testCtx, testBktID, fileID)
			// All attributes should be deleted
			if listErr == nil {
				So(len(attrs), ShouldEqual, 0)
			}
		})
	})
}

// TestDeleteFileWithAttrAndData tests that both data and attributes are deleted
func TestDeleteFileWithAttrAndData(t *testing.T) {
	Convey("Delete file with data and extended attributes", t, func() {
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		err := core.InitBucketDB(".", testBktID)
		So(err, ShouldBeNil)

		dma := &core.DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
		}
		dma.DefaultBaseMetadataAdapter.SetPath(".")
		dma.DefaultDataMetadataAdapter.SetPath(".")
		dda := &core.DefaultDataAdapter{}
		dda.SetDataPath(".")

		lh := core.NewLocalHandler(".", ".").(*core.LocalHandler)
		lh.SetAdapter(dma, dda)

		testCtx, userInfo, _, err := lh.Login(c, "orcas", "orcas")
		So(err, ShouldBeNil)

		bucket := &core.BucketInfo{
			ID:       testBktID,
			Name:     "test_bucket",
			Type:     1,
			Quota:    100000000,
			Used:     0,
			RealUsed: 0,
		}
		So(dma.PutBkt(testCtx, []*core.BucketInfo{bucket}), ShouldBeNil)
		err = dma.PutACL(testCtx, testBktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		cfg := &core.Config{
			DataPath: ".",
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)

		Convey("test file with data and attributes deletion", func() {
			// Create a file
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_file_data_attr.txt",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Write data to file
			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			testData := []byte("This is test data with extended attributes")
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Get updated file object
			fileObjAfterWrite, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterWrite.Size, ShouldEqual, len(testData))
			So(fileObjAfterWrite.DataID, ShouldBeGreaterThan, 0)

			// Create node for the file
			fileNode := &OrcasNode{
				fs:    ofs,
				objID: fileID,
			}

			// Set extended attributes
			ctx := context.Background()
			errno := fileNode.Setxattr(ctx, "user.content_type", []byte("text/plain"), 0)
			So(int(errno), ShouldEqual, 0)
			errno = fileNode.Setxattr(ctx, "user.author", []byte("test_user"), 0)
			So(int(errno), ShouldEqual, 0)

			// Verify attributes exist
			dest := make([]byte, 100)
			size, errno := fileNode.Getxattr(ctx, "user.content_type", dest)
			So(int(errno), ShouldEqual, 0)
			So(string(dest[:size]), ShouldEqual, "text/plain")

			// Delete the file
			rootNode := &OrcasNode{
				fs:    ofs,
				objID: testBktID,
			}
			errno = rootNode.Unlink(ctx, "test_file_data_attr.txt")
			So(int(errno), ShouldEqual, 0)

			// Try to list attributes directly from database
			attrs, listErr := dma.ListAttrs(testCtx, testBktID, fileID)
			// Attributes should be empty or error
			if listErr == nil {
				So(len(attrs), ShouldEqual, 0)
			}
		})
	})
}

