//go:build !windows
// +build !windows

package vfs

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// TestTmpFileMergeAndDirectoryListing tests that when a .tmp file is renamed to an existing file,
// the .tmp file is properly merged, deleted, and removed from directory listing
func TestTmpFileMergeAndDirectoryListing(t *testing.T) {
	Convey("Test .tmp file merge and directory listing accuracy", t, func() {
		ensureTestUser(t)
		handler := core.NewLocalHandler("", "")
		ctx := context.Background()
		ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
		So(err, ShouldBeNil)

		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		err = core.InitBucketDB(".", testBktID)
		So(err, ShouldBeNil)

		// Get user info for bucket creation
		_, _, _, err = handler.Login(ctx, "orcas", "orcas")
		So(err, ShouldBeNil)

		// Create bucket
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-tmp-merge-bucket",
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Step 1: Create an existing target file with some data
		targetFileName := "test-file.txt"
		targetFileData := []byte("Original file content")
		targetFileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   testBktID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  targetFileName,
			Size:  0,
			MTime: core.Now(),
		}

		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{targetFileObj})
		So(err, ShouldBeNil)

		// Write data to target file
		targetRA, err := NewRandomAccessor(ofs, targetFileObj.ID)
		So(err, ShouldBeNil)
		ofs.registerRandomAccessor(targetFileObj.ID, targetRA)

		err = targetRA.Write(0, targetFileData)
		So(err, ShouldBeNil)
		_, err = targetRA.ForceFlush()
		So(err, ShouldBeNil)

		// Close and unregister target file RandomAccessor
		ofs.unregisterRandomAccessor(targetFileObj.ID, targetRA)
		err = targetRA.Close()
		So(err, ShouldBeNil)

		// Step 2: Create a .tmp file with new data
		tmpFileName := "test-file.txt.tmp"
		tmpFileData := []byte("New content from tmp file")
		tmpFileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   testBktID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  tmpFileName,
			Size:  0,
			MTime: core.Now(),
		}

		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{tmpFileObj})
		So(err, ShouldBeNil)

		// Write data to .tmp file
		tmpRA, err := NewRandomAccessor(ofs, tmpFileObj.ID)
		So(err, ShouldBeNil)
		ofs.registerRandomAccessor(tmpFileObj.ID, tmpRA)

		err = tmpRA.Write(0, tmpFileData)
		So(err, ShouldBeNil)
		_, err = tmpRA.ForceFlush()
		So(err, ShouldBeNil)

		// Step 3: Verify directory listing before rename (should contain both files)
		childrenBefore, _, _, err := handler.List(ctx, testBktID, testBktID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		So(err, ShouldBeNil)

		// Find both files in directory listing
		foundTargetBefore := false
		foundTmpBefore := false
		for _, child := range childrenBefore {
			if child.Name == targetFileName && child.ID == targetFileObj.ID {
				foundTargetBefore = true
			}
			if child.Name == tmpFileName && child.ID == tmpFileObj.ID {
				foundTmpBefore = true
			}
		}
		So(foundTargetBefore, ShouldBeTrue)
		So(foundTmpBefore, ShouldBeTrue)

		// Step 4: Rename .tmp file to target file name (this should trigger merge and delete .tmp file)
		// On non-Windows platforms, root node may not be initialized until Mount is called
		// So we need to initialize it manually for testing
		if ofs.root == nil {
			ofs.root = &OrcasNode{
				fs:     ofs,
				objID:  testBktID,
				isRoot: true,
			}
		}
		rootNode := ofs.root

		// Get target file node (as newParent, which is the same as root in this case)
		targetNode := rootNode

		// Rename .tmp file to target file name
		errno := rootNode.Rename(context.Background(), tmpFileName, targetNode, targetFileName, 0)
		So(int(errno), ShouldEqual, 0)

		// Step 5: Verify directory listing after rename
		// Wait a bit for async operations and delayed double delete to complete
		// Delayed double delete waits 200ms, so wait a bit longer to ensure it completes
		time.Sleep(300 * time.Millisecond)

		// List directory contents
		childrenAfter, _, _, err := handler.List(ctx, testBktID, testBktID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		So(err, ShouldBeNil)

		// Check that .tmp file is NOT in the directory listing
		foundTmpAfter := false
		foundTargetAfter := false
		var finalTargetObj *core.ObjectInfo
		for _, child := range childrenAfter {
			if child.Name == tmpFileName {
				foundTmpAfter = true
				t.Errorf("ERROR: .tmp file still exists in directory listing after rename: %s (ID: %d)", child.Name, child.ID)
			}
			if child.Name == targetFileName && child.ID == targetFileObj.ID {
				foundTargetAfter = true
				finalTargetObj = child
			}
		}
		So(foundTmpAfter, ShouldBeFalse)
		So(foundTargetAfter, ShouldBeTrue)

		// Step 6: Verify target file has new data (from .tmp file)
		So(finalTargetObj, ShouldNotBeNil)
		So(finalTargetObj.Size, ShouldEqual, int64(len(tmpFileData)))

		// Read data from target file to verify it has new content
		finalRA, err := NewRandomAccessor(ofs, targetFileObj.ID)
		So(err, ShouldBeNil)
		ofs.registerRandomAccessor(targetFileObj.ID, finalRA)

		readData, err := finalRA.Read(0, len(tmpFileData))
		So(err, ShouldBeNil)
		So(bytes.Equal(readData, tmpFileData), ShouldBeTrue)

		// Step 7: Verify that .tmp file object is actually deleted from database
		tmpFileObjs, err := handler.Get(ctx, testBktID, []int64{tmpFileObj.ID})
		So(err, ShouldBeNil)
		So(len(tmpFileObjs), ShouldEqual, 0)

		// Cleanup
		ofs.unregisterRandomAccessor(targetFileObj.ID, finalRA)
		err = finalRA.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested .tmp file merge and directory listing: .tmp file deleted, target file updated")
	})
}
