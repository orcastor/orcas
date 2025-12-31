package vfs

import (
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// TestFileObjCacheExpiry tests that file size is correct even after fileObjCache expires
func TestFileObjCacheExpiry(t *testing.T) {
	Convey("File size should be correct after cache expiry", t, func() {
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

		// Create OrcasFS
		cfg := &core.Config{
			DataPath: ".",
			BasePath: ".",
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)

		Convey("test file size after fileObjCache expiry", func() {
			// Create a directory
			dirID, _ := ig.New()
			dirObj := &core.ObjectInfo{
				ID:     dirID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_DIR,
				Name:   "test_dir",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{dirObj})
			So(err, ShouldBeNil)

			// Create a file
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    dirID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_file.dat",
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

			// Write 5MB data
			testData := make([]byte, 5*1024*1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush the data
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Verify file size immediately after flush
			fileObjAfterFlush, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterFlush.Size, ShouldEqual, 5*1024*1024)
			DebugLog("[TEST] File size after flush: %d", fileObjAfterFlush.Size)

			// List directory - should show correct size
			children1, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children1), ShouldEqual, 1)
			So(children1[0].Size, ShouldEqual, 5*1024*1024)
			DebugLog("[TEST] File size in dir list (before cache expiry): %d", children1[0].Size)

			// Clear fileObjCache to simulate cache expiry
			fileObjCache.Del(fileID)
			DebugLog("[TEST] Cleared fileObjCache for fileID=%d", fileID)

			// List directory again - should STILL show correct size
			// This tests the fix: even if fileObjCache expired, we should get data from RandomAccessor
			children2, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children2), ShouldEqual, 1)
			So(children2[0].Size, ShouldEqual, 5*1024*1024)
			DebugLog("[TEST] File size in dir list (after cache cleared, RA still active): %d", children2[0].Size)

			// Close RandomAccessor to simulate file no longer in memory
			ra.Close()
			ofs.unregisterRandomAccessor(fileID, ra)
			DebugLog("[TEST] Closed and unregistered RandomAccessor for fileID=%d", fileID)

			// Wait a bit for WAL to sync (in real scenario, this would be longer)
			time.Sleep(100 * time.Millisecond)

			// List directory again - should STILL show correct size
			// This tests that database has been updated (WAL synced)
			children3, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children3), ShouldEqual, 1)
			So(children3[0].Size, ShouldEqual, 5*1024*1024)
			DebugLog("[TEST] File size in dir list (after RA closed, from DB): %d", children3[0].Size)
		})

		Convey("test file size after long delay (cache and RA both gone)", func() {
			// Create a directory
			dirID, _ := ig.New()
			dirObj := &core.ObjectInfo{
				ID:     dirID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_DIR,
				Name:   "test_dir2",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{dirObj})
			So(err, ShouldBeNil)

			// Create a file
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    dirID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_file2.dat",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Write and flush
			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)

			testData := make([]byte, 3*1024*1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Close RA immediately
			ra.Close()
			ofs.unregisterRandomAccessor(fileID, ra)

			// Clear cache
			fileObjCache.Del(fileID)

			// Wait for WAL to sync
			time.Sleep(200 * time.Millisecond)

			// List directory - should show correct size from database
			children, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children), ShouldEqual, 1)
			So(children[0].Size, ShouldEqual, 3*1024*1024)
			DebugLog("[TEST] File size from DB after delay: %d", children[0].Size)
		})
	})
}

