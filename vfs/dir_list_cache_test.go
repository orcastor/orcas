package vfs

import (
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// TestDirListCacheConsistency tests that directory listing cache correctly reflects
// file updates even when database has WAL delay
func TestDirListCacheConsistency(t *testing.T) {
	Convey("Directory list cache consistency", t, func() {
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

		// Create OrcasFS with proper DataPath configuration
		cfg := &core.Config{
			DataPath: ".",
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)

		Convey("test file size in directory listing after write", func() {
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

			// Create a file in the directory
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

			// Open file and write data
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

			// Get file object from cache to verify size
			fileObjAfterFlush, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterFlush.Size, ShouldEqual, 5*1024*1024)

			// Now list directory - this should show the updated file size
			// even if database has WAL delay
			children, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children), ShouldEqual, 1)

			// CRITICAL: The file size in directory listing should match the flushed size
			// This tests that we merge file object cache into directory listing
			So(children[0].Size, ShouldEqual, 5*1024*1024)
			So(children[0].Name, ShouldEqual, "test_file.dat")
		})

		Convey("test file mtime in directory listing after write", func() {
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

			// Create a file in the directory
			fileID, _ := ig.New()
			initialMTime := core.Now()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    dirID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_file2.dat",
				DataID: 0,
				Size:   0,
				MTime:  initialMTime,
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Open file and write data
			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// Write data
			testData := []byte("Hello, World!")
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush the data
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Get file object from cache to verify mtime
			fileObjAfterFlush, err := ra.getFileObj()
			So(err, ShouldBeNil)
			// Note: mtime might be equal if flush happens in the same second
			So(fileObjAfterFlush.MTime, ShouldBeGreaterThanOrEqualTo, initialMTime)

			// List directory
			children, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children), ShouldEqual, 1)

			// CRITICAL: The mtime in directory listing should match or be close to the updated mtime
			// Note: mtime might be equal if flush happens very quickly
			So(children[0].MTime, ShouldBeGreaterThanOrEqualTo, initialMTime)
		})

		Convey("test file size after cache invalidation (WAL scenario)", func() {
			// This test specifically checks that fileObjCache is merged into directory listing
			// even when dirListCache is invalidated and we read from database with potential WAL delay
			
			// Create a directory
			dirID, _ := ig.New()
			dirObj := &core.ObjectInfo{
				ID:     dirID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_DIR,
				Name:   "test_dir_wal",
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
				Name:   "test_wal.dat",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Write and flush
			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			testData := make([]byte, 3*1024*1024) // 3MB
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Verify fileObjCache has updated data
			fileObjAfterFlush, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterFlush.Size, ShouldEqual, 3*1024*1024)

			// CRITICAL: Force invalidate dirListCache to simulate cache miss
			// This forces getDirListWithCache to read from database
			// where WAL may not have checkpointed yet
			dirNode := &OrcasNode{
				fs:    ofs,
				objID: dirID,
			}
			dirNode.invalidateDirListCache(dirID)

			// Now list directory - should still show correct size
			// because we merge fileObjCache into directory listing
			children, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children), ShouldEqual, 1)

			// This is the critical test: size should be 3MB, not 0
			// even if database returns stale data due to WAL
			So(children[0].Size, ShouldEqual, 3*1024*1024)
			So(children[0].Name, ShouldEqual, "test_wal.dat")
		})

		Convey("test multiple files in directory listing", func() {
			// Create a directory
			dirID, _ := ig.New()
			dirObj := &core.ObjectInfo{
				ID:     dirID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_DIR,
				Name:   "test_dir3",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{dirObj})
			So(err, ShouldBeNil)

			// Create multiple files
			file1ID, _ := ig.New()
			file1Obj := &core.ObjectInfo{
				ID:     file1ID,
				PID:    dirID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "file1.dat",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			file2ID, _ := ig.New()
			file2Obj := &core.ObjectInfo{
				ID:     file2ID,
				PID:    dirID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "file2.dat",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{file1Obj, file2Obj})
			So(err, ShouldBeNil)

			// Write to file1
			ra1, err := NewRandomAccessor(ofs, file1ID)
			So(err, ShouldBeNil)
			defer ra1.Close()

			testData1 := make([]byte, 1*1024*1024) // 1MB
			err = ra1.Write(0, testData1)
			So(err, ShouldBeNil)
			_, err = ra1.Flush()
			So(err, ShouldBeNil)

			// Write to file2
			ra2, err := NewRandomAccessor(ofs, file2ID)
			So(err, ShouldBeNil)
			defer ra2.Close()

			testData2 := make([]byte, 2*1024*1024) // 2MB
			err = ra2.Write(0, testData2)
			So(err, ShouldBeNil)
			_, err = ra2.Flush()
			So(err, ShouldBeNil)

			// List directory
			children, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children), ShouldEqual, 2)

			// Find each file and verify size
			file1Found := false
			file2Found := false
			for _, child := range children {
				if child.Name == "file1.dat" {
					So(child.Size, ShouldEqual, 1*1024*1024)
					file1Found = true
				} else if child.Name == "file2.dat" {
					So(child.Size, ShouldEqual, 2*1024*1024)
					file2Found = true
				}
			}
			So(file1Found, ShouldBeTrue)
			So(file2Found, ShouldBeTrue)
		})
	})
}

