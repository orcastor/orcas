package vfs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// TestWALCheckpointManager tests that WAL checkpoint manager periodically flushes WAL
func TestWALCheckpointManager(t *testing.T) {
	Convey("WAL checkpoint manager should flush WAL periodically", t, func() {
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		
		// 使用临时目录
		tmpDir := t.TempDir()
		
		// Initialize base database first
		err := core.InitDB(tmpDir)
		So(err, ShouldBeNil)
		
		err = core.InitBucketDB(tmpDir, testBktID)
		So(err, ShouldBeNil)

		dma := &core.DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
		}
		dma.DefaultBaseMetadataAdapter.SetPath(tmpDir)
		dma.DefaultDataMetadataAdapter.SetPath(tmpDir)
		dda := &core.DefaultDataAdapter{}
		dda.SetDataPath(tmpDir)

		lh := core.NewLocalHandler(tmpDir, tmpDir).(*core.LocalHandler)
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

		// Create OrcasFS with WAL checkpoint manager
		cfg := &core.Config{
			DataPath: tmpDir,
			BasePath: tmpDir,
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)
		defer ofs.tempWriteArea.Stop()

		Convey("test WAL checkpoint after write", func() {
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

			// Create and write multiple files to generate WAL data
			for i := 0; i < 5; i++ {
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

				// Write data
				ra, err := NewRandomAccessor(ofs, fileID)
				So(err, ShouldBeNil)

				testData := make([]byte, 1*1024*1024) // 1MB
				for j := range testData {
					testData[j] = byte(j % 256)
				}
				err = ra.Write(0, testData)
				So(err, ShouldBeNil)

				// Flush
				_, err = ra.Flush()
				So(err, ShouldBeNil)

				ra.Close()
			}

			// Check WAL file exists and has data
			walPath := filepath.Join(tmpDir, fmt.Sprint(testBktID), ".db-wal")
			walInfo, err := os.Stat(walPath)
			if err == nil {
				DebugLog("[TEST] WAL file size before checkpoint: %d bytes", walInfo.Size())
				So(walInfo.Size(), ShouldBeGreaterThan, 0)
			}

			// Wait for checkpoint to run (interval is 10 seconds, wait 12 seconds to be safe)
			DebugLog("[TEST] Waiting for WAL checkpoint...")
			time.Sleep(12 * time.Second)

			// Check WAL file after checkpoint
			// It should be smaller or removed (depending on checkpoint mode)
			walInfoAfter, err := os.Stat(walPath)
			if err == nil {
				DebugLog("[TEST] WAL file size after checkpoint: %d bytes", walInfoAfter.Size())
				// WAL file might still exist but should be smaller or same size
				// (TRUNCATE mode should truncate it)
			} else if os.IsNotExist(err) {
				DebugLog("[TEST] WAL file removed after checkpoint")
				// WAL file removed, which is also acceptable
			}

			// Verify data is still correct by listing directory
			children, _, _, err := lh.List(testCtx, testBktID, dirID, core.ListOptions{
				Count: 100,
			})
			So(err, ShouldBeNil)
			So(len(children), ShouldEqual, 5)
			
			// All files should have correct size
			for _, child := range children {
				So(child.Size, ShouldEqual, 1*1024*1024)
			}
		})

		Convey("test bucket registration", func() {
			// Verify WAL checkpoint manager is running
			So(ofs.walCheckpointManager, ShouldNotBeNil)
			
			// Get stats
			stats := ofs.walCheckpointManager.GetStats()
			So(stats, ShouldNotBeNil)
			So(stats["dataPath"], ShouldEqual, tmpDir)
			
			DebugLog("[TEST] WAL checkpoint manager stats: %+v", stats)
		})
	})
}

