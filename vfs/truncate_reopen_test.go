package vfs

import (
	"testing"

	"github.com/orcastor/orcas/core"
	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

// TestTruncateAndReopenWrite tests the scenario where:
// 1. A file is uploaded (has data)
// 2. File is truncated to 0 (simulating overwrite)
// 3. New data is written immediately
// This reproduces the bug: "file was renamed from .tmp, RandomAccessor must be recreated"
func TestTruncateAndReopenWrite(t *testing.T) {
	Convey("Truncate and reopen write", t, func() {
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

		lh := core.NewLocalHandler("", "").(*core.LocalHandler)
		lh.SetAdapter(dma, dda)

		testCtx, userInfo, _, err := lh.Login(c, "orcas", "orcas")
		So(err, ShouldBeNil)

		bucket := &core.BucketInfo{
			ID:       testBktID,
			Name:     "test_bucket",
			Type:     1,
			Quota:    100000000, // 100MB
			Used:     0,
			RealUsed: 0,
		}
		So(dma.PutBkt(testCtx, []*core.BucketInfo{bucket}), ShouldBeNil)
		err = dma.PutACL(testCtx, testBktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		cfg := &core.Config{
			DataPath: ".",
			BasePath: ".",
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)

		Convey("test upload then truncate and write", func() {
			// Step 1: Create file with initial data (simulating upload)
			fileID, _ := ig.New()
			initialDataID, _ := ig.New()
			
			initialData := make([]byte, 1024*1024) // 1MB
			for i := range initialData {
				initialData[i] = byte(i % 256)
			}
			
			_, err := lh.PutData(testCtx, testBktID, initialDataID, 0, initialData)
			So(err, ShouldBeNil)

			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "boot.log",
				DataID: initialDataID,
				Size:   int64(len(initialData)),
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Step 2: Truncate to 0 (simulating file overwrite)
			ra1, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)

			_, err = ra1.Truncate(0)
			So(err, ShouldBeNil)

			fileObjAfterTruncate, err := ra1.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterTruncate.Size, ShouldEqual, 0)

			ra1.Close()

			// Step 3: Write new data immediately (simulating new upload)
			// This should NOT fail with "file was renamed from .tmp"
			ra2, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra2.Close()

			newData := []byte("New content after truncate")
			err = ra2.Write(0, newData)
			So(err, ShouldBeNil) // This was failing before the fix

			_, err = ra2.Flush()
			So(err, ShouldBeNil)

			fileObjAfterWrite, err := ra2.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterWrite.Size, ShouldEqual, len(newData))
		})
	})
}

