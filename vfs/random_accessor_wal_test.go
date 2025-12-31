package vfs

import (
	"bytes"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// TestFileSizeConsistencyAfterFlush tests that file size remains consistent after flush operations
// This test is designed to catch WAL dirty read issues where file size might revert to old values
func TestFileSizeConsistencyAfterFlush(t *testing.T) {
	Convey("File size consistency after flush operations", t, func() {
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

		lh := core.NewLocalHandler("", "").(*core.LocalHandler)
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

		ofs := NewOrcasFS(lh, testCtx, testBktID)

		Convey("test sequential write file size consistency", func() {
			// Create a file and write 8MB data sequentially
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_sequential.dat",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// Write 8MB data
			dataSize := 8 * 1024 * 1024
			testData := make([]byte, dataSize)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush and check size
			versionID, err := ra.Flush()
			So(err, ShouldBeNil)
			So(versionID, ShouldBeGreaterThan, 0)

			// Get file object and verify size
			objs, err := lh.Get(testCtx, testBktID, []int64{fileID})
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 1)
			So(objs[0].Size, ShouldEqual, dataSize)

			// Read back and verify
			readData, err := ra.Read(0, dataSize)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, dataSize)
			So(bytes.Equal(readData, testData), ShouldBeTrue)
		})

		Convey("test random write file size consistency", func() {
			// Create a file with initial data
			fileID, _ := ig.New()
			initialSize := 1024 * 1024 // 1MB
			initialData := make([]byte, initialSize)
			for i := range initialData {
				initialData[i] = byte(i % 256)
			}

			dataID, _ := ig.New()
			dataInfo := &core.DataInfo{
				ID:       dataID,
				Size:     int64(initialSize),
				OrigSize: int64(initialSize),
				Kind:     core.DATA_NORMAL,
			}
			So(dma.PutData(testCtx, testBktID, []*core.DataInfo{dataInfo}), ShouldBeNil)
			So(dda.Write(testCtx, testBktID, dataID, 0, initialData), ShouldBeNil)

			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_random.dat",
				DataID: dataID,
				Size:   int64(initialSize),
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// Write at offset to extend file
			extendSize := 8 * 1024 * 1024 // 8MB
			extendData := make([]byte, 4096)
			for i := range extendData {
				extendData[i] = byte(255 - i%256)
			}

			err = ra.Write(int64(extendSize-4096), extendData)
			So(err, ShouldBeNil)

			// Flush and check size
			versionID, err := ra.Flush()
			So(err, ShouldBeNil)
			So(versionID, ShouldBeGreaterThan, 0)

			// Get file object and verify size is extended
			objs, err := lh.Get(testCtx, testBktID, []int64{fileID})
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 1)
			So(objs[0].Size, ShouldEqual, extendSize)
		})

	Convey("test writing version file size consistency", func() {
		// Create a sparse file (uses writing version)
		fileID, _ := ig.New()
		fileObj := &core.ObjectInfo{
			ID:     fileID,
			PID:    testBktID,
			Type:   core.OBJ_TYPE_FILE,
			Name:   "test_sparse.dat",
			DataID: 0,
			Size:   0,
			MTime:  core.Now(),
		}
		_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
		So(err, ShouldBeNil)

		ra, err := NewRandomAccessor(ofs, fileID)
		So(err, ShouldBeNil)
		defer ra.Close()

		// Pre-allocate sparse file
		sparseSize := int64(100 * 1024 * 1024) // 100MB
		_, err = ra.Truncate(sparseSize)
		So(err, ShouldBeNil)

		// Write some data at different offsets
		testData1 := []byte("Hello at offset 0")
		err = ra.Write(0, testData1)
		So(err, ShouldBeNil)

		testData2 := []byte("World at 10MB")
		err = ra.Write(10*1024*1024, testData2)
		So(err, ShouldBeNil)

		// Flush and check size
		_, err = ra.Flush()
		So(err, ShouldBeNil)

		// Get file object and verify size
		// Note: After writing to sparse file, the size should be at least the end of last write
		objs, err := lh.Get(testCtx, testBktID, []int64{fileID})
		So(err, ShouldBeNil)
		So(len(objs), ShouldEqual, 1)
		// Size should be at least the end of the last write (10MB + len(testData2))
		expectedMinSize := int64(10*1024*1024 + len(testData2))
		So(objs[0].Size, ShouldBeGreaterThanOrEqualTo, expectedMinSize)

		// Verify data can be read back correctly
		readData1, err := ra.Read(0, len(testData1))
		So(err, ShouldBeNil)
		So(bytes.Equal(readData1, testData1), ShouldBeTrue)

		readData2, err := ra.Read(10*1024*1024, len(testData2))
		So(err, ShouldBeNil)
		So(bytes.Equal(readData2, testData2), ShouldBeTrue)
	})

		Convey("test multiple flush operations size consistency", func() {
			// Test that multiple flush operations maintain correct size
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_multi_flush.dat",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// First write and flush
			data1 := make([]byte, 1024*1024) // 1MB
			for i := range data1 {
				data1[i] = byte(i % 256)
			}
			err = ra.Write(0, data1)
			So(err, ShouldBeNil)

			versionID1, err := ra.Flush()
			So(err, ShouldBeNil)
			So(versionID1, ShouldBeGreaterThan, 0)

			// Verify size after first flush
			objs, err := lh.Get(testCtx, testBktID, []int64{fileID})
			So(err, ShouldBeNil)
			So(objs[0].Size, ShouldEqual, 1024*1024)

			// Second write and flush (extend)
			data2 := make([]byte, 2*1024*1024) // 2MB
			for i := range data2 {
				data2[i] = byte((i + 100) % 256)
			}
			err = ra.Write(1024*1024, data2)
			So(err, ShouldBeNil)

			versionID2, err := ra.Flush()
			So(err, ShouldBeNil)
			So(versionID2, ShouldBeGreaterThan, versionID1)

			// Verify size after second flush
			objs, err = lh.Get(testCtx, testBktID, []int64{fileID})
			So(err, ShouldBeNil)
			So(objs[0].Size, ShouldEqual, 3*1024*1024)

			// Third write and flush (partial overwrite, size should not change)
			data3 := make([]byte, 512*1024) // 512KB
			for i := range data3 {
				data3[i] = byte((i + 200) % 256)
			}
			err = ra.Write(512*1024, data3)
			So(err, ShouldBeNil)

			versionID3, err := ra.Flush()
			So(err, ShouldBeNil)
			So(versionID3, ShouldBeGreaterThan, versionID2)

			// Verify size remains the same after partial overwrite
			objs, err = lh.Get(testCtx, testBktID, []int64{fileID})
			So(err, ShouldBeNil)
			So(objs[0].Size, ShouldEqual, 3*1024*1024)
		})
	})
}

// TestCacheConsistencyAfterWrites tests that cache remains consistent with database after write operations
// This specifically tests for WAL dirty read issues
func TestCacheConsistencyAfterWrites(t *testing.T) {
	Convey("Cache consistency after write operations", t, func() {
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

		lh := core.NewLocalHandler("", "").(*core.LocalHandler)
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

		ofs := NewOrcasFS(lh, testCtx, testBktID)

		Convey("test cache matches database after sequential flush", func() {
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_cache_seq.dat",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// Write data
			testData := make([]byte, 5*1024*1024) // 5MB
			for i := range testData {
				testData[i] = byte(i % 256)
			}
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Get from database
			objsFromDB, err := lh.Get(testCtx, testBktID, []int64{fileID})
			So(err, ShouldBeNil)
			So(len(objsFromDB), ShouldEqual, 1)

			// Get from cache (via getFileObj)
			cachedObj, err := ra.getFileObj()
			So(err, ShouldBeNil)

			// Verify they match
			So(cachedObj.Size, ShouldEqual, objsFromDB[0].Size)
			So(cachedObj.DataID, ShouldEqual, objsFromDB[0].DataID)
			So(cachedObj.Size, ShouldEqual, len(testData))
		})

		Convey("test cache matches database after random flush", func() {
			// Create file with initial data
			fileID, _ := ig.New()
			initialData := make([]byte, 2*1024*1024) // 2MB
			for i := range initialData {
				initialData[i] = byte(i % 256)
			}

			dataID, _ := ig.New()
			dataInfo := &core.DataInfo{
				ID:       dataID,
				Size:     int64(len(initialData)),
				OrigSize: int64(len(initialData)),
				Kind:     core.DATA_NORMAL,
			}
			So(dma.PutData(testCtx, testBktID, []*core.DataInfo{dataInfo}), ShouldBeNil)
			So(dda.Write(testCtx, testBktID, dataID, 0, initialData), ShouldBeNil)

			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test_cache_random.dat",
				DataID: dataID,
				Size:   int64(len(initialData)),
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// Random write to extend file
			extendData := make([]byte, 1024*1024) // 1MB
			for i := range extendData {
				extendData[i] = byte(255 - i%256)
			}
			err = ra.Write(5*1024*1024, extendData) // Write at 5MB
			So(err, ShouldBeNil)

			// Flush
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Get from database
			objsFromDB, err := lh.Get(testCtx, testBktID, []int64{fileID})
			So(err, ShouldBeNil)
			So(len(objsFromDB), ShouldEqual, 1)

			// Get from cache
			cachedObj, err := ra.getFileObj()
			So(err, ShouldBeNil)

			// Verify they match
			So(cachedObj.Size, ShouldEqual, objsFromDB[0].Size)
			So(cachedObj.DataID, ShouldEqual, objsFromDB[0].DataID)
			expectedSize := int64(6 * 1024 * 1024) // 5MB + 1MB
			So(cachedObj.Size, ShouldEqual, expectedSize)
			So(objsFromDB[0].Size, ShouldEqual, expectedSize)
		})

		Convey("test TempFileWriter not recreated after file rename", func() {
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "test.tmp", // Start with .tmp file
				DataID: core.EmptyDataID,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			ra, err := NewRandomAccessor(NewOrcasFS(lh, testCtx, testBktID), fileID)
			So(err, ShouldBeNil)

			// Write some data to .tmp file (should create TempFileWriter)
			testData1 := bytes.Repeat([]byte("A"), 4096)
			err = ra.Write(0, testData1)
			So(err, ShouldBeNil)

			// Verify TempFileWriter was created
			tempWriterVal1 := ra.tempWriter.Load()
			So(tempWriterVal1, ShouldNotBeNil)
			So(tempWriterVal1, ShouldNotEqual, clearedTempWriterMarker)

			// Rename file from .tmp to normal name
			fileObj.Name = "test.txt"
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Update cache with renamed file object
			fileObjCache.Put(ra.fileObjKey, fileObj)

			// Verify cache was updated
			cachedFileObj, ok := fileObjCache.Get(ra.fileObjKey)
			So(ok, ShouldBeTrue)
			So(cachedFileObj.(*core.ObjectInfo).Name, ShouldEqual, "test.txt")

			// IMPORTANT: Also update the local atomic.Value cache in RandomAccessor
			// This is critical because Write() checks the local cache first
			ra.fileObj.Store(fileObj)

			// Try to write again (should fail because file is no longer .tmp)
			testData2 := bytes.Repeat([]byte("B"), 4096)
			err = ra.Write(4096, testData2)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "renamed from .tmp")

			// Verify TempFileWriter was cleared
			tempWriterVal := ra.tempWriter.Load()
			So(tempWriterVal, ShouldEqual, clearedTempWriterMarker)

			// Close and reopen RandomAccessor
			ra.Close()

			// Create new RandomAccessor for the renamed file
			ra2, err := NewRandomAccessor(NewOrcasFS(lh, testCtx, testBktID), fileID)
			So(err, ShouldBeNil)
			defer ra2.Close()

			// Write should now work with normal write path (not TempFileWriter)
			testData3 := bytes.Repeat([]byte("C"), 4096)
			err = ra2.Write(0, testData3)
			So(err, ShouldBeNil)

			// Flush and verify
			_, err = ra2.Flush()
			So(err, ShouldBeNil)

			fileObjAfterFlush, err := ra2.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterFlush.Size, ShouldEqual, 4096)
		})

		Convey("test TempFileWriter size preserved when recreated", func() {
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "recreate_test.txt", // Use non-.tmp file to test normal flush
				DataID: core.EmptyDataID,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			ra, err := NewRandomAccessor(NewOrcasFS(lh, testCtx, testBktID), fileID)
			So(err, ShouldBeNil)

			// Write 8MB data
			dataSize := int64(8 * 1024 * 1024)
			testData := bytes.Repeat([]byte("X"), int(dataSize))
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush to persist data
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Verify size is correct
			fileObjAfterFlush, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterFlush.Size, ShouldEqual, dataSize)

			// Close RandomAccessor
			ra.Close()

			// Create new RandomAccessor (simulating recreation)
			ra2, err := NewRandomAccessor(NewOrcasFS(lh, testCtx, testBktID), fileID)
			So(err, ShouldBeNil)
			defer ra2.Close()

			// Write additional 1MB data
			additionalSize := int64(1 * 1024 * 1024)
			additionalData := bytes.Repeat([]byte("Y"), int(additionalSize))
			err = ra2.Write(dataSize, additionalData)
			So(err, ShouldBeNil)

			// Flush again
			_, err = ra2.Flush()
			So(err, ShouldBeNil)

			// Verify size is 8MB + 1MB = 9MB, not just 1MB
			fileObjAfterFlush2, err := ra2.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfterFlush2.Size, ShouldEqual, dataSize+additionalSize)

			// Verify from database
			objs, err := dma.GetObj(testCtx, testBktID, []int64{fileID})
			So(err, ShouldBeNil)
			So(objs[0].Size, ShouldEqual, dataSize+additionalSize)
		})
	})
}

