package core

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetOrCreateWritingVersion(t *testing.T) {
	Convey("GetOrCreateWritingVersion", t, func() {
		InitDB() // Initialize main database first
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		testUID, _ := ig.New()
		InitBucketDB(c, testBktID)

		dma := &DefaultMetadataAdapter{}
		dda := &DefaultDataAdapter{}
		dda.SetOptions(Options{})
		lh := &LocalHandler{
			ma:  dma,
			da:  dda,
			acm: &DefaultAccessCtrlMgr{ma: dma},
		}

		// Create bucket
		bucket := &BucketInfo{
			ID:   testBktID,
			Name: "test",
			UID:  testUID,
			Type: 1,
		}
		So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

		// Create user context for permission checks
		userInfo := &UserInfo{
			ID:   testUID,
			Role: USER,
		}
		testCtx := UserInfo2Ctx(c, userInfo)

		Convey("create new writing version", func() {
			// Create a file first
			fileID, _ := ig.New()
			fileObj := &ObjectInfo{
				ID:     fileID,
				PID:    0,
				Type:   OBJ_TYPE_FILE,
				Name:   "test.txt",
				DataID: EmptyDataID,
				Size:   0,
				MTime:  Now(),
			}
			_, err := lh.Put(testCtx, testBktID, []*ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Get or create writing version
			writingVersion, err := lh.GetOrCreateWritingVersion(testCtx, testBktID, fileID)
			So(err, ShouldBeNil)
			So(writingVersion, ShouldNotBeNil)
			So(writingVersion.Name, ShouldEqual, WritingVersionName)
			So(writingVersion.Type, ShouldEqual, OBJ_TYPE_VERSION)
			So(writingVersion.PID, ShouldEqual, fileID)
			So(writingVersion.ID, ShouldBeGreaterThan, 0)
		})

		Convey("get existing writing version", func() {
			// Create a file first
			fileID, _ := ig.New()
			fileObj := &ObjectInfo{
				ID:     fileID,
				PID:    0,
				Type:   OBJ_TYPE_FILE,
				Name:   "test2.txt",
				DataID: EmptyDataID,
				Size:   0,
				MTime:  Now(),
			}
			_, err := lh.Put(testCtx, testBktID, []*ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Create writing version
			writingVersion1, err := lh.GetOrCreateWritingVersion(testCtx, testBktID, fileID)
			So(err, ShouldBeNil)
			So(writingVersion1, ShouldNotBeNil)

			// Get again - should return same version
			writingVersion2, err := lh.GetOrCreateWritingVersion(testCtx, testBktID, fileID)
			So(err, ShouldBeNil)
			So(writingVersion2, ShouldNotBeNil)
			So(writingVersion2.ID, ShouldEqual, writingVersion1.ID)
			So(writingVersion2.Name, ShouldEqual, WritingVersionName)
		})
	})
}

func TestListVersionsExcludeWriting(t *testing.T) {
	Convey("ListVersions excludeWriting", t, func() {
		InitDB() // Initialize main database first
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		testUID, _ := ig.New()
		InitBucketDB(c, testBktID)

		dma := &DefaultMetadataAdapter{}
		dda := &DefaultDataAdapter{}
		dda.SetOptions(Options{})
		lh := &LocalHandler{
			ma:  dma,
			da:  dda,
			acm: &DefaultAccessCtrlMgr{ma: dma},
		}

		// Create bucket
		bucket := &BucketInfo{
			ID:   testBktID,
			Name: "test",
			UID:  testUID,
			Type: 1,
		}
		So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

		// Create user context for permission checks
		userInfo := &UserInfo{
			ID:   testUID,
			Role: USER,
		}
		testCtx := UserInfo2Ctx(c, userInfo)

		Convey("list versions excluding writing version", func() {
			// Create a file first
			fileID, _ := ig.New()
			fileObj := &ObjectInfo{
				ID:     fileID,
				PID:    0,
				Type:   OBJ_TYPE_FILE,
				Name:   "test.txt",
				DataID: EmptyDataID,
				Size:   0,
				MTime:  Now(),
			}
			_, err := lh.Put(testCtx, testBktID, []*ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Create writing version
			_, err = lh.GetOrCreateWritingVersion(testCtx, testBktID, fileID)
			So(err, ShouldBeNil)

			// Create normal version
			normalVersion := &ObjectInfo{
				ID:     NewID(),
				PID:    fileID,
				Type:   OBJ_TYPE_VERSION,
				Name:   strconv.FormatInt(Now(), 10),
				DataID: EmptyDataID,
				Size:   0,
				MTime:  Now(),
			}
			_, err = lh.Put(testCtx, testBktID, []*ObjectInfo{normalVersion})
			So(err, ShouldBeNil)

			// List all versions (include writing)
			allVersions, err := lh.ma.ListVersions(testCtx, testBktID, fileID, false)
			So(err, ShouldBeNil)
			So(len(allVersions), ShouldEqual, 2)

			// List versions excluding writing
			versions, err := lh.ma.ListVersions(testCtx, testBktID, fileID, true)
			So(err, ShouldBeNil)
			So(len(versions), ShouldEqual, 1)
			So(versions[0].Name, ShouldNotEqual, WritingVersionName)
			So(versions[0].ID, ShouldEqual, normalVersion.ID)
		})
	})
}

func TestUpdateData(t *testing.T) {
	Convey("UpdateData", t, func() {
		InitDB() // Initialize main database first
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		testUID, _ := ig.New()
		InitBucketDB(c, testBktID)

		dma := &DefaultMetadataAdapter{}
		dda := &DefaultDataAdapter{}
		dda.SetOptions(Options{})
		lh := &LocalHandler{
			ma:  dma,
			da:  dda,
			acm: &DefaultAccessCtrlMgr{ma: dma},
		}

		// Create bucket
		bucket := &BucketInfo{
			ID:   testBktID,
			Name: "test",
			UID:  testUID,
			Type: 1,
			Quota: 1000000, // 1MB quota
		}
		So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

		// Create user context for permission checks
		userInfo := &UserInfo{
			ID:   testUID,
			Role: USER,
		}
		testCtx := UserInfo2Ctx(c, userInfo)

		Convey("update data with partial write", func() {
			// Create initial data
			dataID, _ := ig.New()
			initialData := []byte("hello world")
			_, err := lh.PutData(testCtx, testBktID, dataID, 0, initialData)
			So(err, ShouldBeNil)

			// Update part of data
			updateData := []byte("HELLO")
			err = lh.UpdateData(testCtx, testBktID, dataID, 0, 0, updateData)
			So(err, ShouldBeNil)

			// Read back
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0)
			So(err, ShouldBeNil)
			So(string(readData), ShouldEqual, "HELLO world")
		})

		Convey("update data extends chunk", func() {
			// Create initial data
			dataID, _ := ig.New()
			initialData := []byte("hello")
			_, err := lh.PutData(testCtx, testBktID, dataID, 0, initialData)
			So(err, ShouldBeNil)

			// Update beyond current size
			updateData := []byte(" world")
			err = lh.UpdateData(testCtx, testBktID, dataID, 0, 5, updateData)
			So(err, ShouldBeNil)

			// Read back
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0)
			So(err, ShouldBeNil)
			So(string(readData), ShouldEqual, "hello world")
		})

		Convey("update empty buffer", func() {
			// Create initial data
			dataID, _ := ig.New()
			initialData := []byte("hello")
			_, err := lh.PutData(testCtx, testBktID, dataID, 0, initialData)
			So(err, ShouldBeNil)

			// Update with empty buffer
			err = lh.UpdateData(testCtx, testBktID, dataID, 0, 0, []byte{})
			So(err, ShouldBeNil)

			// Data should remain unchanged
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0)
			So(err, ShouldBeNil)
			So(string(readData), ShouldEqual, "hello")
		})
	})
}

func TestSparseFileSupport(t *testing.T) {
	Convey("Sparse file support", t, func() {
		InitDB() // Initialize main database first
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		testUID, _ := ig.New()
		InitBucketDB(c, testBktID)

		dma := &DefaultMetadataAdapter{}
		dda := &DefaultDataAdapter{}
		dda.SetOptions(Options{})
		lh := &LocalHandler{
			ma:  dma,
			da:  dda,
			acm: &DefaultAccessCtrlMgr{ma: dma},
		}

		// Create user context for permission checks
		userInfo := &UserInfo{
			ID:   testUID,
			Role: USER,
		}
		testCtx := UserInfo2Ctx(c, userInfo)

		Convey("sparse file - read missing chunk as zeros", func() {
			// Create bucket
			bucket := &BucketInfo{
				ID:    testBktID,
				Name:  "test",
				UID:   testUID, // Use testUID as bucket owner
				Type:  1,
				Quota: 1000000,
			}
			So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

			// Create sparse data info
			dataID, _ := ig.New()
			dataInfo := &DataInfo{
				ID:       dataID,
				Size:     100,
				OrigSize: 100,
				Kind:     DATA_NORMAL | DATA_SPARSE, // Mark as sparse
			}
			_, err := lh.PutDataInfo(testCtx, testBktID, []*DataInfo{dataInfo})
			So(err, ShouldBeNil)

			// Try to read non-existent chunk - should return zeros
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0, 0, 50)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 50)
			// All should be zeros
			for _, b := range readData {
				So(b, ShouldEqual, 0)
			}
		})

		Convey("non-sparse file - read missing chunk returns error", func() {
			// Create bucket
			bucket := &BucketInfo{
				ID:    testBktID,
				Name:  "test",
				UID:   testUID, // Use testUID as bucket owner
				Type:  1,
				Quota: 1000000,
			}
			So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

			// Create non-sparse data info
			dataID, _ := ig.New()
			dataInfo := &DataInfo{
				ID:       dataID,
				Size:     100,
				OrigSize: 100,
				Kind:     DATA_NORMAL, // Not sparse
			}
			_, err := lh.PutDataInfo(testCtx, testBktID, []*DataInfo{dataInfo})
			So(err, ShouldBeNil)

			// Try to read non-existent chunk - should return error
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0, 0, 50)
			So(err, ShouldNotBeNil)
			So(readData, ShouldBeNil)
		})

		Convey("sparse file - partial read with zeros", func() {
			// Create bucket
			bucket := &BucketInfo{
				ID:    testBktID,
				Name:  "test",
				UID:   testUID, // Use testUID as bucket owner
				Type:  1,
				Quota: 1000000,
			}
			So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

			// Create sparse data info
			dataID, _ := ig.New()
			dataInfo := &DataInfo{
				ID:       dataID,
				Size:     100,
				OrigSize: 100,
				Kind:     DATA_NORMAL | DATA_SPARSE,
			}
			_, err := lh.PutDataInfo(testCtx, testBktID, []*DataInfo{dataInfo})
			So(err, ShouldBeNil)

			// Write partial data
			partialData := []byte("hello")
			_, err = lh.PutData(testCtx, testBktID, dataID, 0, partialData)
			So(err, ShouldBeNil)

			// Read more than written - should pad with zeros
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0, 0, 20)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 20)
			So(string(readData[:5]), ShouldEqual, "hello")
			// Rest should be zeros
			for i := 5; i < 20; i++ {
				So(readData[i], ShouldEqual, 0)
			}
		})

		Convey("IsSparseFile and MarkSparseFile", func() {
			dataInfo := &DataInfo{
				ID:   123,
				Kind: DATA_NORMAL,
			}
			So(IsSparseFile(dataInfo), ShouldBeFalse)

			MarkSparseFile(dataInfo)
			So(IsSparseFile(dataInfo), ShouldBeTrue)
			So(dataInfo.Kind&DATA_SPARSE, ShouldNotEqual, 0)
		})

		Convey("sparse file - read missing chunk with only sn (no offset/size)", func() {
			// Create bucket with chunk size
			bucket := &BucketInfo{
				ID:        testBktID,
				Name:      "test",
				UID:       testUID, // Use testUID as bucket owner
				Type:      1,
				Quota:     1000000,
				ChunkSize: 4 * 1024 * 1024, // 4MB
			}
			So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

			// Create sparse data info
			dataID, _ := ig.New()
			dataInfo := &DataInfo{
				ID:       dataID,
				Size:     10 * 1024 * 1024, // 10MB
				OrigSize: 10 * 1024 * 1024,
				Kind:     DATA_NORMAL | DATA_SPARSE,
			}
			_, err := lh.PutDataInfo(testCtx, testBktID, []*DataInfo{dataInfo})
			So(err, ShouldBeNil)

			// Read missing chunk with only sn - should return chunkSize zeros
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0)
			So(err, ShouldBeNil)
			chunkSize := 4 * 1024 * 1024
			So(len(readData), ShouldEqual, chunkSize) // Should be chunkSize
			// Compare with all-zero array using bytes.Equal
			zeroChunk := make([]byte, chunkSize)
			So(bytes.Equal(readData, zeroChunk), ShouldBeTrue)

			// Read another missing chunk
			readData2, err := lh.GetData(testCtx, testBktID, dataID, 1)
			So(err, ShouldBeNil)
			So(len(readData2), ShouldEqual, chunkSize) // Should be chunkSize
			// Compare with all-zero array using bytes.Equal
			So(bytes.Equal(readData2, zeroChunk), ShouldBeTrue)
		})

		Convey("sparse file - read multiple chunks with gaps", func() {
			// Create bucket
			bucket := &BucketInfo{
				ID:        testBktID,
				Name:      "test",
				UID:       testUID, // Use testUID as bucket owner
				Type:      1,
				Quota:     1000000,
				ChunkSize: 1024 * 1024, // 1MB chunks for easier testing
			}
			So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

			// Create sparse data info (5MB file)
			dataID, _ := ig.New()
			dataInfo := &DataInfo{
				ID:       dataID,
				Size:     5 * 1024 * 1024,
				OrigSize: 5 * 1024 * 1024,
				Kind:     DATA_NORMAL | DATA_SPARSE,
			}
			_, err := lh.PutDataInfo(testCtx, testBktID, []*DataInfo{dataInfo})
			So(err, ShouldBeNil)

			// Write data only to chunk 1 and 3 (skip chunk 0, 2, 4)
			chunk1Data := []byte("chunk1 data")
			_, err = lh.PutData(testCtx, testBktID, dataID, 1, chunk1Data)
			So(err, ShouldBeNil)

			chunk3Data := []byte("chunk3 data")
			_, err = lh.PutData(testCtx, testBktID, dataID, 3, chunk3Data)
			So(err, ShouldBeNil)

			// Read chunk 0 (missing) - should return zeros
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0, 0, 1024*1024)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 1024*1024)
			for _, b := range readData {
				So(b, ShouldEqual, 0)
			}

			// Read chunk 1 (exists) - should return written data
			readData, err = lh.GetData(testCtx, testBktID, dataID, 1, 0, 1024*1024)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 1024*1024)
			So(string(readData[:len(chunk1Data)]), ShouldEqual, string(chunk1Data))
			// Rest should be zeros
			for i := len(chunk1Data); i < len(readData); i++ {
				So(readData[i], ShouldEqual, 0)
			}

			// Read chunk 2 (missing) - should return zeros
			readData, err = lh.GetData(testCtx, testBktID, dataID, 2, 0, 1024*1024)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 1024*1024)
			for _, b := range readData {
				So(b, ShouldEqual, 0)
			}

			// Read chunk 3 (exists) - should return written data
			readData, err = lh.GetData(testCtx, testBktID, dataID, 3, 0, 1024*1024)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 1024*1024)
			So(string(readData[:len(chunk3Data)]), ShouldEqual, string(chunk3Data))
			// Rest should be zeros
			for i := len(chunk3Data); i < len(readData); i++ {
				So(readData[i], ShouldEqual, 0)
			}
		})

		Convey("sparse file - read across chunk boundary", func() {
			// Create bucket with larger quota
			bucket := &BucketInfo{
				ID:        testBktID,
				Name:      "test",
				UID:       testUID, // Use testUID as bucket owner
				Type:      1,
				Quota:     10 * 1024 * 1024, // 10MB quota to accommodate test data
				ChunkSize: 1024 * 1024,      // 1MB chunks
			}
			So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

			// Create sparse data info
			dataID, _ := ig.New()
			dataInfo := &DataInfo{
				ID:       dataID,
				Size:     3 * 1024 * 1024,
				OrigSize: 3 * 1024 * 1024,
				Kind:     DATA_NORMAL | DATA_SPARSE,
			}
			_, err := lh.PutDataInfo(testCtx, testBktID, []*DataInfo{dataInfo})
			So(err, ShouldBeNil)

			// Write data at end of chunk 0 and start of chunk 1
			// Write 512KB at offset 512KB in chunk 0 using UpdateData
			chunk0Data := make([]byte, 512*1024)
			for i := range chunk0Data {
				chunk0Data[i] = byte(i % 256)
			}
			// First create the chunk with zeros, then update at offset 512KB
			err = lh.UpdateData(testCtx, testBktID, dataID, 0, 512*1024, chunk0Data)
			So(err, ShouldBeNil)

			// Read across chunk boundary: from 512KB to 1.5MB (crosses chunk 0 and 1)
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0, 512*1024, 1024*1024)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 1024*1024)

			// First 512KB should be from chunk0Data
			for i := 0; i < 512*1024; i++ {
				So(readData[i], ShouldEqual, chunk0Data[i])
			}

			// Next 512KB should be zeros (from missing chunk 1)
			for i := 512 * 1024; i < 1024*1024; i++ {
				So(readData[i], ShouldEqual, 0)
			}
		})

		Convey("sparse file - read with offset within chunk", func() {
			// Create bucket
			bucket := &BucketInfo{
				ID:        testBktID,
				Name:      "test",
				UID:       testUID, // Use testUID as bucket owner
				Type:      1,
				Quota:     1000000,
				ChunkSize: 1024 * 1024, // 1MB chunks
			}
			So(dma.PutBkt(c, []*BucketInfo{bucket}), ShouldBeNil)

			// Create sparse data info
			dataID, _ := ig.New()
			dataInfo := &DataInfo{
				ID:       dataID,
				Size:     1024 * 1024,
				OrigSize: 1024 * 1024,
				Kind:     DATA_NORMAL | DATA_SPARSE,
			}
			_, err := lh.PutDataInfo(testCtx, testBktID, []*DataInfo{dataInfo})
			So(err, ShouldBeNil)

			// Write data at offset 100KB using UpdateData
			testData := []byte("test data at offset")
			err = lh.UpdateData(testCtx, testBktID, dataID, 0, 100*1024, testData)
			So(err, ShouldBeNil)

			// Read from offset 50KB to 200KB (includes written data and zeros)
			readData, err := lh.GetData(testCtx, testBktID, dataID, 0, 50*1024, 150*1024)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 150*1024)

			// First 50KB should be zeros (before written data)
			for i := 0; i < 50*1024; i++ {
				So(readData[i], ShouldEqual, 0)
			}

			// Next part should be written data (at offset 100KB, which is 50KB into our read buffer)
			So(string(readData[50*1024:50*1024+len(testData)]), ShouldEqual, string(testData))

			// Rest should be zeros
			for i := 50*1024 + len(testData); i < 150*1024; i++ {
				So(readData[i], ShouldEqual, 0)
			}
		})
	})
}
