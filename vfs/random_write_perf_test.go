package vfs

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// BenchmarkRandomWriteSparseFile benchmarks random writes for sparse files (qBittorrent scenario)
func BenchmarkRandomWriteSparseFile(b *testing.B) {
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	core.InitBucketDB(".", testBktID)

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{} // Use default options
	lh := core.NewLocalHandler("", "")
	lh.SetAdapter(dma, dda)

	// Create test file system
	fs := &OrcasFS{
		h:         lh,
		bktID:     testBktID,
		c:         context.TODO(),
		chunkSize: 4 << 20, // 4MB chunks
	}

	// Create a large sparse file (1GB)
	fileID, _ := ig.New()
	fileSize := int64(1 << 30) // 1GB
	fileObj := &core.ObjectInfo{
		ID:     fileID,
		PID:    0,
		Type:   core.OBJ_TYPE_FILE,
		Name:   "test_sparse.bin",
		DataID: core.EmptyDataID,
		Size:   0,
		MTime:  core.Now(),
	}
	_, err := lh.Put(context.TODO(), testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		b.Fatalf("Failed to create file: %v", err)
	}

	// Mark as sparse file and pre-allocate
	ra, err := NewRandomAccessor(fs, fileID)
	if err != nil {
		b.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	ra.MarkSparseFile(fileSize)

	// Pre-allocate file (simulate qBittorrent fallocate)
	updateFileObj := &core.ObjectInfo{
		ID:     fileID,
		DataID: core.EmptyDataID,
		Size:   fileSize,
		MTime:  core.Now(),
	}
	_, err = lh.Put(context.TODO(), testBktID, []*core.ObjectInfo{updateFileObj})
	if err != nil {
		b.Fatalf("Failed to pre-allocate file: %v", err)
	}

	// Create writing version (need to use LocalHandler directly)
	localHandler, ok := lh.(*core.LocalHandler)
	if !ok {
		b.Fatalf("Handler is not LocalHandler")
	}
	writingVersion, err := localHandler.GetOrCreateWritingVersion(context.TODO(), testBktID, fileID)
	if err != nil {
		b.Fatalf("Failed to create writing version: %v", err)
	}

	// Create sparse DataInfo
	dataID := writingVersion.DataID
	if dataID == 0 || dataID == core.EmptyDataID {
		dataID, _ = ig.New()
		dataInfo := &core.DataInfo{
			ID:       dataID,
			Size:     0,
			OrigSize: fileSize,
			Kind:     core.DATA_NORMAL | core.DATA_SPARSE,
		}
		_, err = lh.PutDataInfo(context.TODO(), testBktID, []*core.DataInfo{dataInfo})
		if err != nil {
			b.Fatalf("Failed to create DataInfo: %v", err)
		}

		// Update writing version with DataID
		updateVersion := &core.ObjectInfo{
			ID:     writingVersion.ID,
			DataID: dataID,
			Size:   fileSize,
		}
		err = dma.SetObj(context.TODO(), testBktID, []string{"did", "size"}, updateVersion)
		if err != nil {
			b.Fatalf("Failed to update writing version: %v", err)
		}
	}

	// Statistics
	var totalWrites int64
	var totalBytesWritten int64
	uniqueOffsets := make(map[int64]bool)
	var totalUniqueBytes int64

	b.ResetTimer()
	b.ReportAllocs()

	// Simulate random writes (typical qBittorrent pattern: 16KB-1MB chunks)
	rand.Seed(time.Now().UnixNano())
	writeSizes := []int{16 << 10, 64 << 10, 256 << 10, 1 << 20} // 16KB, 64KB, 256KB, 1MB

	for i := 0; i < b.N; i++ {
		// Random offset within file
		offset := rand.Int63n(fileSize - 1<<20) // Leave 1MB at end
		writeSize := writeSizes[rand.Intn(len(writeSizes))]
		if offset+int64(writeSize) > fileSize {
			writeSize = int(fileSize - offset)
		}

		// Generate random data
		data := make([]byte, writeSize)
		rand.Read(data)

		// Write to RandomAccessor
		err := ra.Write(offset, data)
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}

		// Statistics
		totalWrites++
		totalBytesWritten += int64(writeSize)

		// Track unique offsets (simplified: use chunk-aligned offsets)
		chunkSize := int64(4 << 20) // 4MB chunks
		chunkStart := (offset / chunkSize) * chunkSize
		if !uniqueOffsets[chunkStart] {
			uniqueOffsets[chunkStart] = true
			totalUniqueBytes += int64(writeSize)
		}
	}

	// Final flush
	_, err = ra.Flush()
	if err != nil {
		b.Fatalf("Failed to flush: %v", err)
	}

	// Report statistics
	b.ReportMetric(float64(totalBytesWritten), "bytes-written")
	b.ReportMetric(float64(fileSize), "file-size")
	redundancyRatio := float64(totalBytesWritten) / float64(fileSize)
	b.ReportMetric(redundancyRatio, "write-redundancy-ratio")
	uniqueRatio := float64(len(uniqueOffsets)*4<<20) / float64(fileSize)
	b.ReportMetric(uniqueRatio, "unique-chunks-ratio")
}

// TestRandomWriteSparseFile tests random writes for sparse files
func TestRandomWriteSparseFile(t *testing.T) {
	Convey("Random write sparse file (qBittorrent scenario)", t, func() {
		core.InitDB(".", "")
		ensureTestUser(t)

		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()

		dma := &core.DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
		}
		dma.DefaultBaseMetadataAdapter.SetPath(".")
		dma.DefaultDataMetadataAdapter.SetPath(".")
		dda := &core.DefaultDataAdapter{}

		lh := core.NewLocalHandler("", "").(*core.LocalHandler)
		lh.SetAdapter(dma, dda)

		testCtx, userInfo, _, err := lh.Login(context.Background(), "orcas", "orcas")
		So(err, ShouldBeNil)

		bucket := &core.BucketInfo{
			ID:    testBktID,
			Name:  "test",
			Type:  1,
			Quota: 100 << 30, // 100GB quota
		}
		admin := core.NewLocalAdmin(".", ".")
		So(admin.PutBkt(testCtx, []*core.BucketInfo{bucket}), ShouldBeNil)

		// Ensure user has ALL permission to write to the bucket
		if userInfo != nil && userInfo.ID > 0 {
			So(admin.PutACL(testCtx, testBktID, userInfo.ID, core.ALL), ShouldBeNil)
		}

		fs := &OrcasFS{
			h:         lh,
			bktID:     testBktID,
			c:         testCtx,
			chunkSize: 4 << 20, // 4MB chunks
		}

		fileID, _ := ig.New()
		fileSize := int64(100 << 20) // 100MB
		fileObj := &core.ObjectInfo{
			ID:     fileID,
			PID:    0,
			Type:   core.OBJ_TYPE_FILE,
			Name:   "test_sparse.bin",
			DataID: core.EmptyDataID,
			Size:   0,
			MTime:  core.Now(),
		}
		_, err = lh.Put(testCtx, testBktID, []*core.ObjectInfo{fileObj})
		So(err, ShouldBeNil)

		ra, err := NewRandomAccessor(fs, fileID)
		So(err, ShouldBeNil)
		ra.MarkSparseFile(fileSize)

		updateFileObj := &core.ObjectInfo{
			ID:     fileID,
			DataID: core.EmptyDataID,
			Size:   fileSize,
			MTime:  core.Now(),
		}
		_, err = lh.Put(testCtx, testBktID, []*core.ObjectInfo{updateFileObj})
		So(err, ShouldBeNil)

		writingVersion, err := lh.GetOrCreateWritingVersion(testCtx, testBktID, fileID)
		So(err, ShouldBeNil)
		So(writingVersion.Name, ShouldEqual, core.WritingVersionName)

		dataID := writingVersion.DataID
		if dataID == 0 || dataID == core.EmptyDataID {
			dataID, _ = ig.New()
			dataInfo := &core.DataInfo{
				ID:       dataID,
				Size:     0,
				OrigSize: fileSize,
				Kind:     core.DATA_NORMAL | core.DATA_SPARSE,
			}
			_, err = lh.PutDataInfo(testCtx, testBktID, []*core.DataInfo{dataInfo})
			So(err, ShouldBeNil)

			updateVersion := &core.ObjectInfo{
				ID:     writingVersion.ID,
				DataID: dataID,
				Size:   fileSize,
			}
			err = dma.SetObj(testCtx, testBktID, []string{"did", "size"}, updateVersion)
			So(err, ShouldBeNil)
		}

		Convey("random writes should use writing version", func() {
			rand.Seed(time.Now().UnixNano())
			numWrites := 100
			writeSize := 64 << 10 // 64KB

			for i := 0; i < numWrites; i++ {
				offset := rand.Int63n(fileSize - int64(writeSize))
				data := make([]byte, writeSize)
				rand.Read(data)

				err := ra.Write(offset, data)
				So(err, ShouldBeNil)
			}

			versionID, err := ra.Flush()
			So(err, ShouldBeNil)
			So(versionID, ShouldEqual, 0)

			versions, err := dma.ListVersions(testCtx, testBktID, fileID, false)
			So(err, ShouldBeNil)
			hasWritingVersion := false
			for _, v := range versions {
				if v.Name == core.WritingVersionName {
					hasWritingVersion = true
					break
				}
			}
			So(hasWritingVersion, ShouldBeTrue)
		})

		Convey("read should return written data", func() {
			testData := []byte("test data for sparse file")
			offset := int64(10 << 20) // 10MB offset
			err := ra.Write(offset, testData)
			So(err, ShouldBeNil)

			_, err = ra.Flush()
			So(err, ShouldBeNil)

			readData, err := ra.Read(offset, len(testData))
			So(err, ShouldBeNil)
			So(string(readData), ShouldEqual, string(testData))
		})

		Convey("read sparse regions should return zeros", func() {
			readData, err := ra.Read(50<<20, 1024) // Read 1KB from 50MB offset
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, 1024)
			for _, b := range readData {
				So(b, ShouldEqual, 0)
			}
		})
	})
}

// BenchmarkRandomWriteDeduplication benchmarks write deduplication
func BenchmarkRandomWriteDeduplication(b *testing.B) {
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	core.InitBucketDB(".", testBktID)

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{}
	lh := core.NewLocalHandler("", "")
	lh.SetAdapter(dma, dda)

	// Get LocalHandler for writing version operations
	localHandler, ok := lh.(*core.LocalHandler)
	if !ok {
		b.Fatalf("Handler is not LocalHandler")
	}

	fs := &OrcasFS{
		h:         lh,
		bktID:     testBktID,
		c:         context.TODO(),
		chunkSize: 4 << 20,
	}

	fileID, _ := ig.New()
	fileSize := int64(100 << 20) // 100MB
	fileObj := &core.ObjectInfo{
		ID:     fileID,
		PID:    0,
		Type:   core.OBJ_TYPE_FILE,
		Name:   "test_dedup.bin",
		DataID: core.EmptyDataID,
		Size:   0,
		MTime:  core.Now(),
	}
	_, err := lh.Put(context.TODO(), testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		b.Fatalf("Failed to create file: %v", err)
	}

	ra, err := NewRandomAccessor(fs, fileID)
	if err != nil {
		b.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	ra.MarkSparseFile(fileSize)

	// Pre-allocate
	updateFileObj := &core.ObjectInfo{
		ID:     fileID,
		DataID: core.EmptyDataID,
		Size:   fileSize,
		MTime:  core.Now(),
	}
	_, err = lh.Put(context.TODO(), testBktID, []*core.ObjectInfo{updateFileObj})
	if err != nil {
		b.Fatalf("Failed to pre-allocate: %v", err)
	}

	// Create writing version
	_, err = localHandler.GetOrCreateWritingVersion(context.TODO(), testBktID, fileID)
	if err != nil {
		b.Fatalf("Failed to create writing version: %v", err)
	}

	// Same data written multiple times
	testData := make([]byte, 64<<10) // 64KB
	rand.Seed(42)                    // Fixed seed for reproducibility
	rand.Read(testData)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := int64(i * 64 << 10)
		if offset+int64(len(testData)) > fileSize {
			offset = 0 // Wrap around
		}
		err := ra.Write(offset, testData)
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}
	}

	_, err = ra.Flush()
	if err != nil {
		b.Fatalf("Failed to flush: %v", err)
	}
}
