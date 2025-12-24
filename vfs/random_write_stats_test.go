package vfs

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
)

// TestRandomWriteRedundancy tests random write redundancy for sparse files
func TestRandomWriteRedundancy(t *testing.T) {
	core.InitDB(".", "")
	ensureTestUser(t)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{}
	dda.SetOptions(core.Options{}) // Use default options
	lh := core.NewLocalHandler("", "").(*core.LocalHandler)
	lh.SetAdapter(dma, dda)

	ctx, _, _, err := lh.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	bucket := &core.BucketInfo{
		ID:    testBktID,
		Name:  "test",
		Type:  1,
		Quota: 10 << 30, // 10GB quota
	}
	if err := dma.PutBkt(ctx, []*core.BucketInfo{bucket}); err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	fs := &OrcasFS{
		h:         lh,
		bktID:     testBktID,
		c:         ctx,
		chunkSize: 4 << 20, // 4MB chunks
	}

	// Create a sparse file (1GB)
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
	_, err = lh.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Mark as sparse file and pre-allocate
	ra, err := NewRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	ra.MarkSparseFile(fileSize)

	// Pre-allocate file (simulate qBittorrent fallocate)
	updateFileObj := &core.ObjectInfo{
		ID:     fileID,
		DataID: core.EmptyDataID,
		Size:   fileSize,
		MTime:  core.Now(),
	}
	_, err = lh.Put(ctx, testBktID, []*core.ObjectInfo{updateFileObj})
	if err != nil {
		t.Fatalf("Failed to pre-allocate file: %v", err)
	}

	// Create writing version
	writingVersion, err := lh.GetOrCreateWritingVersion(ctx, testBktID, fileID)
	if err != nil {
		t.Fatalf("Failed to create writing version: %v", err)
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
		_, err = lh.PutDataInfo(ctx, testBktID, []*core.DataInfo{dataInfo})
		if err != nil {
			t.Fatalf("Failed to create DataInfo: %v", err)
		}

		// Update writing version with DataID
		updateVersion := &core.ObjectInfo{
			ID:     writingVersion.ID,
			DataID: dataID,
			Size:   fileSize,
		}
		err = dma.SetObj(ctx, testBktID, []string{"did", "size"}, updateVersion)
		if err != nil {
			t.Fatalf("Failed to update writing version: %v", err)
		}
	}

	// Statistics
	var totalWrites int64
	var totalBytesWritten int64
	writeOffsets := make(map[int64]int64) // offset -> size, to track overlapping writes
	var uniqueWriteBytes int64

	// Simulate random writes (typical qBittorrent pattern: 16KB-1MB chunks)
	rand.Seed(42)                                               // Fixed seed for reproducibility
	writeSizes := []int{16 << 10, 64 << 10, 256 << 10, 1 << 20} // 16KB, 64KB, 256KB, 1MB
	numWrites := 10000                                          // 10K random writes

	startTime := time.Now()

	for i := 0; i < numWrites; i++ {
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
			t.Fatalf("Failed to write: %v", err)
		}

		// Statistics
		totalWrites++
		totalBytesWritten += int64(writeSize)

		// Track unique writes (simplified: check if this offset range overlaps with previous writes)
		writeEnd := offset + int64(writeSize)
		isUnique := true
		for prevOffset, prevSize := range writeOffsets {
			prevEnd := prevOffset + prevSize
			// Check for overlap
			if !(writeEnd <= prevOffset || offset >= prevEnd) {
				// Overlapping write - mark as not unique
				// This is a simplified calculation - in reality, we'd need more sophisticated tracking
				isUnique = false
				break
			}
		}
		if isUnique {
			writeOffsets[offset] = int64(writeSize)
			uniqueWriteBytes += int64(writeSize)
		}
	}

	// Final flush
	flushStart := time.Now()
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	flushDuration := time.Since(flushStart)

	totalDuration := time.Since(startTime)

	// Calculate actual written data by checking data files
	// This is a simplified approach - in production, we'd query the actual data size
	actualDataSize := int64(0)
	chunkSize := int64(4 << 20) // 4MB
	numChunks := (fileSize + chunkSize - 1) / chunkSize
	for sn := 0; sn < int(numChunks); sn++ {
		data, err := lh.GetData(ctx, testBktID, dataID, sn)
		if err == nil && len(data) > 0 {
			actualDataSize += int64(len(data))
		}
	}

	// Report statistics
	fmt.Printf("\n=== Random Write Performance Test Results ===\n")
	fmt.Printf("File Size: %d bytes (%.2f GB)\n", fileSize, float64(fileSize)/(1<<30))
	fmt.Printf("Number of Writes: %d\n", totalWrites)
	fmt.Printf("Total Bytes Written (requests): %d bytes (%.2f GB)\n", totalBytesWritten, float64(totalBytesWritten)/(1<<30))
	fmt.Printf("Unique Write Bytes (estimated): %d bytes (%.2f GB)\n", uniqueWriteBytes, float64(uniqueWriteBytes)/(1<<30))
	fmt.Printf("Actual Data Size (on disk): %d bytes (%.2f GB)\n", actualDataSize, float64(actualDataSize)/(1<<30))
	fmt.Printf("\nWrite Redundancy Ratio: %.2f%% (total writes / file size)\n", float64(totalBytesWritten)*100/float64(fileSize))
	fmt.Printf("Unique Write Ratio: %.2f%% (unique writes / file size)\n", float64(uniqueWriteBytes)*100/float64(fileSize))
	fmt.Printf("Actual Storage Ratio: %.2f%% (actual data / file size)\n", float64(actualDataSize)*100/float64(fileSize))
	fmt.Printf("Write Efficiency: %.2f%% (actual data / total writes)\n", float64(actualDataSize)*100/float64(totalBytesWritten))
	fmt.Printf("\nTotal Time: %v\n", totalDuration)
	fmt.Printf("Flush Time: %v\n", flushDuration)
	fmt.Printf("Average Write Time: %v\n", totalDuration/time.Duration(totalWrites))
	fmt.Printf("Write Throughput: %.2f MB/s\n", float64(totalBytesWritten)/(1<<20)/totalDuration.Seconds())
	fmt.Printf("==========================================\n")
}
