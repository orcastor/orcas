package vfs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	// Disable batch write optimization for tests to ensure immediate flush after each write
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "false")
}

// TestVFSConcurrentUploadReproduceZeroBytes reproduces the issue where 100MB files
// have some zero bytes causing inconsistency with original file
func TestVFSConcurrentUploadReproduceZeroBytes(t *testing.T) {
	Convey("Reproduce zero bytes issue in 100MB file with concurrent upload", t, func() {
		ensureTestUser(t)
		handler := core.NewLocalHandler()
		ctx := context.Background()
		ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
		So(err, ShouldBeNil)

		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		err = core.InitBucketDB(ctx, testBktID)
		So(err, ShouldBeNil)

		// Get user info for bucket creation
		_, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
		So(err, ShouldBeNil)

		// Create bucket with encryption enabled
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-reproduce-zero-bytes-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 10 * 1024 * 1024, // 10MB chunk size
			EndecWay:  core.DATA_ENDEC_AES256,
			EndecKey:  "this-is-a-test-encryption-key-that-is-long-enough-for-aes256-encryption-12345678901234567890",
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Clear bucket config cache
		bucketConfigCache.Del(testBktID)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate 100MB test data with known pattern (to detect zeros)
		fileSize := 100 * 1024 * 1024 // 100MB
		testData := make([]byte, fileSize)
		// Fill with pattern that doesn't contain zeros
		for i := range testData {
			testData[i] = byte((i%255) + 1) // Values 1-255, no zeros
		}

		// Calculate MD5 for verification
		md5Hash := md5.New()
		md5Hash.Write(testData)
		expectedMD5 := md5Hash.Sum(nil)

		// Step 1: Create file object (use .tmp suffix to match business layer behavior)
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-reproduce-zero-100mb.bin.tmp",
			Size:  0,
			MTime: core.Now(),
		}

		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
		So(err, ShouldBeNil)

		// Create RandomAccessor for upload
		ra, err := NewRandomAccessor(ofs, fileObj.ID)
		So(err, ShouldBeNil)
		So(ra, ShouldNotBeNil)

		// Register RandomAccessor
		ofs.registerRandomAccessor(fileObj.ID, ra)

		// Step 2: Concurrent upload - 5 chunks (10MB each) written concurrently
		chunkSize := 10 * 1024 * 1024 // 10MB
		numChunks := 5
		totalChunks := 10 // 100MB / 10MB = 10 chunks total

		var wg sync.WaitGroup
		var writeErrors int64
		var zeroBytePositions []int64
		var zeroByteMu sync.Mutex

		// Write first 5 chunks concurrently
		for i := 0; i < numChunks; i++ {
			wg.Add(1)
			go func(chunkNum int) {
				defer wg.Done()

				offset := int64(chunkNum * chunkSize)
				end := offset + int64(chunkSize)
				if end > int64(fileSize) {
					end = int64(fileSize)
				}

				chunkData := testData[offset:end]
				err := ra.Write(offset, chunkData)
				if err != nil {
					atomic.AddInt64(&writeErrors, 1)
					t.Logf("Chunk %d write error: %v", chunkNum, err)
					return
				}
			}(i)
		}

		// Wait for concurrent writes to complete
		wg.Wait()

		// Wait a bit to ensure all writes are processed
		time.Sleep(2 * time.Second)

		// Step 3: Write remaining chunks sequentially
		for i := numChunks; i < totalChunks; i++ {
			offset := int64(i * chunkSize)
			end := offset + int64(chunkSize)
			if end > int64(fileSize) {
				end = int64(fileSize)
			}

			chunkData := testData[offset:end]
			err := ra.Write(offset, chunkData)
			if err != nil {
				atomic.AddInt64(&writeErrors, 1)
				t.Logf("Chunk %d write error: %v", i, err)
			}
		}

		// Step 4: Flush to ensure all data is uploaded
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Force flush batch writer
		batchMgr := sdk.GetBatchWriterForBucket(ofs.h, ofs.bktID)
		if batchMgr != nil {
			batchMgr.FlushAll(ctx)
		}
		// Wait longer to ensure all data is fully persisted
		time.Sleep(2 * time.Second)

		// Clear caches
		fileObjCache.Del(fileObj.ID)
		dataInfoCache.Del(int64(0))

		// Step 5: Verify file integrity with retry
		var updatedFileObj *core.ObjectInfo
		for retry := 0; retry < 10; retry++ {
			updatedFileObj, err = ra.getFileObj()
			if err == nil && updatedFileObj.Size == int64(fileSize) && updatedFileObj.DataID != 0 && updatedFileObj.DataID != core.EmptyDataID {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		So(err, ShouldBeNil)
		So(updatedFileObj.Size, ShouldEqual, int64(fileSize))

		// Read entire file back with retry
		var readData []byte
		for retry := 0; retry < 10; retry++ {
			readData, err = ra.Read(0, fileSize)
			if err == nil && len(readData) == fileSize {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Step 6: Check for zero bytes (should be none since we used pattern 1-255)
		zeroByteMu.Lock()
		for i, b := range readData {
			if b == 0 {
				zeroBytePositions = append(zeroBytePositions, int64(i))
				// Only collect first 100 positions to avoid too much output
				if len(zeroBytePositions) >= 100 {
					break
				}
			}
		}
		zeroByteMu.Unlock()

		if len(zeroBytePositions) > 0 {
			t.Logf("Found %d zero bytes in file (first 100 positions):", len(zeroBytePositions))
			for i, pos := range zeroBytePositions {
				if i < 20 { // Only log first 20
					// Show context around zero byte
					start := pos - 10
					if start < 0 {
						start = 0
					}
					end := pos + 10
					if end > int64(len(readData)) {
						end = int64(len(readData))
					}
					t.Logf("  Zero byte at position %d, context: %v", pos, readData[start:end])
				}
			}
		}

		// Verify MD5
		md5Hash2 := md5.New()
		md5Hash2.Write(readData)
		actualMD5 := md5Hash2.Sum(nil)

		// Log comparison results
		if !bytes.Equal(actualMD5, expectedMD5) {
			t.Logf("MD5 mismatch detected!")
			t.Logf("Expected MD5: %x", expectedMD5)
			t.Logf("Actual MD5:   %x", actualMD5)
			t.Logf("Zero bytes found: %d", len(zeroBytePositions))
			t.Logf("Write errors: %d", atomic.LoadInt64(&writeErrors))

			// Find first difference
			for i := 0; i < len(testData) && i < len(readData); i++ {
				if testData[i] != readData[i] {
					t.Logf("First difference at position %d: expected %d, got %d", i, testData[i], readData[i])
					// Show context
					start := i - 20
					if start < 0 {
						start = 0
					}
					end := i + 20
					if end > len(testData) {
						end = len(testData)
					}
					t.Logf("Expected context: %v", testData[start:end])
					t.Logf("Actual context:   %v", readData[start:end])
					break
				}
			}
		}

		So(bytes.Equal(actualMD5, expectedMD5), ShouldBeTrue)
		So(len(zeroBytePositions), ShouldEqual, 0)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Test completed: %d bytes, %d zero bytes found, %d write errors", fileSize, len(zeroBytePositions), atomic.LoadInt64(&writeErrors))
	})
}

// TestVFSRepeatedChunkWriteReproduce reproduces the issue where already written chunks
// are deleted and rewritten, causing duplicate data issues
func TestVFSRepeatedChunkWriteReproduce(t *testing.T) {
	Convey("Reproduce repeated chunk write issue", t, func() {
		ensureTestUser(t)
		handler := core.NewLocalHandler()
		ctx := context.Background()
		ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
		So(err, ShouldBeNil)

		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		err = core.InitBucketDB(ctx, testBktID)
		So(err, ShouldBeNil)

		// Get user info for bucket creation
		_, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
		So(err, ShouldBeNil)

		// Create bucket with encryption enabled
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-reproduce-repeated-write-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 10 * 1024 * 1024, // 10MB chunk size
			EndecWay:  core.DATA_ENDEC_AES256,
			EndecKey:  "this-is-a-test-encryption-key-that-is-long-enough-for-aes256-encryption-12345678901234567890",
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Clear bucket config cache
		bucketConfigCache.Del(testBktID)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate 50MB test data with unique markers for each chunk
		fileSize := 50 * 1024 * 1024 // 50MB
		testData := make([]byte, fileSize)
		chunkSize := 10 * 1024 * 1024 // 10MB
		totalChunks := 5

		// Fill each chunk with unique pattern
		for chunkNum := 0; chunkNum < totalChunks; chunkNum++ {
			offset := chunkNum * chunkSize
			end := offset + chunkSize
			if end > fileSize {
				end = fileSize
			}
			// Fill chunk with pattern based on chunk number
			for i := offset; i < end; i++ {
				testData[i] = byte((chunkNum*256 + (i-offset)%256) % 256)
				if testData[i] == 0 {
					testData[i] = 1 // Avoid zeros
				}
			}
		}

		// Calculate MD5 for verification
		md5Hash := md5.New()
		md5Hash.Write(testData)
		expectedMD5 := md5Hash.Sum(nil)

		// Step 1: Create file object (use .tmp suffix to match business layer behavior)
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-reproduce-repeated-50mb.bin.tmp",
			Size:  0,
			MTime: core.Now(),
		}

		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
		So(err, ShouldBeNil)

		// Create RandomAccessor for upload
		ra, err := NewRandomAccessor(ofs, fileObj.ID)
		So(err, ShouldBeNil)
		So(ra, ShouldNotBeNil)

		// Register RandomAccessor
		ofs.registerRandomAccessor(fileObj.ID, ra)

		// Step 2: Write all chunks initially
		for i := 0; i < totalChunks; i++ {
			offset := int64(i * chunkSize)
			end := offset + int64(chunkSize)
			if end > int64(fileSize) {
				end = int64(fileSize)
			}

			chunkData := testData[offset:end]
			err := ra.Write(offset, chunkData)
			So(err, ShouldBeNil)
		}

		// Flush initial writes
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)
		time.Sleep(1 * time.Second)

		// Step 3: Simulate chunk deletion and rewrite (retry scenario)
		// Rewrite chunks 1, 2, and 3 multiple times
		chunksToRewrite := []int{1, 2, 3}
		for rewriteRound := 0; rewriteRound < 3; rewriteRound++ {
			for _, chunkNum := range chunksToRewrite {
				offset := int64(chunkNum * chunkSize)
				end := offset + int64(chunkSize)
				if end > int64(fileSize) {
					end = int64(fileSize)
				}

				// Rewrite the same chunk with the same data
				chunkData := testData[offset:end]
				err := ra.Write(offset, chunkData)
				if err != nil {
					t.Logf("Rewrite round %d, chunk %d error: %v", rewriteRound, chunkNum, err)
				}
			}
			// Small delay between rewrite rounds
			time.Sleep(100 * time.Millisecond)
		}

		// Flush repeated writes
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Force flush batch writer
		batchMgr := sdk.GetBatchWriterForBucket(ofs.h, ofs.bktID)
		if batchMgr != nil {
			batchMgr.FlushAll(ctx)
		}
		time.Sleep(2 * time.Second)

		// Clear caches
		fileObjCache.Del(fileObj.ID)
		dataInfoCache.Del(int64(0))

		// Step 4: Verify file integrity after repeated writes
		var updatedFileObj *core.ObjectInfo
		for retry := 0; retry < 10; retry++ {
			updatedFileObj, err = ra.getFileObj()
			if err == nil && updatedFileObj.Size == int64(fileSize) {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		So(err, ShouldBeNil)
		So(updatedFileObj.Size, ShouldEqual, int64(fileSize))

		// Read entire file back
		var readData []byte
		for retry := 0; retry < 10; retry++ {
			readData, err = ra.Read(0, fileSize)
			if err == nil && len(readData) == fileSize {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Verify each chunk has correct pattern
		for chunkNum := 0; chunkNum < totalChunks; chunkNum++ {
			offset := chunkNum * chunkSize
			end := offset + chunkSize
			if end > fileSize {
				end = fileSize
			}

			readChunk := readData[offset:end]
			expectedChunk := testData[offset:end]

			// Check for pattern mismatch
			mismatches := 0
			firstMismatch := -1
			for i := 0; i < len(readChunk) && i < len(expectedChunk); i++ {
				if readChunk[i] != expectedChunk[i] {
					mismatches++
					if firstMismatch == -1 {
						firstMismatch = i
					}
				}
			}

			if mismatches > 0 {
				t.Logf("Chunk %d has %d mismatches, first at position %d", chunkNum, mismatches, firstMismatch)
				if firstMismatch >= 0 {
					start := offset + firstMismatch - 10
					if start < 0 {
						start = 0
					}
					endPos := offset + firstMismatch + 10
					if endPos > fileSize {
						endPos = fileSize
					}
					t.Logf("Expected: %v", testData[start:endPos])
					t.Logf("Actual:   %v", readData[start:endPos])
				}
			}

			So(bytes.Equal(readChunk, expectedChunk), ShouldBeTrue)
		}

		// Verify MD5
		md5Hash2 := md5.New()
		md5Hash2.Write(readData)
		actualMD5 := md5Hash2.Sum(nil)

		if !bytes.Equal(actualMD5, expectedMD5) {
			t.Logf("MD5 mismatch after repeated writes!")
			t.Logf("Expected MD5: %x", expectedMD5)
			t.Logf("Actual MD5:   %x", actualMD5)
		}

		So(bytes.Equal(actualMD5, expectedMD5), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Repeated write test completed: %d bytes, rewrote chunks %v 3 times", fileSize, chunksToRewrite)
	})
}

// TestVFSConcurrentUploadStressTest runs multiple concurrent uploads to stress test the system
func TestVFSConcurrentUploadStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	Convey("Stress test concurrent uploads", t, func() {
		ensureTestUser(t)
		handler := core.NewLocalHandler()
		ctx := context.Background()
		ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
		So(err, ShouldBeNil)

		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		err = core.InitBucketDB(ctx, testBktID)
		So(err, ShouldBeNil)

		// Get user info for bucket creation
		_, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
		So(err, ShouldBeNil)

		// Create bucket with encryption enabled
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-stress-concurrent-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 10 * 1024 * 1024, // 10MB chunk size
			EndecWay:  core.DATA_ENDEC_AES256,
			EndecKey:  "this-is-a-test-encryption-key-that-is-long-enough-for-aes256-encryption-12345678901234567890",
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Clear bucket config cache
		bucketConfigCache.Del(testBktID)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Run multiple test iterations
		numIterations := 3
		fileSize := 100 * 1024 * 1024 // 100MB
		chunkSize := 10 * 1024 * 1024 // 10MB
		numChunks := 5

		for iteration := 0; iteration < numIterations; iteration++ {
			t.Logf("Starting iteration %d/%d", iteration+1, numIterations)

			// Generate test data
			testData := make([]byte, fileSize)
			_, err = rand.Read(testData)
			So(err, ShouldBeNil)

			// Calculate MD5
			md5Hash := md5.New()
			md5Hash.Write(testData)
			expectedMD5 := md5Hash.Sum(nil)

			// Create file object (use .tmp suffix to match business layer behavior)
			fileObj := &core.ObjectInfo{
				ID:    core.NewID(),
				PID:   core.ROOT_OID,
				Type:  core.OBJ_TYPE_FILE,
				Name:  fmt.Sprintf("test-stress-%d.bin.tmp", iteration),
				Size:  0,
				MTime: core.Now(),
			}

			_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Create RandomAccessor
			ra, err := NewRandomAccessor(ofs, fileObj.ID)
			So(err, ShouldBeNil)
			ofs.registerRandomAccessor(fileObj.ID, ra)

			// Concurrent write
			var wg sync.WaitGroup
			for i := 0; i < numChunks; i++ {
				wg.Add(1)
				go func(chunkNum int) {
					defer wg.Done()
					offset := int64(chunkNum * chunkSize)
					end := offset + int64(chunkSize)
					if end > int64(fileSize) {
						end = int64(fileSize)
					}
					chunkData := testData[offset:end]
					ra.Write(offset, chunkData)
				}(i)
			}
			wg.Wait()

			// Write remaining chunks
			totalChunks := 10
			for i := numChunks; i < totalChunks; i++ {
				offset := int64(i * chunkSize)
				end := offset + int64(chunkSize)
				if end > int64(fileSize) {
					end = int64(fileSize)
				}
				chunkData := testData[offset:end]
				ra.Write(offset, chunkData)
			}

			// Flush
			ra.ForceFlush()
			batchMgr := sdk.GetBatchWriterForBucket(ofs.h, ofs.bktID)
			if batchMgr != nil {
				batchMgr.FlushAll(ctx)
			}
			time.Sleep(2 * time.Second)

			// Verify
			fileObjCache.Del(fileObj.ID)
			readData, err := ra.Read(0, fileSize)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, fileSize)

			md5Hash2 := md5.New()
			md5Hash2.Write(readData)
			actualMD5 := md5Hash2.Sum(nil)

			if !bytes.Equal(actualMD5, expectedMD5) {
				t.Errorf("Iteration %d: MD5 mismatch!", iteration+1)
			}

			ofs.unregisterRandomAccessor(fileObj.ID, ra)
			ra.Close()
		}

		t.Logf("Stress test completed: %d iterations", numIterations)
	})
}

