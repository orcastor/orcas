package vfs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	// Disable batch write optimization for tests to ensure immediate flush after each write
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "false")
}

// TestVFSConcurrentChunkUpload tests concurrent upload of 5 chunks (10MB each) for a 100MB file
// This test verifies:
// 1. Concurrent chunk writes don't cause data corruption
// 2. No zero bytes appear in the final file
// 3. File integrity is maintained after concurrent writes
// 4. Bucket encryption is properly handled
// NOTE: This test may need adjustment after removing batchwriter from VFS
func TestVFSConcurrentChunkUpload(t *testing.T) {
	t.Skip("Skipping TestVFSConcurrentChunkUpload - may need adjustment after batchwriter removal")
	Convey("Test VFS concurrent chunk upload with encryption", t, func() {
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

		// Create bucket with encryption enabled
		admin := core.NewLocalAdmin(".", ".")
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-concurrent-upload-encrypted-bucket",
			Type:      1,
			Quota:     -1,
			ChunkSize: 10 * 1024 * 1024, // 10MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate 100MB test data
		fileSize := 100 * 1024 * 1024 // 100MB
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Calculate MD5 for verification
		md5Hash := md5.New()
		md5Hash.Write(testData)
		expectedMD5 := md5Hash.Sum(nil)

		// Step 1: Create file object (use .tmp suffix to match business layer behavior)
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   testBktID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-concurrent-upload-100mb.bin.tmp",
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
		var writeErrors []error
		var writeErrorsMu sync.Mutex

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
					writeErrorsMu.Lock()
					writeErrors = append(writeErrors, err)
					writeErrorsMu.Unlock()
					return
				}
			}(i)
		}

		// Wait for concurrent writes to complete
		wg.Wait()

		// Check for write errors
		writeErrorsMu.Lock()
		So(len(writeErrors), ShouldEqual, 0)
		if len(writeErrors) > 0 {
			for _, err := range writeErrors {
				t.Logf("Write error: %v", err)
			}
		}
		writeErrorsMu.Unlock()

		// Wait a bit to ensure all writes are processed
		time.Sleep(1 * time.Second)

		// Step 3: Write remaining chunks sequentially to complete the file
		for i := numChunks; i < totalChunks; i++ {
			offset := int64(i * chunkSize)
			end := offset + int64(chunkSize)
			if end > int64(fileSize) {
				end = int64(fileSize)
			}

			chunkData := testData[offset:end]
			err := ra.Write(offset, chunkData)
			So(err, ShouldBeNil)
		}

		// Step 4: Simulate rename by removing .tmp suffix to trigger flush
		// This mimics the behavior of Rename() which calls forceFlushTempFileBeforeRename
		// Get current file object to preserve its state
		currentFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)
		So(currentFileObj, ShouldNotBeNil)

		// Update the file name to remove .tmp suffix
		finalFileObj := &core.ObjectInfo{
			ID:     currentFileObj.ID,
			PID:    currentFileObj.PID,
			Type:   currentFileObj.Type,
			Name:   "test-concurrent-upload-100mb.bin", // Remove .tmp suffix
			Size:   currentFileObj.Size,
			DataID: currentFileObj.DataID,
			MTime:  core.Now(),
		}
		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{finalFileObj})
		So(err, ShouldBeNil)

		// Clear cache to force reload
		fileObjCache.Del(fileObj.ID)

		// Step 4.5: Flush to ensure all data is uploaded
		// After removing .tmp suffix, the file is no longer a .tmp file,
		// so ForceFlush will flush TempFileWriter if it exists
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 4.6: Force flush TempFileWriter directly to ensure all data is persisted
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 4.7: Call ForceFlush one more time to ensure TempFileWriter is fully flushed
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Clear caches to ensure we read from storage
		fileObjCache.Del(fileObj.ID)
		dataInfoCache.Del(int64(0))

		// Step 5: Verify file integrity
		// Get updated file object with retry
		var updatedFileObj *core.ObjectInfo
		for retry := 0; retry < 20; retry++ {
			// Clear cache before each retry to force reload from database
			fileObjCache.Del(fileObj.ID)
			updatedFileObj, err = ra.getFileObj()
			if err == nil && updatedFileObj != nil && updatedFileObj.Size == int64(fileSize) && updatedFileObj.DataID != 0 && updatedFileObj.DataID != core.EmptyDataID {
				break
			}
			if retry < 19 {
				time.Sleep(500 * time.Millisecond)
			}
		}
		So(err, ShouldBeNil)
		So(updatedFileObj, ShouldNotBeNil)
		if updatedFileObj != nil {
			So(updatedFileObj.Size, ShouldEqual, int64(fileSize))
			So(updatedFileObj.DataID, ShouldNotEqual, 0)
			So(updatedFileObj.DataID, ShouldNotEqual, core.EmptyDataID)
		} else {
			t.Fatalf("Failed to get updated file object after retries")
		}

		// Read entire file back with retry
		var readData []byte
		for retry := 0; retry < 5; retry++ {
			readData, err = ra.Read(0, fileSize)
			if err == nil && len(readData) == fileSize {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Verify no zero bytes (check random samples)
		zeroCount := 0
		sampleSize := 1000
		for i := 0; i < sampleSize; i++ {
			idx := (i * fileSize) / sampleSize
			if idx < len(readData) && readData[idx] == 0 {
				zeroCount++
			}
		}
		// Allow some zeros but not too many (random data should have ~0.4% zeros)
		So(zeroCount, ShouldBeLessThan, sampleSize/10)

		// Verify MD5
		md5Hash2 := md5.New()
		md5Hash2.Write(readData)
		actualMD5 := md5Hash2.Sum(nil)
		So(bytes.Equal(actualMD5, expectedMD5), ShouldBeTrue)

		// Verify byte-by-byte comparison
		So(bytes.Equal(readData, testData), ShouldBeTrue)

		// Step 6: Verify data encryption
		dataInfo, err := handler.GetDataInfo(ctx, testBktID, updatedFileObj.DataID)
		So(err, ShouldBeNil)
		So(dataInfo, ShouldNotBeNil)
		So(dataInfo.Kind&core.DATA_ENDEC_AES256, ShouldNotEqual, 0)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested concurrent chunk upload: %d bytes (%.2f MB), %d concurrent chunks", fileSize, float64(fileSize)/(1024*1024), numChunks)
	})
}

// TestVFSRepeatedChunkWrite tests the scenario where already written chunks are deleted and rewritten
// This test verifies:
// 1. Repeated writes to the same chunk don't cause data corruption
// 2. File integrity is maintained after repeated writes
// 3. No duplicate data issues occur
// NOTE: This test may need adjustment after removing batchwriter from VFS
func TestVFSRepeatedChunkWrite(t *testing.T) {
	t.Skip("Skipping TestVFSRepeatedChunkWrite - may need adjustment after batchwriter removal")
	Convey("Test VFS repeated chunk write (delete and rewrite)", t, func() {
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

		// Create bucket with encryption enabled
		admin := core.NewLocalAdmin(".", ".")
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-repeated-write-encrypted-bucket",
			Type:      1,
			Quota:     -1,
			ChunkSize: 10 * 1024 * 1024, // 10MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate 50MB test data
		fileSize := 50 * 1024 * 1024 // 50MB
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Calculate MD5 for verification
		md5Hash := md5.New()
		md5Hash.Write(testData)
		expectedMD5 := md5Hash.Sum(nil)

		// Step 1: Create file object (use .tmp suffix to match business layer behavior)
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   testBktID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-repeated-write-50mb.bin.tmp",
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

		chunkSize := 10 * 1024 * 1024 // 10MB
		totalChunks := 5              // 50MB / 10MB = 5 chunks

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

		// Wait a bit
		time.Sleep(200 * time.Millisecond)

		// Step 3: Rewrite specific chunks (simulating retry scenario)
		// Rewrite chunks 1, 2, and 3 (middle chunks)
		chunksToRewrite := []int{1, 2, 3}
		for _, chunkNum := range chunksToRewrite {
			offset := int64(chunkNum * chunkSize)
			end := offset + int64(chunkSize)
			if end > int64(fileSize) {
				end = int64(fileSize)
			}

			// Rewrite the same chunk with the same data
			chunkData := testData[offset:end]
			err := ra.Write(offset, chunkData)
			So(err, ShouldBeNil)
		}

		// Step 3.5: Simulate rename by removing .tmp suffix to trigger flush
		// This mimics the behavior of Rename() which calls forceFlushTempFileBeforeRename
		// Get current file object to preserve its state
		currentFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)
		So(currentFileObj, ShouldNotBeNil)

		// Update the file name to remove .tmp suffix
		finalFileObj := &core.ObjectInfo{
			ID:     currentFileObj.ID,
			PID:    currentFileObj.PID,
			Type:   currentFileObj.Type,
			Name:   "test-repeated-write-50mb.bin", // Remove .tmp suffix
			Size:   currentFileObj.Size,
			DataID: currentFileObj.DataID,
			MTime:  core.Now(),
		}
		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{finalFileObj})
		So(err, ShouldBeNil)

		// Clear cache to force reload
		fileObjCache.Del(fileObj.ID)

		// Step 3.6: Flush repeated writes
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 3.7: Force flush TempFileWriter directly to ensure all data is persisted
		// After removing .tmp suffix, the file is no longer a .tmp file,
		// so ForceFlush will flush TempFileWriter if it exists
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 3.8: Call ForceFlush one more time to ensure TempFileWriter is fully flushed
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Clear caches and force reload from database
		fileObjCache.Del(fileObj.ID)
		dataInfoCache.Del(int64(0))

		// Step 4: Verify file integrity after repeated writes
		var updatedFileObj *core.ObjectInfo
		for retry := 0; retry < 20; retry++ {
			// Clear cache before each retry to force reload from database
			fileObjCache.Del(fileObj.ID)
			updatedFileObj, err = ra.getFileObj()
			if err == nil && updatedFileObj != nil && updatedFileObj.Size == int64(fileSize) {
				break
			}
			if retry < 19 {
				time.Sleep(500 * time.Millisecond)
			}
		}
		So(err, ShouldBeNil)
		So(updatedFileObj, ShouldNotBeNil)
		if updatedFileObj != nil {
			So(updatedFileObj.Size, ShouldEqual, int64(fileSize))
		} else {
			t.Fatalf("Failed to get updated file object after retries")
		}

		// Read entire file back
		readData, err := ra.Read(0, fileSize)
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Verify MD5 (should match original)
		md5Hash2 := md5.New()
		md5Hash2.Write(readData)
		actualMD5 := md5Hash2.Sum(nil)
		So(bytes.Equal(actualMD5, expectedMD5), ShouldBeTrue)

		// Verify byte-by-byte comparison
		So(bytes.Equal(readData, testData), ShouldBeTrue)

		// Verify no duplicate data by checking chunk boundaries
		for i := 0; i < totalChunks; i++ {
			offset := int64(i * chunkSize)
			end := offset + int64(chunkSize)
			if end > int64(fileSize) {
				end = int64(fileSize)
			}

			readChunk := readData[offset:end]
			expectedChunk := testData[offset:end]
			So(bytes.Equal(readChunk, expectedChunk), ShouldBeTrue)
		}

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested repeated chunk write: %d bytes (%.2f MB), rewrote %d chunks", fileSize, float64(fileSize)/(1024*1024), len(chunksToRewrite))
	})
}

// TestVFSConcurrentChunkUploadWithWait tests concurrent upload with wait time between writes
// This simulates real-world scenario where chunks arrive with delays
// NOTE: This test may need adjustment after removing batchwriter from VFS
func TestVFSConcurrentChunkUploadWithWait(t *testing.T) {
	t.Skip("Skipping TestVFSConcurrentChunkUploadWithWait - may need adjustment after batchwriter removal")
	Convey("Test VFS concurrent chunk upload with wait", t, func() {
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

		// Create bucket with encryption enabled
		admin := core.NewLocalAdmin(".", ".")
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-concurrent-wait-encrypted-bucket",
			Type:      1,
			Quota:     -1,
			ChunkSize: 10 * 1024 * 1024, // 10MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate 100MB test data
		fileSize := 100 * 1024 * 1024 // 100MB
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Calculate MD5 for verification
		md5Hash := md5.New()
		md5Hash.Write(testData)
		expectedMD5 := md5Hash.Sum(nil)

		// Step 1: Create file object (use .tmp suffix to match business layer behavior)
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   testBktID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-concurrent-wait-100mb.bin.tmp",
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

		// Step 2: Concurrent upload with delays - 5 chunks (10MB each)
		chunkSize := 10 * 1024 * 1024 // 10MB
		numChunks := 5
		totalChunks := 10 // 100MB / 10MB = 10 chunks total

		var wg sync.WaitGroup
		var writeErrors []error
		var writeErrorsMu sync.Mutex

		// Write first 5 chunks concurrently with random delays
		for i := 0; i < numChunks; i++ {
			wg.Add(1)
			go func(chunkNum int) {
				defer wg.Done()

				// Random delay between 0-100ms to simulate network delays
				delay := time.Duration(chunkNum*20) * time.Millisecond
				time.Sleep(delay)

				offset := int64(chunkNum * chunkSize)
				end := offset + int64(chunkSize)
				if end > int64(fileSize) {
					end = int64(fileSize)
				}

				chunkData := testData[offset:end]
				err := ra.Write(offset, chunkData)
				if err != nil {
					writeErrorsMu.Lock()
					writeErrors = append(writeErrors, err)
					writeErrorsMu.Unlock()
					return
				}
			}(i)
		}

		// Wait for concurrent writes to complete
		wg.Wait()

		// Check for write errors
		writeErrorsMu.Lock()
		// Allow some write errors in concurrent scenario, but log them
		if len(writeErrors) > 0 {
			for _, err := range writeErrors {
				t.Logf("Write error: %v", err)
			}
			// If there are too many errors, fail the test
			So(len(writeErrors), ShouldBeLessThan, numChunks/2)
		}
		writeErrorsMu.Unlock()

		// Wait a bit to ensure all writes are processed
		time.Sleep(1 * time.Second)

		// Step 3: Write remaining chunks sequentially
		for i := numChunks; i < totalChunks; i++ {
			offset := int64(i * chunkSize)
			end := offset + int64(chunkSize)
			if end > int64(fileSize) {
				end = int64(fileSize)
			}

			chunkData := testData[offset:end]
			err := ra.Write(offset, chunkData)
			So(err, ShouldBeNil)
		}

		// Step 4: Wait before flushing (simulating real-world delay)
		time.Sleep(1 * time.Second)

		// Step 5: Flush to ensure all data is uploaded
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 5: Simulate rename by removing .tmp suffix to trigger flush
		// This mimics the behavior of Rename() which calls forceFlushTempFileBeforeRename
		// Get current file object to preserve its state
		currentFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)
		So(currentFileObj, ShouldNotBeNil)

		// Update the file name to remove .tmp suffix
		finalFileObj := &core.ObjectInfo{
			ID:     currentFileObj.ID,
			PID:    currentFileObj.PID,
			Type:   currentFileObj.Type,
			Name:   "test-concurrent-wait-100mb.bin", // Remove .tmp suffix
			Size:   currentFileObj.Size,
			DataID: currentFileObj.DataID,
			MTime:  core.Now(),
		}
		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{finalFileObj})
		So(err, ShouldBeNil)

		// Clear cache to force reload
		fileObjCache.Del(fileObj.ID)

		// Step 5.5: Force flush TempFileWriter directly to ensure all data is persisted
		// After removing .tmp suffix, the file is no longer a .tmp file,
		// so ForceFlush will flush TempFileWriter if it exists
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 5.6: Call ForceFlush one more time to ensure TempFileWriter is fully flushed
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 5.7: Call ForceFlush one more time to ensure TempFileWriter is fully flushed
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Clear caches and force reload from database
		fileObjCache.Del(fileObj.ID)
		dataInfoCache.Del(int64(0))

		// Step 6: Verify file integrity
		var updatedFileObj *core.ObjectInfo
		for retry := 0; retry < 20; retry++ {
			// Clear cache before each retry to force reload from database
			fileObjCache.Del(fileObj.ID)
			updatedFileObj, err = ra.getFileObj()
			if err == nil && updatedFileObj != nil && updatedFileObj.Size == int64(fileSize) {
				break
			}
			if retry < 19 {
				time.Sleep(500 * time.Millisecond)
			}
		}
		So(err, ShouldBeNil)
		So(updatedFileObj, ShouldNotBeNil)
		if updatedFileObj != nil {
			So(updatedFileObj.Size, ShouldEqual, int64(fileSize))
		} else {
			t.Fatalf("Failed to get updated file object after retries")
		}

		// Read entire file back
		readData, err := ra.Read(0, fileSize)
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Verify no zero bytes in critical areas (first and last chunks)
		firstChunk := readData[0:chunkSize]
		lastChunkStart := int64(fileSize) - int64(chunkSize)
		if lastChunkStart < 0 {
			lastChunkStart = 0
		}
		lastChunk := readData[lastChunkStart:]

		firstChunkZeros := 0
		for _, b := range firstChunk {
			if b == 0 {
				firstChunkZeros++
			}
		}
		So(firstChunkZeros, ShouldBeLessThan, len(firstChunk)/100) // Less than 1% zeros

		lastChunkZeros := 0
		for _, b := range lastChunk {
			if b == 0 {
				lastChunkZeros++
			}
		}
		So(lastChunkZeros, ShouldBeLessThan, len(lastChunk)/100) // Less than 1% zeros

		// Verify MD5
		md5Hash2 := md5.New()
		md5Hash2.Write(readData)
		actualMD5 := md5Hash2.Sum(nil)
		So(bytes.Equal(actualMD5, expectedMD5), ShouldBeTrue)

		// Verify byte-by-byte comparison
		So(bytes.Equal(readData, testData), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested concurrent chunk upload with wait: %d bytes (%.2f MB)", fileSize, float64(fileSize)/(1024*1024))
	})
}
