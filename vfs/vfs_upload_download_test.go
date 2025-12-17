package vfs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	// Disable batch write optimization for tests to ensure immediate flush after each write
	// This makes tests more predictable and easier to understand
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "false")
}

// TestVFSUploadDownload tests basic file upload and download through VFS
func TestVFSUploadDownload(t *testing.T) {
	Convey("Test VFS file upload and download", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-upload-download-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Test data: small file
		testData := []byte("Hello, VFS Upload and Download Test!")
		testDataSize := len(testData)

		// Step 1: Upload - Create file and write data
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-upload-download.txt",
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

		// Upload: Write data
		err = ra.Write(0, testData)
		So(err, ShouldBeNil)

		// Flush to ensure data is uploaded (with batch write disabled, this will flush immediately)
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Get updated file object from RandomAccessor (which has latest cache)
		updatedFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)

		// Verify size from RandomAccessor's cache (most up-to-date)
		// With batch write disabled, data should be immediately available after ForceFlush
		So(updatedFileObj.Size, ShouldEqual, int64(testDataSize))
		So(updatedFileObj.DataID, ShouldNotEqual, 0)
		So(updatedFileObj.DataID, ShouldNotEqual, core.EmptyDataID)

		// Step 2: Download - Read data back
		readData, err := ra.Read(0, testDataSize)
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, testDataSize)
		So(bytes.Equal(readData, testData), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested upload and download: %d bytes", testDataSize)
	})
}

// TestVFSLargeFileUploadDownload tests large file upload and download
func TestVFSLargeFileUploadDownload(t *testing.T) {
	Convey("Test VFS large file upload and download", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-large-file-upload-download-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 10 * 1024 * 1024, // 10MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate large test data: 15MB (will span multiple chunks)
		fileSize := 15 * 1024 * 1024 // 15MB
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Calculate MD5 for verification
		md5Hash := md5.New()
		md5Hash.Write(testData)
		expectedMD5 := md5Hash.Sum(nil)

		// Step 1: Upload - Create file and write data in chunks
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-large-file-upload-download.bin",
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

		// Upload: Write data in chunks (simulating real upload scenario)
		writeChunkSize := 1024 * 1024 // 1MB chunks
		totalWritten := 0
		for offset := 0; offset < len(testData); offset += writeChunkSize {
			end := offset + writeChunkSize
			if end > len(testData) {
				end = len(testData)
			}
			err = ra.Write(int64(offset), testData[offset:end])
			So(err, ShouldBeNil)
			totalWritten += (end - offset)
		}
		So(totalWritten, ShouldEqual, len(testData))

		// Flush to ensure all data is uploaded (with batch write disabled, this will flush immediately)
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Get updated file object from RandomAccessor
		updatedFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)
		So(updatedFileObj.Size, ShouldEqual, int64(fileSize))
		So(updatedFileObj.DataID, ShouldNotEqual, 0)
		So(updatedFileObj.DataID, ShouldNotEqual, core.EmptyDataID)

		// Step 2: Download - Read entire file
		readData, err := ra.Read(0, fileSize)
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Verify MD5
		md5Hash2 := md5.New()
		md5Hash2.Write(readData)
		actualMD5 := md5Hash2.Sum(nil)
		So(bytes.Equal(actualMD5, expectedMD5), ShouldBeTrue)

		// Verify byte-by-byte comparison
		So(bytes.Equal(readData, testData), ShouldBeTrue)

		// Step 3: Download - Read in chunks (simulating real download scenario)
		readChunkSize := 2 * 1024 * 1024 // 2MB chunks
		downloadedData := make([]byte, 0, fileSize)
		for offset := 0; offset < fileSize; offset += readChunkSize {
			readSize := readChunkSize
			if offset+readSize > fileSize {
				readSize = fileSize - offset
			}
			var chunkData []byte
			chunkData, err = ra.Read(int64(offset), readSize)
			So(err, ShouldBeNil)
			So(len(chunkData), ShouldEqual, readSize)
			downloadedData = append(downloadedData, chunkData...)
		}
		So(len(downloadedData), ShouldEqual, fileSize)
		So(bytes.Equal(downloadedData, testData), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested large file upload and download: %d bytes (%.2f MB)", fileSize, float64(fileSize)/(1024*1024))
	})
}

// TestVFSRandomAccessUploadDownload tests random access upload and download
func TestVFSRandomAccessUploadDownload(t *testing.T) {
	Convey("Test VFS random access upload and download", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-random-access-upload-download-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate test data: 8MB file
		fileSize := 8 * 1024 * 1024 // 8MB
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Step 1: Upload - Create file
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-random-access-upload-download.bin",
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

		// Upload: Write data at random offsets (not sequential)
		// Write at offset 0
		err = ra.Write(0, testData[0:2*1024*1024]) // First 2MB
		So(err, ShouldBeNil)

		// Write at offset 4MB (skip middle)
		err = ra.Write(4*1024*1024, testData[4*1024*1024:6*1024*1024]) // 4MB-6MB
		So(err, ShouldBeNil)

		// Write at offset 2MB (fill the gap)
		err = ra.Write(2*1024*1024, testData[2*1024*1024:4*1024*1024]) // 2MB-4MB
		So(err, ShouldBeNil)

		// Write at offset 6MB (end)
		err = ra.Write(6*1024*1024, testData[6*1024*1024:]) // 6MB-end
		So(err, ShouldBeNil)

		// Flush to ensure all data is uploaded (with batch write disabled, this will flush immediately)
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Get updated file object from RandomAccessor
		updatedFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)
		So(updatedFileObj.Size, ShouldEqual, int64(fileSize))
		So(updatedFileObj.DataID, ShouldNotEqual, 0)
		So(updatedFileObj.DataID, ShouldNotEqual, core.EmptyDataID)

		// Step 2: Download - Read at random offsets
		// Read from offset 0
		readData0, err := ra.Read(0, 1024*1024) // First 1MB
		So(err, ShouldBeNil)
		So(len(readData0), ShouldEqual, 1024*1024)
		So(bytes.Equal(readData0, testData[0:1024*1024]), ShouldBeTrue)

		// Read from offset 3MB
		readData3, err := ra.Read(3*1024*1024, 1024*1024) // 3MB-4MB
		So(err, ShouldBeNil)
		So(len(readData3), ShouldEqual, 1024*1024)
		So(bytes.Equal(readData3, testData[3*1024*1024:4*1024*1024]), ShouldBeTrue)

		// Read from offset 5MB
		readData5, err := ra.Read(5*1024*1024, 1024*1024) // 5MB-6MB
		So(err, ShouldBeNil)
		So(len(readData5), ShouldEqual, 1024*1024)
		So(bytes.Equal(readData5, testData[5*1024*1024:6*1024*1024]), ShouldBeTrue)

		// Read from offset 7MB (near end)
		readData7, err := ra.Read(7*1024*1024, 1024*1024) // 7MB-8MB
		So(err, ShouldBeNil)
		So(len(readData7), ShouldEqual, 1024*1024)
		So(bytes.Equal(readData7, testData[7*1024*1024:]), ShouldBeTrue)

		// Read entire file to verify
		readDataFull, err := ra.Read(0, fileSize)
		So(err, ShouldBeNil)
		So(len(readDataFull), ShouldEqual, fileSize)
		So(bytes.Equal(readDataFull, testData), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested random access upload and download: %d bytes (%.2f MB)", fileSize, float64(fileSize)/(1024*1024))
	})
}

// TestVFSMultipleFileUploadDownload tests uploading and downloading multiple files
func TestVFSMultipleFileUploadDownload(t *testing.T) {
	Convey("Test VFS multiple file upload and download", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-multiple-file-upload-download-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Create multiple test files
		numFiles := 5
		fileSize := 1024 * 1024 // 1MB per file
		testFiles := make([]struct {
			fileObj  *core.ObjectInfo
			testData []byte
			ra       *RandomAccessor
		}, numFiles)

		// Step 1: Upload multiple files
		for i := 0; i < numFiles; i++ {
			// Generate test data
			testData := make([]byte, fileSize)
			_, err = rand.Read(testData)
			So(err, ShouldBeNil)

			// Create file object
			fileObj := &core.ObjectInfo{
				ID:    core.NewID(),
				PID:   core.ROOT_OID,
				Type:  core.OBJ_TYPE_FILE,
				Name:  filepath.Join("test", "file-"+string(rune(i+'0'))+".bin"),
				Size:  0,
				MTime: core.Now(),
			}

			_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Create RandomAccessor
			ra, err := NewRandomAccessor(ofs, fileObj.ID)
			So(err, ShouldBeNil)
			So(ra, ShouldNotBeNil)

			// Register RandomAccessor
			ofs.registerRandomAccessor(fileObj.ID, ra)

			// Upload: Write data
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush
			_, err = ra.ForceFlush()
			So(err, ShouldBeNil)

			testFiles[i] = struct {
				fileObj  *core.ObjectInfo
				testData []byte
				ra       *RandomAccessor
			}{
				fileObj:  fileObj,
				testData: testData,
				ra:       ra,
			}
		}

		// Step 2: Download and verify all files
		for i := 0; i < numFiles; i++ {
			// Download: Read data
			readData, err := testFiles[i].ra.Read(0, fileSize)
			So(err, ShouldBeNil)
			So(len(readData), ShouldEqual, fileSize)
			So(bytes.Equal(readData, testFiles[i].testData), ShouldBeTrue)

			// Cleanup
			ofs.unregisterRandomAccessor(testFiles[i].fileObj.ID, testFiles[i].ra)
			err = testFiles[i].ra.Close()
			So(err, ShouldBeNil)
		}

		t.Logf("Successfully tested multiple file upload and download: %d files, %d bytes each", numFiles, fileSize)
	})
}

// TestVFSAppendUploadDownload tests append upload and download
func TestVFSAppendUploadDownload(t *testing.T) {
	Convey("Test VFS append upload and download", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-append-upload-download-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Step 1: Initial upload
		initialData := []byte("Initial data. ")
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-append-upload-download.txt",
			Size:  0,
			MTime: core.Now(),
		}

		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
		So(err, ShouldBeNil)

		ra1, err := NewRandomAccessor(ofs, fileObj.ID)
		So(err, ShouldBeNil)
		So(ra1, ShouldNotBeNil)

		ofs.registerRandomAccessor(fileObj.ID, ra1)

		// Upload initial data
		err = ra1.Write(0, initialData)
		So(err, ShouldBeNil)

		// Flush to ensure initial data is uploaded (with batch write disabled, this will flush immediately)
		_, err = ra1.ForceFlush()
		So(err, ShouldBeNil)

		// Get current file size
		currentFileObj, err := ra1.getFileObj()
		So(err, ShouldBeNil)
		So(currentFileObj.Size, ShouldEqual, int64(len(initialData)))
		appendOffset := currentFileObj.Size

		// Step 2: Append upload
		appendData := []byte("Appended data. ")
		err = ra1.Write(appendOffset, appendData)
		So(err, ShouldBeNil)

		// Flush to ensure appended data is uploaded (with batch write disabled, this will flush immediately)
		_, err = ra1.ForceFlush()
		So(err, ShouldBeNil)

		// Get updated file object
		updatedFileObj, err := ra1.getFileObj()
		So(err, ShouldBeNil)
		So(updatedFileObj.Size, ShouldEqual, int64(len(initialData)+len(appendData)))
		So(updatedFileObj.DataID, ShouldNotEqual, 0)
		So(updatedFileObj.DataID, ShouldNotEqual, core.EmptyDataID)

		// Close and reopen to ensure data is persisted
		ofs.unregisterRandomAccessor(fileObj.ID, ra1)
		err = ra1.Close()
		So(err, ShouldBeNil)

		// Reopen for reading
		ra2, err := NewRandomAccessor(ofs, fileObj.ID)
		So(err, ShouldBeNil)
		So(ra2, ShouldNotBeNil)
		ofs.registerRandomAccessor(fileObj.ID, ra2)

		// Step 3: Download and verify
		expectedData := append(initialData, appendData...)
		readData, err := ra2.Read(0, len(expectedData))
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, len(expectedData))
		So(bytes.Equal(readData, expectedData), ShouldBeTrue)

		// Verify initial data
		readInitial, err := ra2.Read(0, len(initialData))
		So(err, ShouldBeNil)
		So(bytes.Equal(readInitial, initialData), ShouldBeTrue)

		// Verify appended data
		readAppended, err := ra2.Read(appendOffset, len(appendData))
		So(err, ShouldBeNil)
		So(bytes.Equal(readAppended, appendData), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra2)
		err = ra2.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested append upload and download: initial %d bytes + appended %d bytes = %d bytes total",
			len(initialData), len(appendData), len(expectedData))
	})
}

// TestVFSOverwriteUploadDownload tests overwrite upload and download
func TestVFSOverwriteUploadDownload(t *testing.T) {
	Convey("Test VFS overwrite upload and download", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-overwrite-upload-download-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Step 1: Initial upload
		initialData := make([]byte, 2*1024*1024) // 2MB
		_, err = rand.Read(initialData)
		So(err, ShouldBeNil)

		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-overwrite-upload-download.bin",
			Size:  0,
			MTime: core.Now(),
		}

		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
		So(err, ShouldBeNil)

		ra, err := NewRandomAccessor(ofs, fileObj.ID)
		So(err, ShouldBeNil)
		So(ra, ShouldNotBeNil)

		ofs.registerRandomAccessor(fileObj.ID, ra)

		// Upload initial data
		err = ra.Write(0, initialData)
		So(err, ShouldBeNil)

		// Flush to ensure initial data is uploaded (with batch write disabled, this will flush immediately)
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 2: Overwrite upload (write at offset 0 with different data)
		overwriteData := make([]byte, 1*1024*1024) // 1MB (smaller than initial)
		_, err = rand.Read(overwriteData)
		So(err, ShouldBeNil)

		err = ra.Write(0, overwriteData)
		So(err, ShouldBeNil)

		// Flush to ensure overwrite data is uploaded (with batch write disabled, this will flush immediately)
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Step 3: Download and verify
		// Get updated file object
		updatedFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)
		So(updatedFileObj.Size, ShouldEqual, int64(len(overwriteData)))
		So(updatedFileObj.DataID, ShouldNotEqual, 0)
		So(updatedFileObj.DataID, ShouldNotEqual, core.EmptyDataID)

		// Read entire file
		readData, err := ra.Read(0, len(overwriteData))
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, len(overwriteData))
		So(bytes.Equal(readData, overwriteData), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested overwrite upload and download: overwrote %d bytes with %d bytes",
			len(initialData), len(overwriteData))
	})
}

// TestVFSPartialDownload tests partial download (reading specific ranges)
func TestVFSPartialDownload(t *testing.T) {
	Convey("Test VFS partial download", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-partial-download-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate test data: 10MB
		fileSize := 10 * 1024 * 1024
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Upload file
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-partial-download.bin",
			Size:  0,
			MTime: core.Now(),
		}

		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
		So(err, ShouldBeNil)

		ra, err := NewRandomAccessor(ofs, fileObj.ID)
		So(err, ShouldBeNil)
		So(ra, ShouldNotBeNil)

		ofs.registerRandomAccessor(fileObj.ID, ra)

		err = ra.Write(0, testData)
		So(err, ShouldBeNil)

		// Flush to ensure data is uploaded (with batch write disabled, this will flush immediately)
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Test partial downloads at various offsets and sizes
		testCases := []struct {
			offset int64
			size   int
			name   string
		}{
			{0, 1024, "First 1KB"},
			{1024, 1024, "Second 1KB"},
			{1024 * 1024, 2 * 1024 * 1024, "2MB starting at 1MB"},
			{5 * 1024 * 1024, 1024 * 1024, "1MB starting at 5MB"},
			{9 * 1024 * 1024, 1024 * 1024, "Last 1MB"},
			{1024, 9*1024*1024 - 1024, "Almost entire file skipping first 1KB"},
		}

		for _, tc := range testCases {
			var readData []byte
			readData, err = ra.Read(tc.offset, tc.size)
			So(err, ShouldBeNil)

			expectedSize := tc.size
			if tc.offset+int64(tc.size) > int64(fileSize) {
				expectedSize = fileSize - int(tc.offset)
			}
			So(len(readData), ShouldEqual, expectedSize)
			expectedData := testData[tc.offset : tc.offset+int64(len(readData))]
			So(bytes.Equal(readData, expectedData), ShouldBeTrue)
		}

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully tested partial download: %d test cases", len(testCases))
	})
}
