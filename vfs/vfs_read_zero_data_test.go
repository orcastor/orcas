package vfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	// Disable batch write optimization for tests to ensure immediate flush after each write
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "false")
}

// TestVFSReadZeroData tests the issue where Read returns all zeros after upload
// This test reproduces the bug where uploaded data is read back as all zeros
func TestVFSReadZeroData(t *testing.T) {
	Convey("Test VFS Read returns zero data after upload", t, func() {
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
			Name:      "test-read-zero-data-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate test data with non-zero values
		fileSize := 1024 * 1024 // 1MB
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Verify test data is not all zeros
		allZeros := true
		for i := 0; i < len(testData); i++ {
			if testData[i] != 0 {
				allZeros = false
				break
			}
		}
		So(allZeros, ShouldBeFalse)

		// Step 1: Upload - Create file and write data
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-read-zero-data.bin",
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

		// Flush to ensure data is uploaded
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Get updated file object
		updatedFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)
		So(updatedFileObj.Size, ShouldEqual, int64(fileSize))
		So(updatedFileObj.DataID, ShouldNotEqual, 0)
		So(updatedFileObj.DataID, ShouldNotEqual, core.EmptyDataID)

		// Step 2: Download - Read data back and verify it's not all zeros
		readData, err := ra.Read(0, fileSize)
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Check if read data is all zeros (this is the bug)
		allZerosRead := true
		firstNonZeroIndex := -1
		for i := 0; i < len(readData); i++ {
			if readData[i] != 0 {
				allZerosRead = false
				firstNonZeroIndex = i
				break
			}
		}

		if allZerosRead {
			t.Errorf("BUG: Read returned all zeros! Expected non-zero data. FileID=%d, DataID=%d, Size=%d",
				fileObj.ID, updatedFileObj.DataID, updatedFileObj.Size)
		}

		So(allZerosRead, ShouldBeFalse)
		So(bytes.Equal(readData, testData), ShouldBeTrue)

		// Additional verification: Check first and last bytes
		So(readData[0], ShouldEqual, testData[0])
		So(readData[len(readData)-1], ShouldEqual, testData[len(testData)-1])

		// Step 3: Test reading in chunks to verify partial reads work correctly
		chunkSize := 64 * 1024 // 64KB chunks
		for offset := 0; offset < fileSize; offset += chunkSize {
			readSize := chunkSize
			if offset+readSize > fileSize {
				readSize = fileSize - offset
			}
			chunkData, err := ra.Read(int64(offset), readSize)
			So(err, ShouldBeNil)
			So(len(chunkData), ShouldEqual, readSize)

			// Verify chunk is not all zeros
			chunkAllZeros := true
			for i := 0; i < len(chunkData); i++ {
				if chunkData[i] != 0 {
					chunkAllZeros = false
					break
				}
			}
			So(chunkAllZeros, ShouldBeFalse)

			// Verify chunk matches original data
			expectedChunk := testData[offset : offset+readSize]
			So(bytes.Equal(chunkData, expectedChunk), ShouldBeTrue)
		}

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully verified Read does not return zeros: %d bytes, firstNonZeroIndex=%d",
			fileSize, firstNonZeroIndex)
	})
}

// TestVFSReadZeroDataAfterReopen tests reading data after closing and reopening RandomAccessor
// This tests if the issue occurs when reopening the file
func TestVFSReadZeroDataAfterReopen(t *testing.T) {
	Convey("Test VFS Read returns zero data after reopen", t, func() {
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
			Name:      "test-read-zero-data-reopen-bucket",
			UID:       userInfo.ID,
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem
		ofs := NewOrcasFS(handler, ctx, testBktID)

		// Generate test data
		fileSize := 512 * 1024 // 512KB
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Create file object
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-read-zero-data-reopen.bin",
			Size:  0,
			MTime: core.Now(),
		}

		_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
		So(err, ShouldBeNil)

		// Step 1: Upload with first RandomAccessor
		ra1, err := NewRandomAccessor(ofs, fileObj.ID)
		So(err, ShouldBeNil)
		ofs.registerRandomAccessor(fileObj.ID, ra1)

		err = ra1.Write(0, testData)
		So(err, ShouldBeNil)

		_, err = ra1.ForceFlush()
		So(err, ShouldBeNil)

		// Close first RandomAccessor
		ofs.unregisterRandomAccessor(fileObj.ID, ra1)
		err = ra1.Close()
		So(err, ShouldBeNil)

		// Step 2: Reopen with new RandomAccessor and read
		ra2, err := NewRandomAccessor(ofs, fileObj.ID)
		So(err, ShouldBeNil)
		ofs.registerRandomAccessor(fileObj.ID, ra2)

		readData, err := ra2.Read(0, fileSize)
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Verify data is not all zeros
		allZerosRead := true
		for i := 0; i < len(readData); i++ {
			if readData[i] != 0 {
				allZerosRead = false
				break
			}
		}
		So(allZerosRead, ShouldBeFalse)
		So(bytes.Equal(readData, testData), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra2)
		err = ra2.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully verified Read does not return zeros after reopen: %d bytes", fileSize)
	})
}

