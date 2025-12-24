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
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "false")
}

// TestVFSEncryptionKeyMismatch tests the issue where wrong encryption key causes read to return zeros
// This test reproduces the bug where uploaded encrypted data is read back as all zeros due to key mismatch
func TestVFSEncryptionKeyMismatch(t *testing.T) {
	Convey("Test VFS encryption key mismatch causes read zeros", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		encryptionKey := "test-encryption-key-12345678901234567890123456789012" // 32 bytes for AES256
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-encryption-key-bucket",
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem with encryption configuration (not from bucket config)
		cfg := &core.Config{
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: encryptionKey,
		}
		ofs := NewOrcasFSWithConfig(handler, ctx, testBktID, cfg)

		// Generate test data with non-zero values
		fileSize := 2 * 1024 * 1024 // 2MB
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

		// Step 1: Upload - Create file and write data with encryption
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-encryption-key.bin",
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

		// Flush to ensure data is uploaded and encrypted
		_, err = ra.ForceFlush()
		So(err, ShouldBeNil)

		// Get updated file object
		updatedFileObj, err := ra.getFileObj()
		So(err, ShouldBeNil)
		So(updatedFileObj.Size, ShouldEqual, int64(fileSize))
		So(updatedFileObj.DataID, ShouldNotEqual, 0)
		So(updatedFileObj.DataID, ShouldNotEqual, core.EmptyDataID)

		// Verify DataInfo has encryption flag
		dataInfo, err := handler.GetDataInfo(ctx, testBktID, updatedFileObj.DataID)
		So(err, ShouldBeNil)
		So(dataInfo, ShouldNotBeNil)
		So(dataInfo.Kind&core.DATA_ENDEC_MASK, ShouldNotEqual, 0)

		// Step 2: Download - Read data back and verify it's not all zeros
		readData, err := ra.Read(0, fileSize)
		So(err, ShouldBeNil)
		So(len(readData), ShouldEqual, fileSize)

		// Check if read data is all zeros (this is the bug if encryption key is wrong)
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
			t.Errorf("BUG: Read returned all zeros! This indicates encryption key mismatch. FileID=%d, DataID=%d, Size=%d",
				fileObj.ID, updatedFileObj.DataID, updatedFileObj.Size)
		}

		So(allZerosRead, ShouldBeFalse)
		So(bytes.Equal(readData, testData), ShouldBeTrue)

		// Cleanup
		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		err = ra.Close()
		So(err, ShouldBeNil)

		t.Logf("Successfully verified encryption/decryption: %d bytes, firstNonZeroIndex=%d",
			fileSize, firstNonZeroIndex)
	})
}

// TestVFSEncryptionKeyMismatchAfterReopen tests reading encrypted data after reopening
func TestVFSEncryptionKeyMismatchAfterReopen(t *testing.T) {
	Convey("Test VFS encryption key mismatch after reopen", t, func() {
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

		// Create bucket
		admin := core.NewLocalAdmin()
		encryptionKey := "test-encryption-key-12345678901234567890123456789012" // 32 bytes for AES256
		bkt := &core.BucketInfo{
			ID:        testBktID,
			Name:      "test-encryption-key-reopen-bucket",
			Type:      1,
			Quota:     -1,
			ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
		}
		err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Create filesystem with encryption configuration (not from bucket config)
		cfg := &core.Config{
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: encryptionKey,
		}
		ofs := NewOrcasFSWithConfig(handler, ctx, testBktID, cfg)

		// Generate test data
		fileSize := 1024 * 1024 // 1MB
		testData := make([]byte, fileSize)
		_, err = rand.Read(testData)
		So(err, ShouldBeNil)

		// Create file object
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   core.ROOT_OID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  "test-encryption-key-reopen.bin",
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

		// Verify data is not all zeros (key should match)
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

		t.Logf("Successfully verified encryption/decryption after reopen: %d bytes", fileSize)
	})
}
