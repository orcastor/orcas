package vfs

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	"github.com/zeebo/xxh3"

	. "github.com/smartystreets/goconvey/convey"
)

// TestTempWriteHashSimple 简单测试临时写入区的哈希值计算
func TestTempWriteHashSimple(t *testing.T) {
	Convey("Test TempWriteArea Hash Calculation", t, func() {
		// Setup test environment
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

		lh := core.NewLocalHandler(".", ".").(*core.LocalHandler)
		lh.SetAdapter(dma, dda)

		testCtx, userInfo, _, err := lh.Login(c, "orcas", "orcas")
		So(err, ShouldBeNil)

		bucket := &core.BucketInfo{
			ID:       testBktID,
			Name:     "test_bucket",
			Type:     1,
			Quota:    1000000000,
			Used:     0,
			RealUsed: 0,
		}
		So(dma.PutBkt(testCtx, []*core.BucketInfo{bucket}), ShouldBeNil)
		err = dma.PutACL(testCtx, testBktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		// Create VFS with temp write area enabled
		cfg := &core.Config{
			DataPath: ".",
			BasePath: ".",
		}
		fs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)
		defer fs.tempWriteArea.Stop()

		Convey("Test large file hash values", func() {
			// Create a file object
			fileID := core.NewID()
			fileObj := &core.ObjectInfo{
				ID:    fileID,
				PID:   0,
				Type:  core.OBJ_TYPE_FILE,
				Name:  "test_large.bin",
				Size:  0,
				MTime: core.Now(),
			}

			// Save file object
			_, err := dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Create RandomAccessor
			ra, err := NewRandomAccessor(fs, fileID)
			So(err, ShouldBeNil)
			So(ra, ShouldNotBeNil)

			// Prepare test data (6MB to trigger temp write area)
			testData := make([]byte, 6*1024*1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			// Calculate expected hash values
			expectedXXH3 := int64(xxh3.Hash(testData))
			sha256Hash := sha256.Sum256(testData)
			expectedSHA256_0 := int64(binary.BigEndian.Uint64(sha256Hash[0:8]))
			expectedSHA256_1 := int64(binary.BigEndian.Uint64(sha256Hash[8:16]))
			expectedSHA256_2 := int64(binary.BigEndian.Uint64(sha256Hash[16:24]))
			expectedSHA256_3 := int64(binary.BigEndian.Uint64(sha256Hash[24:32]))

			DebugLog("[Test] Expected hashes: XXH3=%d, SHA256_0=%d", expectedXXH3, expectedSHA256_0)

			// Write data
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Get updated file object
			updatedFileObj, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(updatedFileObj.Size, ShouldEqual, len(testData))
			So(updatedFileObj.DataID, ShouldBeGreaterThan, 0)

			DebugLog("[Test] File object after flush: fileID=%d, dataID=%d, size=%d",
				updatedFileObj.ID, updatedFileObj.DataID, updatedFileObj.Size)

			// Get DataInfo from database
			dataInfo, err := lh.GetDataInfo(testCtx, testBktID, updatedFileObj.DataID)
			So(err, ShouldBeNil)
			So(dataInfo, ShouldNotBeNil)

			DebugLog("[Test] DataInfo from DB: dataID=%d, size=%d, origSize=%d, kind=%d, xxh3=%d, sha256_0=%d",
				dataInfo.ID, dataInfo.Size, dataInfo.OrigSize, dataInfo.Kind, dataInfo.XXH3, dataInfo.SHA256_0)

			// Verify DataInfo values
			So(dataInfo.ID, ShouldEqual, updatedFileObj.DataID)
			So(dataInfo.Size, ShouldEqual, len(testData))
			So(dataInfo.OrigSize, ShouldEqual, len(testData))
			So(dataInfo.Kind, ShouldEqual, 0) // No compression/encryption

			// Verify hash values are not zero
			So(dataInfo.XXH3, ShouldNotEqual, 0)
			So(dataInfo.SHA256_0, ShouldNotEqual, 0)
			So(dataInfo.SHA256_1, ShouldNotEqual, 0)
			So(dataInfo.SHA256_2, ShouldNotEqual, 0)
			So(dataInfo.SHA256_3, ShouldNotEqual, 0)

			// Verify hash values match expected
			So(dataInfo.XXH3, ShouldEqual, expectedXXH3)
			So(dataInfo.SHA256_0, ShouldEqual, expectedSHA256_0)
			So(dataInfo.SHA256_1, ShouldEqual, expectedSHA256_1)
			So(dataInfo.SHA256_2, ShouldEqual, expectedSHA256_2)
			So(dataInfo.SHA256_3, ShouldEqual, expectedSHA256_3)

			DebugLog("[Test] ✅ Hash values verified successfully: fileID=%d, dataID=%d", fileID, updatedFileObj.DataID)

			// Close RandomAccessor
			ra.Close()
		})

		Convey("Test compressed file hash values", func() {
			// Enable compression
			fs.CmprWay = core.DATA_CMPR_SNAPPY

			// Create a file object
			fileID := core.NewID()
			fileObj := &core.ObjectInfo{
				ID:    fileID,
				PID:   0,
				Type:  core.OBJ_TYPE_FILE,
				Name:  "test_compressed.bin",
				Size:  0,
				MTime: core.Now(),
			}

			// Save file object
			_, err := dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// Create RandomAccessor
			ra, err := NewRandomAccessor(fs, fileID)
			So(err, ShouldBeNil)
			So(ra, ShouldNotBeNil)

			// Prepare test data (6MB with repeated pattern for better compression)
			testData := make([]byte, 6*1024*1024)
			pattern := []byte("Hello World! This is a test pattern for compression. ")
			for i := 0; i < len(testData); i += len(pattern) {
				copy(testData[i:], pattern)
			}

			// Calculate expected hash values (from original data)
			expectedXXH3 := int64(xxh3.Hash(testData))
			sha256Hash := sha256.Sum256(testData)
			expectedSHA256_0 := int64(binary.BigEndian.Uint64(sha256Hash[0:8]))

			DebugLog("[Test] Expected hashes (original data): XXH3=%d, SHA256_0=%d", expectedXXH3, expectedSHA256_0)

			// Write data
			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// Get updated file object
			updatedFileObj, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(updatedFileObj.Size, ShouldEqual, len(testData))
			So(updatedFileObj.DataID, ShouldBeGreaterThan, 0)

			// Get DataInfo from database
			dataInfo, err := lh.GetDataInfo(testCtx, testBktID, updatedFileObj.DataID)
			So(err, ShouldBeNil)
			So(dataInfo, ShouldNotBeNil)

			DebugLog("[Test] DataInfo from DB: dataID=%d, size=%d, origSize=%d, kind=%d, xxh3=%d, sha256_0=%d",
				dataInfo.ID, dataInfo.Size, dataInfo.OrigSize, dataInfo.Kind, dataInfo.XXH3, dataInfo.SHA256_0)

			// Verify DataInfo values
			So(dataInfo.ID, ShouldEqual, updatedFileObj.DataID)
			So(dataInfo.Size, ShouldBeLessThan, len(testData)) // Compressed size should be smaller
			So(dataInfo.OrigSize, ShouldEqual, len(testData))
			So(dataInfo.Kind&core.DATA_CMPR_MASK, ShouldEqual, core.DATA_CMPR_SNAPPY)

			// Verify hash values (should match original data, not compressed data)
			So(dataInfo.XXH3, ShouldEqual, expectedXXH3)
			So(dataInfo.SHA256_0, ShouldEqual, expectedSHA256_0)

			DebugLog("[Test] ✅ Hash values verified for compressed file: fileID=%d, dataID=%d, compression ratio: %.2f%%",
				fileID, updatedFileObj.DataID, float64(dataInfo.Size)*100/float64(len(testData)))

			// Close RandomAccessor
			ra.Close()
		})
	})
}

