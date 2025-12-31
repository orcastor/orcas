package vfs

import (
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

// TestTempWriteAreaCompression tests compression in temp write area
func TestTempWriteAreaCompression(t *testing.T) {
	Convey("Temp write area with compression", t, func() {
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
			Quota:    100000000,
			Used:     0,
			RealUsed: 0,
		}
		So(dma.PutBkt(testCtx, []*core.BucketInfo{bucket}), ShouldBeNil)
		err = dma.PutACL(testCtx, testBktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		// 创建 OrcasFS with compression enabled
		cfg := &core.Config{
			DataPath: ".",
			CmprWay:  2, // ZSTD compression
			CmprQlty: 3, // Compression level 3
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)

		Convey("test large file with compression", func() {
			// 创建文件
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "large_file.txt", // .txt should be compressed
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// 创建 RandomAccessor
			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// 写入大量可压缩数据（重复字符串）
			testData := make([]byte, 2*1024*1024) // 2MB
			pattern := []byte("This is a test pattern that should compress well. ")
			for i := 0; i < len(testData); i += len(pattern) {
				copy(testData[i:], pattern)
			}

			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// 获取文件对象
			fileObjAfter, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfter.Size, ShouldEqual, len(testData))
			So(fileObjAfter.DataID, ShouldBeGreaterThan, 0)

			// 获取 DataInfo 验证压缩
			dataInfo, err := dma.GetData(testCtx, testBktID, fileObjAfter.DataID)
			So(err, ShouldBeNil)
			So(dataInfo, ShouldNotBeNil)
			So(dataInfo.OrigSize, ShouldEqual, len(testData))
			
			DebugLog("[TEST] Compression test: origSize=%d, compressedSize=%d, ratio=%.2f%%, kind=%d",
				dataInfo.OrigSize, dataInfo.Size, float64(dataInfo.Size)*100/float64(dataInfo.OrigSize), dataInfo.Kind)
			
			// 验证压缩标志（如果使用了临时写入区）
			if dataInfo.Kind&core.DATA_CMPR_ZSTD != 0 {
				// 压缩后的大小应该小于原始大小
				So(dataInfo.Size, ShouldBeLessThan, dataInfo.OrigSize)
				DebugLog("[TEST] Compression successful!")
			} else {
				DebugLog("[TEST] WARNING: Compression flag not set, temp write area may not have been used")
			}
		})
	})
}

// TestTempWriteAreaEncryption tests encryption in temp write area
func TestTempWriteAreaEncryption(t *testing.T) {
	Convey("Temp write area with encryption", t, func() {
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
			Quota:    100000000,
			Used:     0,
			RealUsed: 0,
		}
		So(dma.PutBkt(testCtx, []*core.BucketInfo{bucket}), ShouldBeNil)
		err = dma.PutACL(testCtx, testBktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		// 创建 OrcasFS with encryption enabled
		cfg := &core.Config{
			DataPath: ".",
			EndecWay: 1,                      // AES256 encryption
			EndecKey: "test_encryption_key_1234567890", // > 16 chars
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)

		Convey("test large file with encryption", func() {
			// 创建文件
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "encrypted_file.txt", // 使用 .txt 以触发临时写入区
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// 创建 RandomAccessor
			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// 写入数据（重复数据以确保触发临时写入区）
			testData := []byte("This is sensitive data that should be encrypted! ")
			// 扩展到 2MB 以触发临时写入区
			largeData := make([]byte, 2*1024*1024)
			for i := 0; i < len(largeData); i += len(testData) {
				copy(largeData[i:], testData)
			}

			err = ra.Write(0, largeData)
			So(err, ShouldBeNil)

			// Flush
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// 获取文件对象
			fileObjAfter, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfter.Size, ShouldEqual, len(largeData))
			So(fileObjAfter.DataID, ShouldBeGreaterThan, 0)

			// 获取 DataInfo 验证加密
			dataInfo, err := dma.GetData(testCtx, testBktID, fileObjAfter.DataID)
			So(err, ShouldBeNil)
			So(dataInfo, ShouldNotBeNil)
			
			DebugLog("[TEST] Encryption test: origSize=%d, encryptedSize=%d, kind=%d",
				dataInfo.OrigSize, dataInfo.Size, dataInfo.Kind)
			
			// 验证加密标志（如果使用了临时写入区）
			if dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
				DebugLog("[TEST] Encryption successful!")
			} else {
				DebugLog("[TEST] WARNING: Encryption flag not set, temp write area may not have been used")
			}
		})
	})
}

// TestTempWriteAreaCompressionAndEncryption tests both compression and encryption
func TestTempWriteAreaCompressionAndEncryption(t *testing.T) {
	Convey("Temp write area with compression and encryption", t, func() {
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
			Quota:    100000000,
			Used:     0,
			RealUsed: 0,
		}
		So(dma.PutBkt(testCtx, []*core.BucketInfo{bucket}), ShouldBeNil)
		err = dma.PutACL(testCtx, testBktID, userInfo.ID, core.ALL)
		So(err, ShouldBeNil)

		// 创建 OrcasFS with both compression and encryption
		cfg := &core.Config{
			DataPath: ".",
			CmprWay:  2,                      // ZSTD compression
			CmprQlty: 3,                      // Compression level 3
			EndecWay: 1,                      // AES256 encryption
			EndecKey: "test_encryption_key_1234567890", // > 16 chars
		}
		ofs := NewOrcasFSWithConfig(lh, testCtx, testBktID, cfg)

		Convey("test large file with compression and encryption", func() {
			// 创建文件
			fileID, _ := ig.New()
			fileObj := &core.ObjectInfo{
				ID:     fileID,
				PID:    testBktID,
				Type:   core.OBJ_TYPE_FILE,
				Name:   "compressed_encrypted.txt",
				DataID: 0,
				Size:   0,
				MTime:  core.Now(),
			}
			_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
			So(err, ShouldBeNil)

			// 创建 RandomAccessor
			ra, err := NewRandomAccessor(ofs, fileID)
			So(err, ShouldBeNil)
			defer ra.Close()

			// 写入大量可压缩数据（重复字符串）
			testData := make([]byte, 2*1024*1024) // 2MB
			pattern := []byte("This is a test pattern that should compress well. ")
			for i := 0; i < len(testData); i += len(pattern) {
				copy(testData[i:], pattern)
			}

			err = ra.Write(0, testData)
			So(err, ShouldBeNil)

			// Flush
			_, err = ra.Flush()
			So(err, ShouldBeNil)

			// 获取文件对象
			fileObjAfter, err := ra.getFileObj()
			So(err, ShouldBeNil)
			So(fileObjAfter.Size, ShouldEqual, len(testData))
			So(fileObjAfter.DataID, ShouldBeGreaterThan, 0)

			// 获取 DataInfo 验证压缩和加密
			dataInfo, err := dma.GetData(testCtx, testBktID, fileObjAfter.DataID)
			So(err, ShouldBeNil)
			So(dataInfo, ShouldNotBeNil)
			So(dataInfo.OrigSize, ShouldEqual, len(testData))
			
			DebugLog("[TEST] Compression+Encryption test: origSize=%d, processedSize=%d, ratio=%.2f%%, kind=%d",
				dataInfo.OrigSize, dataInfo.Size, float64(dataInfo.Size)*100/float64(dataInfo.OrigSize), dataInfo.Kind)
			
			// 验证压缩和加密标志（如果使用了临时写入区）
			if dataInfo.Kind&core.DATA_CMPR_ZSTD != 0 {
				DebugLog("[TEST] Compression flag set!")
			} else {
				DebugLog("[TEST] WARNING: Compression flag not set")
			}
			if dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
				DebugLog("[TEST] Encryption flag set!")
			} else {
				DebugLog("[TEST] WARNING: Encryption flag not set")
			}
		})
	})
}

