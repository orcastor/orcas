package vfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTempWriteArea(t *testing.T) {
	Convey("TempWriteArea basic operations", t, func() {
		// 创建临时目录
		tmpDir := t.TempDir()
		
		// 创建模拟的 OrcasFS
		fs := &OrcasFS{
			Config: core.Config{
				DataPath: tmpDir,
			},
		}
		
		// 创建临时写入区
		config := DefaultTempWriteAreaConfig()
		config.TempDir = ".temp_write_test"
		twa, err := NewTempWriteArea(fs, config)
		So(err, ShouldBeNil)
		So(twa, ShouldNotBeNil)
		defer twa.Stop()
		
		Convey("test create and write to temp file", func() {
			fileID := int64(12345)
			dataID := int64(67890)
			
			// 创建临时写入文件
			twf, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			So(twf, ShouldNotBeNil)
			So(twf.fileID, ShouldEqual, fileID)
			So(twf.dataID, ShouldEqual, dataID)
			
			// 写入数据
			data1 := make([]byte, 4096)
			for i := range data1 {
				data1[i] = byte(i % 256)
			}
			err = twf.Write(0, data1)
			So(err, ShouldBeNil)
			So(twf.size, ShouldEqual, 4096)
			So(len(twf.ranges), ShouldEqual, 1)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 4096)
			
			// 写入更多数据
			data2 := make([]byte, 4096)
			for i := range data2 {
				data2[i] = byte((i + 100) % 256)
			}
			err = twf.Write(4096, data2)
			So(err, ShouldBeNil)
			So(twf.size, ShouldEqual, 8192)
			So(len(twf.ranges), ShouldEqual, 1) // 应该合并成一个范围
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 8192)
			
			// 验证文件存在
			_, err = os.Stat(twf.tempPath)
			So(err, ShouldBeNil)
			
			// 关闭文件
			err = twf.Close()
			So(err, ShouldBeNil)
		})
		
		Convey("test write with gaps (sparse file)", func() {
			fileID := int64(11111)
			dataID := int64(22222)
			
			twf, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			
			// 写入第一块数据
			data1 := make([]byte, 1024)
			for i := range data1 {
				data1[i] = 0xAA
			}
			err = twf.Write(0, data1)
			So(err, ShouldBeNil)
			
			// 写入第二块数据（有间隙）
			data2 := make([]byte, 1024)
			for i := range data2 {
				data2[i] = 0xBB
			}
			err = twf.Write(10240, data2)
			So(err, ShouldBeNil)
			
			// 应该有两个不连续的范围
			So(len(twf.ranges), ShouldEqual, 2)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 1024)
			So(twf.ranges[1].Start, ShouldEqual, 10240)
			So(twf.ranges[1].End, ShouldEqual, 11264)
			
			// 文件大小应该是最大偏移
			So(twf.size, ShouldEqual, 11264)
			
			// 填充间隙
			data3 := make([]byte, 9216) // 1024 到 10240
			for i := range data3 {
				data3[i] = 0xCC
			}
			err = twf.Write(1024, data3)
			So(err, ShouldBeNil)
			
			// 现在应该合并成一个连续范围
			So(len(twf.ranges), ShouldEqual, 1)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 11264)
			
			twf.Close()
		})
		
		Convey("test overlapping writes", func() {
			fileID := int64(33333)
			dataID := int64(44444)
			
			twf, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			
			// 写入第一块
			data1 := make([]byte, 2048)
			for i := range data1 {
				data1[i] = 0x11
			}
			err = twf.Write(0, data1)
			So(err, ShouldBeNil)
			
			// 写入重叠的第二块
			data2 := make([]byte, 2048)
			for i := range data2 {
				data2[i] = 0x22
			}
			err = twf.Write(1024, data2)
			So(err, ShouldBeNil)
			
			// 应该合并成一个范围
			So(len(twf.ranges), ShouldEqual, 1)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 3072)
			So(twf.size, ShouldEqual, 3072)
			
			twf.Close()
		})
		
		Convey("test concurrent access", func() {
			fileID := int64(55555)
			dataID := int64(66666)
			
			twf, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			
			// 多个 goroutine 并发写入
			done := make(chan error, 10)
			for i := 0; i < 10; i++ {
				go func(offset int) {
					data := make([]byte, 1024)
					for j := range data {
						data[j] = byte(offset)
					}
					err := twf.Write(int64(offset*1024), data)
					done <- err
				}(i)
			}
			
			// 等待所有写入完成并检查错误
			for i := 0; i < 10; i++ {
				writeErr := <-done
				So(writeErr, ShouldBeNil)
			}
			
			// 验证文件大小
			So(twf.size, ShouldEqual, 10240)
			
			// 验证范围合并
			So(len(twf.ranges), ShouldEqual, 1)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 10240)
			
			twf.Close()
		})
		
		Convey("test get or create returns same instance", func() {
			fileID := int64(77777)
			dataID := int64(88888)
			
			// 第一次创建
			twf1, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			
			// 第二次获取应该返回同一个实例
			twf2, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			So(twf2, ShouldEqual, twf1) // 应该是同一个指针
			
			twf1.Close()
		})
		
		Convey("test temp file path", func() {
			fileID := int64(99999)
			dataID := int64(11111)
			
			twf, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			
			// 验证临时文件路径
			expectedPath := filepath.Join(tmpDir, config.TempDir, "99999_11111")
			So(twf.tempPath, ShouldEqual, expectedPath)
			
			// 验证文件确实创建了
			info, err := os.Stat(twf.tempPath)
			So(err, ShouldBeNil)
			So(info.IsDir(), ShouldBeFalse)
			
			twf.Close()
		})
	})
}

func TestTempWriteAreaCleanup(t *testing.T) {
	Convey("TempWriteArea cleanup", t, func() {
		tmpDir := t.TempDir()
		
		fs := &OrcasFS{
			Config: core.Config{
				DataPath: tmpDir,
			},
		}
		
		config := DefaultTempWriteAreaConfig()
		config.TempDir = ".temp_write_cleanup_test"
		config.RetentionPeriod = 100 * time.Millisecond // 很短的保留期用于测试
		config.CleanupInterval = 50 * time.Millisecond  // 很短的清理间隔
		
		twa, err := NewTempWriteArea(fs, config)
		So(err, ShouldBeNil)
		defer twa.Stop()
		
		Convey("test cleanup removes expired files", func() {
			fileID := int64(12345)
			dataID := int64(67890)
			
			// 创建临时文件
			twf, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			
			// 写入一些数据
			data := make([]byte, 1024)
			err = twf.Write(0, data)
			So(err, ShouldBeNil)
			
			tempPath := twf.tempPath
			
			// 验证文件存在
			_, err = os.Stat(tempPath)
			So(err, ShouldBeNil)
			
			// 等待超过保留期
			time.Sleep(200 * time.Millisecond)
			
			// 触发清理（等待清理循环执行）
			time.Sleep(100 * time.Millisecond)
			
			// 验证文件被删除
			_, err = os.Stat(tempPath)
			So(os.IsNotExist(err), ShouldBeTrue)
			
			// 验证从活跃列表中移除
			twa.mu.RLock()
			_, exists := twa.activeFiles[fileID]
			twa.mu.RUnlock()
			So(exists, ShouldBeFalse)
		})
	})
}

func TestWriteRangeMerging(t *testing.T) {
	Convey("WriteRange merging logic", t, func() {
		tmpDir := t.TempDir()
		
		fs := &OrcasFS{
			Config: core.Config{
				DataPath: tmpDir,
			},
		}
		
		config := DefaultTempWriteAreaConfig()
		twa, err := NewTempWriteArea(fs, config)
		So(err, ShouldBeNil)
		defer twa.Stop()
		
		Convey("test merge adjacent ranges", func() {
			twf, _ := twa.GetOrCreate(1, 1, 0, false, false)
			defer twf.Close()
			
			// 写入 [0, 100)
			twf.updateRanges(0, 100)
			So(len(twf.ranges), ShouldEqual, 1)
			
			// 写入 [100, 200) - 应该合并
			twf.updateRanges(100, 100)
			So(len(twf.ranges), ShouldEqual, 1)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 200)
		})
		
		Convey("test merge overlapping ranges", func() {
			twf, _ := twa.GetOrCreate(2, 2, 0, false, false)
			defer twf.Close()
			
			// 写入 [0, 100)
			twf.updateRanges(0, 100)
			
			// 写入 [50, 150) - 有重叠，应该合并
			twf.updateRanges(50, 100)
			So(len(twf.ranges), ShouldEqual, 1)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 150)
		})
		
		Convey("test non-overlapping ranges", func() {
			twf, _ := twa.GetOrCreate(3, 3, 0, false, false)
			defer twf.Close()
			
			// 写入 [0, 100)
			twf.updateRanges(0, 100)
			
			// 写入 [200, 300) - 不重叠
			twf.updateRanges(200, 100)
			So(len(twf.ranges), ShouldEqual, 2)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 100)
			So(twf.ranges[1].Start, ShouldEqual, 200)
			So(twf.ranges[1].End, ShouldEqual, 300)
		})
		
		Convey("test fill gap between ranges", func() {
			twf, _ := twa.GetOrCreate(4, 4, 0, false, false)
			defer twf.Close()
			
			// 写入 [0, 100)
			twf.updateRanges(0, 100)
			
			// 写入 [200, 300)
			twf.updateRanges(200, 100)
			So(len(twf.ranges), ShouldEqual, 2)
			
			// 填充间隙 [100, 200)
			twf.updateRanges(100, 100)
			So(len(twf.ranges), ShouldEqual, 1)
			So(twf.ranges[0].Start, ShouldEqual, 0)
			So(twf.ranges[0].End, ShouldEqual, 300)
		})
	})
}

