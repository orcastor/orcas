package vfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orcastor/orcas/core"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTempWriteAreaOrphanedFilesCleanup(t *testing.T) {
	Convey("TempWriteArea orphaned files cleanup", t, func() {
		tmpDir := t.TempDir()

		fs := &OrcasFS{
			Config: core.Config{
				DataPath: tmpDir,
			},
		}

		config := DefaultTempWriteAreaConfig()
		config.TempDir = ".temp_write_orphan_test"
		config.RetentionPeriod = 100 * time.Millisecond // 很短的保留期用于测试
		config.CleanupInterval = 50 * time.Millisecond  // 很短的清理间隔

		twa, err := NewTempWriteArea(fs, config)
		So(err, ShouldBeNil)
		defer twa.Stop()

		Convey("test cleanup orphaned files on disk", func() {
			// 创建临时目录
			tempDir := filepath.Join(tmpDir, config.TempDir)
			err := os.MkdirAll(tempDir, 0755)
			So(err, ShouldBeNil)

			// 手动创建一些"孤儿"临时文件（模拟程序崩溃后遗留的文件）
			orphanFiles := []string{
				"12345_67890", // fileID=12345, dataID=67890
				"23456_78901",
				"34567_89012",
			}

			for _, fileName := range orphanFiles {
				filePath := filepath.Join(tempDir, fileName)
				err := os.WriteFile(filePath, []byte("orphaned data"), 0644)
				So(err, ShouldBeNil)

				// 修改文件的修改时间，使其看起来是旧文件
				oldTime := time.Now().Add(-200 * time.Millisecond)
				err = os.Chtimes(filePath, oldTime, oldTime)
				So(err, ShouldBeNil)
			}

			// 验证文件存在
			for _, fileName := range orphanFiles {
				filePath := filepath.Join(tempDir, fileName)
				_, err := os.Stat(filePath)
				So(err, ShouldBeNil)
			}

			// 等待清理循环执行
			time.Sleep(200 * time.Millisecond)

			// 验证孤儿文件被删除
			for _, fileName := range orphanFiles {
				filePath := filepath.Join(tempDir, fileName)
				_, err := os.Stat(filePath)
				So(os.IsNotExist(err), ShouldBeTrue)
			}
		})

		Convey("test active files are not cleaned up", func() {
			// 创建一个活跃的临时文件
			fileID := int64(99999)
			dataID := int64(88888)

			twf, err := twa.GetOrCreate(fileID, dataID, 0, false, false)
			So(err, ShouldBeNil)
			defer twf.Close()

			// 写入一些数据
			data := make([]byte, 1024)
			err = twf.Write(0, data)
			So(err, ShouldBeNil)

			tempPath := twf.tempPath

			// 验证文件存在
			_, err = os.Stat(tempPath)
			So(err, ShouldBeNil)

			// 等待一段时间但不超过保留期
			time.Sleep(50 * time.Millisecond)

			// 访问文件以更新 lastAccess
			err = twf.Write(100, data)
			So(err, ShouldBeNil)

			// 等待清理循环执行
			time.Sleep(200 * time.Millisecond)

			// 验证活跃文件没有被删除（因为 lastAccess 被更新了）
			_, err = os.Stat(tempPath)
			So(err, ShouldBeNil)
		})

		Convey("test mixed scenario - active and orphaned files", func() {
			// 创建临时目录
			tempDir := filepath.Join(tmpDir, config.TempDir)
			err := os.MkdirAll(tempDir, 0755)
			So(err, ShouldBeNil)

			// 创建一个活跃文件
			activeFileID := int64(11111)
			activeDataID := int64(22222)
			activeTwf, err := twa.GetOrCreate(activeFileID, activeDataID, 0, false, false)
			So(err, ShouldBeNil)
			defer activeTwf.Close()

			// 写入数据以确保文件被创建
			data := make([]byte, 1024)
			err = activeTwf.Write(0, data)
			So(err, ShouldBeNil)

			// 创建孤儿文件
			orphanFileName := "33333_44444"
			orphanFilePath := filepath.Join(tempDir, orphanFileName)
			err = os.WriteFile(orphanFilePath, []byte("orphaned"), 0644)
			So(err, ShouldBeNil)

			// 修改孤儿文件的修改时间
			oldTime := time.Now().Add(-200 * time.Millisecond)
			err = os.Chtimes(orphanFilePath, oldTime, oldTime)
			So(err, ShouldBeNil)

			// 验证两个文件都存在
			_, err = os.Stat(activeTwf.tempPath)
			So(err, ShouldBeNil)
			_, err = os.Stat(orphanFilePath)
			So(err, ShouldBeNil)

			// 等待清理循环执行前，先访问活跃文件
			time.Sleep(50 * time.Millisecond)
			err = activeTwf.Write(100, data)
			So(err, ShouldBeNil)

			// 等待清理循环执行
			time.Sleep(200 * time.Millisecond)

			// 验证活跃文件仍然存在
			_, err = os.Stat(activeTwf.tempPath)
			So(err, ShouldBeNil)

			// 验证孤儿文件被删除
			_, err = os.Stat(orphanFilePath)
			So(os.IsNotExist(err), ShouldBeTrue)
		})
	})
}

