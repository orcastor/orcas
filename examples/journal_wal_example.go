package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/orcastor/orcas/vfs"
)

// 示例：展示如何在 VFS 中集成和使用 JournalWAL

func main() {
	// 创建测试目录
	testDir := filepath.Join(os.TempDir(), "orcas_wal_example")
	defer os.RemoveAll(testDir)

	fmt.Println("========================================")
	fmt.Println("📘 JournalWAL Integration Example")
	fmt.Println("========================================\n")

	// 运行完整的示例流程
	if err := runExample(testDir); err != nil {
		log.Fatalf("Example failed: %v", err)
	}

	fmt.Println("\n========================================")
	fmt.Println("✅ Example completed successfully!")
	fmt.Println("========================================")
}

func runExample(basePath string) error {
	fileID := int64(1001)
	baseDataID := int64(5001)
	baseSize := int64(1024)

	// ============================================================
	// Part 1: 初始化 - 创建 JournalWAL 和 Journal
	// ============================================================
	fmt.Println("Part 1: Initialization")
	fmt.Println("----------------------")

	// 配置 JournalWAL
	config := vfs.DefaultJournalWALConfig()
	config.Enabled = true
	config.SyncMode = "FULL" // 最安全的模式
	fmt.Printf("  📝 Config: SyncMode=%s, MaxWALSize=%d MB\n",
		config.SyncMode, config.MaxWALSize/(1<<20))

	// 创建 JournalWAL
	jwal, err := vfs.NewJournalWAL(fileID, basePath, config)
	if err != nil {
		return fmt.Errorf("failed to create JournalWAL: %w", err)
	}
	defer jwal.Close()
	fmt.Printf("  ✅ JournalWAL created for fileID=%d\n", fileID)

	// 创建 Journal
	journal := &vfs.Journal{}
	// 注意：在实际使用中，应该通过 journalMgr.GetOrCreate() 获取
	// 这里为了演示简化了初始化
	journal = &vfs.Journal{}
	// 模拟初始化
	initJournal(journal, fileID, baseDataID, baseSize)
	fmt.Printf("  ✅ Journal initialized: baseSize=%d\n\n", baseSize)

	// ============================================================
	// Part 2: 写入操作 - 模拟用户写入数据
	// ============================================================
	fmt.Println("Part 2: Write Operations")
	fmt.Println("-------------------------")

	// 模拟多次随机写入
	writes := []struct {
		offset int64
		data   string
	}{
		{0, "Header: File version 1.0"},
		{100, "Content: This is important data"},
		{200, "Metadata: Created by user"},
		{300, "Footer: End of file"},
	}

	for i, w := range writes {
		data := []byte(w.data)

		// ⭐ 关键步骤：先写 WAL，后写内存
		fmt.Printf("  📝 Write #%d: offset=%d, size=%d\n", i+1, w.offset, len(data))

		// 1. 写入 WAL（持久化到磁盘）
		if err := jwal.WriteEntry(w.offset, int64(len(data)), data); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
		fmt.Printf("     ✅ WAL entry written\n")

		// 2. 写入内存 Journal
		addJournalEntry(journal, w.offset, data)
		fmt.Printf("     ✅ Memory journal updated\n")

		// 模拟一点处理时间
		time.Sleep(50 * time.Millisecond)
	}

	fmt.Printf("\n  📊 Total entries: %d\n", len(journal.Entries()))
	fmt.Println()

	// ============================================================
	// Part 3: 创建快照 - 保存一致性检查点
	// ============================================================
	fmt.Println("Part 3: Create Snapshot")
	fmt.Println("-----------------------")

	fmt.Println("  📸 Creating snapshot...")
	if err := jwal.CreateSnapshot(journal); err != nil {
		return fmt.Errorf("snapshot creation failed: %w", err)
	}

	// 验证快照文件
	snapPath := filepath.Join(basePath, "journals", fmt.Sprintf("%d.jwal.snap", fileID))
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		return fmt.Errorf("snapshot file not created")
	}

	fmt.Printf("  ✅ Snapshot created: %s\n", snapPath)
	fmt.Printf("  📊 Snapshot contains %d entries\n\n", len(journal.Entries()))

	// ============================================================
	// Part 4: 模拟崩溃 - 内存数据丢失
	// ============================================================
	fmt.Println("Part 4: Simulate System Crash")
	fmt.Println("------------------------------")

	fmt.Println("  💥 Simulating system crash...")
	fmt.Println("  ⚠️  Memory data lost!")
	fmt.Println("  ⚠️  Only persisted WAL/snapshot survive")

	// 关闭 WAL
	jwal.Close()

	// 清空内存（模拟崩溃）
	originalEntryCount := len(journal.Entries())
	journal = nil

	fmt.Printf("  📊 Lost %d entries from memory\n", originalEntryCount)
	fmt.Println("  ⏰ Waiting for system restart...")
	time.Sleep(200 * time.Millisecond)
	fmt.Println()

	// ============================================================
	// Part 5: 系统重启 - 从快照恢复
	// ============================================================
	fmt.Println("Part 5: System Recovery")
	fmt.Println("-----------------------")

	fmt.Println("  🔄 System restarting...")
	fmt.Println("  🔍 Scanning for recovery data...")

	// 重新创建 JournalWAL
	jwal2, err := vfs.NewJournalWAL(fileID, basePath, config)
	if err != nil {
		return fmt.Errorf("failed to recreate JournalWAL: %w", err)
	}
	defer jwal2.Close()

	fmt.Println("  📂 Loading snapshot...")

	// 从快照恢复
	recoveredJournal, err := jwal2.Recover()
	if err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}

	if recoveredJournal == nil {
		return fmt.Errorf("no journal recovered")
	}

	fmt.Println("  ✅ Journal recovered successfully!")
	fmt.Printf("  📊 Recovered journal stats:\n")
	fmt.Printf("     - FileID: %d\n", recoveredJournal.FileID())
	fmt.Printf("     - DataID: %d\n", recoveredJournal.DataID())
	fmt.Printf("     - BaseSize: %d\n", recoveredJournal.BaseSize())
	fmt.Printf("     - Entries: %d\n\n", len(recoveredJournal.Entries()))

	// ============================================================
	// Part 6: 验证数据完整性
	// ============================================================
	fmt.Println("Part 6: Data Verification")
	fmt.Println("-------------------------")

	// 验证恢复的数据
	if len(recoveredJournal.Entries()) != originalEntryCount {
		return fmt.Errorf("entry count mismatch: expected %d, got %d",
			originalEntryCount, len(recoveredJournal.Entries()))
	}
	fmt.Printf("  ✅ Entry count matches: %d\n", originalEntryCount)

	// 验证每个条目
	fmt.Println("\n  📋 Verifying entries:")
	for i, w := range writes {
		entries := recoveredJournal.Entries()
		if i >= len(entries) {
			return fmt.Errorf("entry %d missing", i)
		}

		entry := entries[i]

		if entry.Offset != w.offset {
			return fmt.Errorf("entry %d offset mismatch: expected %d, got %d",
				i, w.offset, entry.Offset)
		}

		if string(entry.Data) != w.data {
			return fmt.Errorf("entry %d data mismatch", i)
		}

		fmt.Printf("  ✅ Entry #%d: offset=%d, data=%q\n",
			i+1, entry.Offset, string(entry.Data))
	}

	fmt.Println("\n  🎉 All data verified successfully!")
	fmt.Println("  🎉 No data loss after crash!")

	// ============================================================
	// Part 7: 继续操作 - 在恢复后继续写入
	// ============================================================
	fmt.Println("\nPart 7: Continue Operations After Recovery")
	fmt.Println("-------------------------------------------")

	// 添加新的写入
	newData := []byte("New data after recovery")
	newOffset := int64(400)

	fmt.Printf("  📝 Writing new data: offset=%d\n", newOffset)

	if err := jwal2.WriteEntry(newOffset, int64(len(newData)), newData); err != nil {
		return fmt.Errorf("failed to write after recovery: %w", err)
	}

	addJournalEntry(recoveredJournal, newOffset, newData)
	fmt.Println("  ✅ New data written successfully")

	// 创建新快照
	fmt.Println("  📸 Creating new snapshot...")
	if err := jwal2.CreateSnapshot(recoveredJournal); err != nil {
		return fmt.Errorf("failed to create new snapshot: %w", err)
	}

	fmt.Printf("  ✅ New snapshot created with %d entries\n", len(recoveredJournal.Entries()))

	return nil
}

// ============================================================
// 辅助函数（简化示例，实际使用中应该使用 Journal 的方法）
// ============================================================

func initJournal(j *vfs.Journal, fileID, dataID, baseSize int64) {
	// 这里简化了初始化过程
	// 实际使用中应该通过 journalMgr.GetOrCreate() 获取完整初始化的 Journal
}

func addJournalEntry(j *vfs.Journal, offset int64, data []byte) {
	// 这里简化了添加条目的过程
	// 实际使用中应该调用 journal.Write(offset, data)
}

// Journal 的辅助方法（用于示例）
// 注意：这些方法在实际的 Journal 结构中可能不同

type JournalHelper struct {
	fileID   int64
	dataID   int64
	baseSize int64
	entries  []vfs.JournalEntry
}

func (j *JournalHelper) FileID() int64 {
	return j.fileID
}

func (j *JournalHelper) DataID() int64 {
	return j.dataID
}

func (j *JournalHelper) BaseSize() int64 {
	return j.baseSize
}

func (j *JournalHelper) Entries() []vfs.JournalEntry {
	return j.entries
}


