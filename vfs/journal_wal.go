package vfs

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
)

// JournalWALEntry Journal WAL 条目
type JournalWALEntry struct {
	Timestamp int64  // 时间戳
	Offset    int64  // 写入偏移
	Length    int64  // 数据长度
	Data      []byte // 数据内容
	Checksum  uint64 // 校验和
}

// JournalWALSnapshot Journal WAL 快照
type JournalWALSnapshot struct {
	SnapshotID  int64              `json:"snapshot_id"`  // 快照ID
	FileID      int64              `json:"file_id"`      // 文件ID
	BaseDataID  int64              `json:"base_data_id"` // 基础 DataID
	BaseSize    int64              `json:"base_size"`    // 基础文件大小
	VirtualSize int64              `json:"virtual_size"` // 虚拟大小（包含 Journal）
	IsSparse    bool               `json:"is_sparse"`    // 是否稀疏文件
	Timestamp   int64              `json:"timestamp"`    // 快照时间
	EntryCount  int                `json:"entry_count"`  // 条目数量
	Committed   bool               `json:"committed"`    // 是否已提交
	Entries     []*JournalWALEntry `json:"entries"`      // WAL 条目列表
}

// JournalWAL Journal WAL 管理器
type JournalWAL struct {
	fileID   int64
	basePath string
	walPath  string
	walFile  *os.File
	mu       sync.Mutex
	config   JournalWALConfig
	enabled  bool
}

// JournalWALConfig Journal WAL 配置
type JournalWALConfig struct {
	Enabled          bool          // 是否启用
	MaxWALSize       int64         // 最大 WAL 大小
	SnapshotInterval time.Duration // 快照间隔
	SyncMode         string        // 同步模式：FULL, NORMAL, NONE
}

// DefaultJournalWALConfig 返回默认配置
func DefaultJournalWALConfig() JournalWALConfig {
	return JournalWALConfig{
		Enabled:          true,
		MaxWALSize:       50 << 20, // 50MB
		SnapshotInterval: 1 * time.Minute,
		SyncMode:         "FULL",
	}
}

// NewJournalWAL 创建 Journal WAL
func NewJournalWAL(fileID int64, basePath string, config JournalWALConfig) (*JournalWAL, error) {
	if !config.Enabled {
		return &JournalWAL{
			fileID:  fileID,
			enabled: false,
		}, nil
	}

	journalDir := filepath.Join(basePath, "journals")
	if err := os.MkdirAll(journalDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create journal directory: %w", err)
	}

	walPath := filepath.Join(journalDir, fmt.Sprintf("%d.jwal", fileID))
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open journal WAL file: %w", err)
	}

	return &JournalWAL{
		fileID:   fileID,
		basePath: basePath,
		walPath:  walPath,
		walFile:  walFile,
		config:   config,
		enabled:  true,
	}, nil
}

// WriteEntry 写入 Journal WAL 条目
func (jw *JournalWAL) WriteEntry(offset, length int64, data []byte) error {
	if !jw.enabled {
		return nil
	}

	jw.mu.Lock()
	defer jw.mu.Unlock()

	entry := &JournalWALEntry{
		Timestamp: time.Now().UnixNano(),
		Offset:    offset,
		Length:    length,
		Data:      data,
		Checksum:  xxh3.Hash(data),
	}

	return jw.writeEntryToFile(entry)
}

// CreateSnapshot 创建快照
func (jw *JournalWAL) CreateSnapshot(journal *Journal) error {
	if !jw.enabled {
		return nil
	}

	jw.mu.Lock()
	defer jw.mu.Unlock()

	// NOTE: Do not acquire journal.entriesMu here - caller already holds it
	// to avoid deadlock when called from Journal.CreateJournalSnapshot

	snapshot := &JournalWALSnapshot{
		SnapshotID:  time.Now().UnixNano(),
		FileID:      journal.fileID,
		BaseDataID:  journal.dataID, // 使用 dataID 而不是 baseDataID
		BaseSize:    journal.baseSize,
		VirtualSize: journal.virtualSize,
		IsSparse:    journal.isSparse,
		Timestamp:   time.Now().UnixNano(),
		EntryCount:  len(journal.entries),
		Entries:     make([]*JournalWALEntry, 0, len(journal.entries)),
		Committed:   true,
	}

	// 复制 Journal entries
	for i := range journal.entries {
		entry := &journal.entries[i]
		snapshot.Entries = append(snapshot.Entries, &JournalWALEntry{
			Offset:   entry.Offset,
			Length:   entry.Length,
			Data:     entry.Data,
			Checksum: xxh3.Hash(entry.Data),
		})
	}

	// 写入快照文件
	snapPath := filepath.Join(filepath.Dir(jw.walPath), fmt.Sprintf("%d.jwal.snap", jw.fileID))
	if err := jw.writeSnapshotToFile(snapshot, snapPath); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	DebugLog("[Journal WAL] Created snapshot: fileID=%d, entries=%d, virtualSize=%d",
		jw.fileID, len(snapshot.Entries), snapshot.VirtualSize)

	return nil
}

// LoadSnapshot 加载快照
func (jw *JournalWAL) LoadSnapshot() (*JournalWALSnapshot, error) {
	if !jw.enabled {
		return nil, nil
	}

	snapPath := filepath.Join(filepath.Dir(jw.walPath), fmt.Sprintf("%d.jwal.snap", jw.fileID))

	// 检查快照文件是否存在
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		return nil, nil // 无快照
	}

	return jw.loadSnapshotFromFile(snapPath)
}

// Recover 从 WAL 恢复 Journal
func (jw *JournalWAL) Recover() (*Journal, error) {
	if !jw.enabled {
		return nil, nil
	}

	// 尝试加载快照
	snapshot, err := jw.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	if snapshot == nil {
		return nil, nil // 无快照，无需恢复
	}

	// 从快照重建 Journal
	journal := &Journal{
		fileID:      snapshot.FileID,
		dataID:      snapshot.BaseDataID, // 使用 dataID
		baseSize:    snapshot.BaseSize,
		virtualSize: snapshot.VirtualSize,
		isSparse:    snapshot.IsSparse,
		entries:     make([]JournalEntry, 0, len(snapshot.Entries)), // 不是指针切片
	}

	for _, walEntry := range snapshot.Entries {
		journal.entries = append(journal.entries, JournalEntry{ // 不使用指针
			Offset: walEntry.Offset,
			Length: walEntry.Length,
			Data:   walEntry.Data,
		})
	}

	DebugLog("[Journal WAL] Recovered journal: fileID=%d, entries=%d, virtualSize=%d",
		journal.fileID, len(journal.entries), journal.virtualSize)

	return journal, nil
}

// DeleteSnapshot 删除快照
func (jw *JournalWAL) DeleteSnapshot() error {
	if !jw.enabled {
		return nil
	}

	snapPath := filepath.Join(filepath.Dir(jw.walPath), fmt.Sprintf("%d.jwal.snap", jw.fileID))

	if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	DebugLog("[Journal WAL] Deleted snapshot: fileID=%d", jw.fileID)
	return nil
}

// writeEntryToFile 写入条目到文件
func (jw *JournalWAL) writeEntryToFile(entry *JournalWALEntry) error {
	// [Entry: 32 bytes header + data]
	//   Timestamp: 8 bytes
	//   Offset: 8 bytes
	//   Length: 8 bytes
	//   Checksum: 8 bytes
	//   Data: variable

	buf := make([]byte, 32)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(entry.Timestamp))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(entry.Offset))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(entry.Length))
	binary.LittleEndian.PutUint64(buf[24:32], entry.Checksum)

	if _, err := jw.walFile.Write(buf); err != nil {
		return err
	}

	if len(entry.Data) > 0 {
		if _, err := jw.walFile.Write(entry.Data); err != nil {
			return err
		}
	}

	if jw.config.SyncMode == "FULL" {
		return jw.walFile.Sync()
	}

	return nil
}

// writeSnapshotToFile 写入快照到文件
func (jw *JournalWAL) writeSnapshotToFile(snapshot *JournalWALSnapshot, path string) error {
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	// 原子写入：先写临时文件，再重命名
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return nil
}

// loadSnapshotFromFile 加载快照
func (jw *JournalWAL) loadSnapshotFromFile(path string) (*JournalWALSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var snapshot JournalWALSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}

	return &snapshot, nil
}

// ReadEntryFromWAL 从 WAL 读取单个条目（用于恢复）
func ReadEntryFromWAL(reader io.Reader) (*JournalWALEntry, error) {
	// Read entry header
	header := make([]byte, 32)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}

	entry := &JournalWALEntry{
		Timestamp: int64(binary.LittleEndian.Uint64(header[0:8])),
		Offset:    int64(binary.LittleEndian.Uint64(header[8:16])),
		Length:    int64(binary.LittleEndian.Uint64(header[16:24])),
		Checksum:  binary.LittleEndian.Uint64(header[24:32]),
	}

	// Read data
	if entry.Length > 0 {
		entry.Data = make([]byte, entry.Length)
		if _, err := io.ReadFull(reader, entry.Data); err != nil {
			return nil, err
		}

		// Verify checksum
		actualChecksum := xxh3.Hash(entry.Data)
		if actualChecksum != entry.Checksum {
			return nil, fmt.Errorf("checksum mismatch: expected %d, got %d", entry.Checksum, actualChecksum)
		}
	}

	return entry, nil
}

// Close 关闭 WAL
func (jw *JournalWAL) Close() error {
	if !jw.enabled || jw.walFile == nil {
		return nil
	}

	jw.mu.Lock()
	defer jw.mu.Unlock()

	return jw.walFile.Close()
}

// DeleteFiles 删除所有 WAL 相关文件（包括 .jwal 和 .jwal.snap）
// 应该在文件删除时调用，确保清理所有 WAL 文件
func (jw *JournalWAL) DeleteFiles() error {
	if !jw.enabled {
		return nil
	}

	jw.mu.Lock()
	defer jw.mu.Unlock()

	// Close file handle first if open
	if jw.walFile != nil {
		jw.walFile.Close()
		jw.walFile = nil
	}

	// Delete WAL file
	if jw.walPath != "" {
		if err := os.Remove(jw.walPath); err != nil && !os.IsNotExist(err) {
			DebugLog("[Journal WAL] WARNING: Failed to delete WAL file: fileID=%d, path=%s, error=%v", jw.fileID, jw.walPath, err)
			// Continue to delete snapshot even if WAL deletion fails
		} else {
			DebugLog("[Journal WAL] Deleted WAL file: fileID=%d, path=%s", jw.fileID, jw.walPath)
		}
	}

	// Delete snapshot file
	if jw.walPath != "" {
		snapPath := filepath.Join(filepath.Dir(jw.walPath), fmt.Sprintf("%d.jwal.snap", jw.fileID))
		if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
			DebugLog("[Journal WAL] WARNING: Failed to delete snapshot file: fileID=%d, path=%s, error=%v", jw.fileID, snapPath, err)
			return fmt.Errorf("failed to delete snapshot file: %w", err)
		} else {
			DebugLog("[Journal WAL] Deleted snapshot file: fileID=%d, path=%s", jw.fileID, snapPath)
		}
	}

	return nil
}

// GetWALSize 获取 WAL 文件大小
func (jw *JournalWAL) GetWALSize() (int64, error) {
	if !jw.enabled || jw.walFile == nil {
		return 0, nil
	}

	info, err := jw.walFile.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

// TruncateWAL 截断 WAL 文件（在成功创建快照后）
func (jw *JournalWAL) TruncateWAL() error {
	if !jw.enabled || jw.walFile == nil {
		return nil
	}

	jw.mu.Lock()
	defer jw.mu.Unlock()

	// 关闭当前文件
	jw.walFile.Close()

	// 删除旧 WAL 文件
	if err := os.Remove(jw.walPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove old WAL: %w", err)
	}

	// 创建新 WAL 文件
	walFile, err := os.OpenFile(jw.walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %w", err)
	}

	jw.walFile = walFile
	DebugLog("[Journal WAL] Truncated WAL: fileID=%d", jw.fileID)

	return nil
}
