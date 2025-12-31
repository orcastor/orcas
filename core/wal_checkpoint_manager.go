package core

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WALCheckpointManager 定期刷新 WAL 的管理器
type WALCheckpointManager struct {
	dataPath           string
	checkpointInterval time.Duration
	stopCh             chan struct{}
	wg                 sync.WaitGroup
	mu                 sync.RWMutex
	activeBuckets      map[int64]time.Time // bktID -> last checkpoint time
}

// WALCheckpointConfig WAL checkpoint 配置
type WALCheckpointConfig struct {
	CheckpointInterval time.Duration // checkpoint 间隔
}

// DefaultWALCheckpointConfig 返回默认配置
func DefaultWALCheckpointConfig() *WALCheckpointConfig {
	return &WALCheckpointConfig{
		CheckpointInterval: 60 * time.Second, // 每60秒刷新一次
	}
}

// NewWALCheckpointManager 创建 WAL checkpoint 管理器
func NewWALCheckpointManager(dataPath string, config *WALCheckpointConfig) *WALCheckpointManager {
	if config == nil {
		config = DefaultWALCheckpointConfig()
	}

	return &WALCheckpointManager{
		dataPath:           dataPath,
		checkpointInterval: config.CheckpointInterval,
		stopCh:             make(chan struct{}),
		activeBuckets:      make(map[int64]time.Time),
	}
}

// Start 启动管理器
func (wcm *WALCheckpointManager) Start() error {
	if wcm.checkpointInterval == 0 {
		log.Printf("[WAL Checkpoint Manager] Checkpoint disabled (interval=0)")
		return nil
	}

	wcm.wg.Add(1)
	go wcm.checkpointLoop()

	log.Printf("[WAL Checkpoint Manager] Started: interval=%v, dataPath=%s",
		wcm.checkpointInterval, wcm.dataPath)
	return nil
}

// Stop 停止管理器
func (wcm *WALCheckpointManager) Stop() {
	close(wcm.stopCh)
	wcm.wg.Wait()
	log.Printf("[WAL Checkpoint Manager] Stopped")
}

// RegisterBucket 注册一个活跃的桶（有写入操作）
func (wcm *WALCheckpointManager) RegisterBucket(bktID int64) {
	wcm.mu.Lock()
	defer wcm.mu.Unlock()
	wcm.activeBuckets[bktID] = time.Now()
}

// checkpointLoop 定期执行 checkpoint
func (wcm *WALCheckpointManager) checkpointLoop() {
	defer wcm.wg.Done()

	ticker := time.NewTicker(wcm.checkpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wcm.stopCh:
			return
		case <-ticker.C:
			wcm.runCheckpoint()
		}
	}
}

// runCheckpoint 执行一次 checkpoint
func (wcm *WALCheckpointManager) runCheckpoint() {
	// 获取活跃桶列表
	wcm.mu.RLock()
	buckets := make([]int64, 0, len(wcm.activeBuckets))
	for bktID := range wcm.activeBuckets {
		buckets = append(buckets, bktID)
	}
	wcm.mu.RUnlock()

	if len(buckets) == 0 {
		// 没有活跃桶，扫描 dataPath 下的所有桶
		buckets = wcm.scanBuckets()
	}

	// 对每个桶执行 checkpoint
	successCount := 0
	errorCount := 0

	for _, bktID := range buckets {
		if err := wcm.checkpointBucket(bktID); err != nil {
			log.Printf("[WAL Checkpoint Manager] ERROR: Failed to checkpoint bucket %d: %v", bktID, err)
			errorCount++
		} else {
			successCount++
		}
	}

	if successCount > 0 || errorCount > 0 {
		log.Printf("[WAL Checkpoint Manager] Checkpoint completed: success=%d, error=%d, total=%d",
			successCount, errorCount, len(buckets))
	}
}

// scanBuckets 扫描 dataPath 下的所有桶目录
func (wcm *WALCheckpointManager) scanBuckets() []int64 {
	buckets := make([]int64, 0)

	// 读取 dataPath 目录
	entries, err := os.ReadDir(wcm.dataPath)
	if err != nil {
		log.Printf("[WAL Checkpoint Manager] ERROR: Failed to read dataPath %s: %v", wcm.dataPath, err)
		return buckets
	}

	// 查找所有数字命名的目录（桶ID）
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		var bktID int64
		if _, err := fmt.Sscanf(entry.Name(), "%d", &bktID); err == nil {
			buckets = append(buckets, bktID)
		}
	}

	return buckets
}

// checkpointBucket 对指定桶执行 checkpoint
func (wcm *WALCheckpointManager) checkpointBucket(bktID int64) error {
	// 构造桶数据库路径
	bktDirPath := filepath.Join(wcm.dataPath, fmt.Sprint(bktID))
	dbPath := filepath.Join(bktDirPath, ".db")

	// 检查数据库文件是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		// 数据库不存在，跳过
		return nil
	}

	// 检查 WAL 文件是否存在
	walPath := dbPath + "-wal"
	walInfo, err := os.Stat(walPath)
	if os.IsNotExist(err) {
		// WAL 文件不存在，跳过
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to stat WAL file: %w", err)
	}

	// 如果 WAL 文件为空或很小，跳过
	if walInfo.Size() < 1024 {
		return nil
	}

	// 获取数据库连接
	db, err := GetWriteDB(bktDirPath, "")
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}
	// 注意：不要关闭连接，它来自连接池

	// 执行 WAL checkpoint (TRUNCATE 模式)
	// TRUNCATE 模式会将 WAL 内容写入主数据库文件，并截断 WAL 文件
	var busy, logPages, checkpointed int
	err = db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logPages, &checkpointed)
	if err != nil {
		return fmt.Errorf("failed to execute checkpoint: %w", err)
	}

	// 记录 checkpoint 结果
	if checkpointed > 0 {
		log.Printf("[WAL Checkpoint Manager] Checkpointed bucket %d: busy=%d, logPages=%d, checkpointed=%d, walSize=%d",
			bktID, busy, logPages, checkpointed, walInfo.Size())
	}

	return nil
}

// GetStats 获取统计信息
func (wcm *WALCheckpointManager) GetStats() map[string]interface{} {
	wcm.mu.RLock()
	defer wcm.mu.RUnlock()

	return map[string]interface{}{
		"dataPath":           wcm.dataPath,
		"checkpointInterval": wcm.checkpointInterval.String(),
		"activeBuckets":      len(wcm.activeBuckets),
	}
}
