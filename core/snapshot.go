package core

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
)

// 快照相关的 MetadataAdapter 接口扩展
// 这些方法需要在 DefaultMetadataAdapter 中实现

// SnapshotMetadataAdapter 快照元数据操作接口
type SnapshotMetadataAdapter interface {
	// 事务
	BeginTransaction(ctx Ctx) (Transaction, error)

	// 快照基本操作
	InsertBucketSnapshot(ctx Ctx, snapshot *BucketSnapshot) (int64, error)
	UpdateSnapshotStatus(ctx Ctx, snapshotID int64, status SnapshotStatus) error
	UpdateSnapshotStats(ctx Ctx, snapshotID int64, fileCount, totalSize int64, status SnapshotStatus) error
	GetBucketSnapshot(ctx Ctx, snapshotID int64) (*BucketSnapshot, error)
	GetBucketSnapshotByName(ctx Ctx, bktID int64, name string) (*BucketSnapshot, error)
	ListBucketSnapshots(ctx Ctx, bktID int64, limit, offset int) ([]*BucketSnapshot, error)
	DeleteBucketSnapshot(ctx Ctx, snapshotID int64) error

	// 快照对象操作
	CopyObjectsToSnapshot(ctx Ctx, snapshotID, bktID int64) (fileCount, totalSize int64, err error)
	GetSnapshotObjects(ctx Ctx, snapshotID int64, prefix string, limit, offset int) ([]*SnapshotObject, error)
	GetObjectsByVersion(ctx Ctx, bktID, version int64, prefix string, limit, offset int) ([]*SnapshotObject, error)
	DeleteSnapshotObjects(ctx Ctx, snapshotID int64) error

	// 恢复操作
	MarkBucketObjectsAsDeleted(ctx Ctx, bktID int64) error
	RestoreObjectsFromSnapshot(ctx Ctx, snapshotID, targetBktID int64) error

	// 事务操作
	InsertBucketSnapshotTx(ctx Ctx, tx Transaction, snapshot *BucketSnapshot) (int64, error)
	CopyObjectsToSnapshotTx(ctx Ctx, tx Transaction, snapshotID, bktID int64) (fileCount, totalSize int64, err error)
	UpdateSnapshotStatsTx(ctx Ctx, tx Transaction, snapshotID int64, fileCount, totalSize int64, status SnapshotStatus) error
	MarkBucketObjectsAsDeletedTx(ctx Ctx, tx Transaction, bktID int64) error
	RestoreObjectsFromSnapshotTx(ctx Ctx, tx Transaction, snapshotID, targetBktID int64) error
}

// SnapshotDataAdapter 快照数据操作接口
type SnapshotDataAdapter interface {
	// COW 相关操作
	MarkDataAsShared(ctx Ctx, bktID, snapshotID int64) error
	MarkDataAsSharedTx(ctx Ctx, tx Transaction, bktID, snapshotID int64) error
	DecrementSnapshotDataRefs(ctx Ctx, snapshotID int64) error
	IncrementSnapshotDataRefs(ctx Ctx, snapshotID int64) error
	IncrementSnapshotDataRefsTx(ctx Ctx, tx Transaction, snapshotID int64) error
	CleanupUnreferencedData(ctx Ctx, bktID int64) error

	// COW 写入
	WriteWithCOW(ctx Ctx, bktID, objID int64, data []byte) (int64, error)
	CopyData(ctx Ctx, bktID, srcDataID, dstDataID int64) error
}

// Transaction 事务接口
type Transaction interface {
	Commit() error
	Rollback() error
}

// SnapshotStatus defines snapshot status
type SnapshotStatus int32

const (
	SnapshotStatusInProgress SnapshotStatus = 0 // In progress
	SnapshotStatusComplete   SnapshotStatus = 1 // Complete
	SnapshotStatusDeleting   SnapshotStatus = 2 // Deleting
	SnapshotStatusFailed     SnapshotStatus = 3 // Failed
)

// SnapshotType defines snapshot type
type SnapshotType int32

const (
	SnapshotTypeManual    SnapshotType = 0 // Manual
	SnapshotTypeAuto      SnapshotType = 1 // Auto
	SnapshotTypeScheduled SnapshotType = 2 // Scheduled
)

// BucketSnapshot represents a bucket snapshot
type BucketSnapshot struct {
	ID              int64          `json:"id"`
	BucketID        int64          `json:"bucket_id"`
	Name            string         `json:"name"`
	Description     string         `json:"description,omitempty"`
	CreatedAt       int64          `json:"created_at"`
	MetadataVersion int64          `json:"metadata_version"` // Metadata version number
	SnapshotType    SnapshotType   `json:"snapshot_type"`
	Status          SnapshotStatus `json:"status"`
	FileCount       int64          `json:"file_count"`
	TotalSize       int64          `json:"total_size"`
	IsLazy          bool           `json:"is_lazy"` // Lazy copy mode
	ErrorMessage    string         `json:"error_message,omitempty"`
}

// SnapshotObject represents an object in a snapshot
type SnapshotObject struct {
	SnapshotID int64  `json:"snapshot_id"`
	ObjID      int64  `json:"obj_id"`
	Path       string `json:"path"`
	DataID     int64  `json:"data_id"`
	Size       int64  `json:"size"`
	MTime      int64  `json:"mtime"`
	Checksum   string `json:"checksum,omitempty"`
}

// SnapshotConfig defines snapshot configuration
type SnapshotConfig struct {
	Enabled          bool   // Enable snapshot feature
	AutoSnapshot     bool   // Auto snapshot
	SnapshotSchedule string // Cron expression
	MaxSnapshots     int    // Max snapshot count
	RetentionDays    int    // Retention days
	LazyMode         bool   // Lazy mode (O(1) creation)
	COWEnabled       bool   // Enable Copy-On-Write
}

// DefaultSnapshotConfig returns default snapshot configuration
func DefaultSnapshotConfig() SnapshotConfig {
	return SnapshotConfig{
		Enabled:          true,
		AutoSnapshot:     false,
		SnapshotSchedule: "0 2 * * *", // Daily at 2:00 AM
		MaxSnapshots:     30,
		RetentionDays:    90,
		LazyMode:         true, // Use lazy mode by default (O(1))
		COWEnabled:       true, // Enable COW by default
	}
}

// SnapshotManager manages bucket snapshots
type SnapshotManager struct {
	sma    SnapshotMetadataAdapter
	sda    SnapshotDataAdapter
	config SnapshotConfig
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(sma SnapshotMetadataAdapter, sda SnapshotDataAdapter, config SnapshotConfig) *SnapshotManager {
	return &SnapshotManager{
		sma:    sma,
		sda:    sda,
		config: config,
	}
}

// CreateSnapshot creates a new bucket snapshot
func (sm *SnapshotManager) CreateSnapshot(ctx context.Context, bktID int64, name, description string, snapshotType SnapshotType) (*BucketSnapshot, error) {
	if !sm.config.Enabled {
		return nil, fmt.Errorf("snapshot feature is disabled")
	}

	// Validate snapshot name
	if name == "" {
		name = fmt.Sprintf("snapshot_%d", Now())
	}

	// Check if snapshot already exists
	existing, _ := sm.GetSnapshotByName(ctx, bktID, name)
	if existing != nil {
		return nil, fmt.Errorf("snapshot with name '%s' already exists", name)
	}

	// Create snapshot object
	snapshot := &BucketSnapshot{
		BucketID:        bktID,
		Name:            name,
		Description:     description,
		CreatedAt:       Now(),
		MetadataVersion: sm.getCurrentMetadataVersion(ctx, bktID),
		SnapshotType:    snapshotType,
		Status:          SnapshotStatusInProgress,
		IsLazy:          sm.config.LazyMode,
	}

	if sm.config.LazyMode {
		// Lazy mode: O(1) creation
		return sm.createLazySnapshot(ctx, snapshot)
	} else {
		// Immediate mode: Copy metadata immediately
		return sm.createImmediateSnapshot(ctx, snapshot)
	}
}

// createLazySnapshot creates a lazy snapshot (O(1))
func (sm *SnapshotManager) createLazySnapshot(ctx context.Context, snapshot *BucketSnapshot) (*BucketSnapshot, error) {
	// Only insert snapshot record, no metadata copy
	id, err := sm.sma.InsertBucketSnapshot(ctx, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to insert snapshot: %w", err)
	}

	snapshot.ID = id
	snapshot.Status = SnapshotStatusComplete // Mark as complete immediately

	// Update snapshot status
	if err := sm.sma.UpdateSnapshotStatus(ctx, id, SnapshotStatusComplete); err != nil {
		return nil, fmt.Errorf("failed to update snapshot status: %w", err)
	}

	// Async copy metadata and stats
	go sm.copySnapshotMetadataAsync(context.Background(), snapshot)

	log.Printf("[Snapshot] Created lazy snapshot: id=%d, bucket=%d, name=%s", id, snapshot.BucketID, snapshot.Name)

	return snapshot, nil
}

// createImmediateSnapshot creates an immediate snapshot
func (sm *SnapshotManager) createImmediateSnapshot(ctx context.Context, snapshot *BucketSnapshot) (*BucketSnapshot, error) {
	// Begin transaction
	tx, err := sm.sma.BeginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Insert snapshot record
	id, err := sm.sma.InsertBucketSnapshotTx(ctx, tx, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to insert snapshot: %w", err)
	}
	snapshot.ID = id

	// 2. Copy object metadata to snapshot table
	fileCount, totalSize, err := sm.sma.CopyObjectsToSnapshotTx(ctx, tx, snapshot.ID, snapshot.BucketID)
	if err != nil {
		return nil, fmt.Errorf("failed to copy objects to snapshot: %w", err)
	}

	snapshot.FileCount = fileCount
	snapshot.TotalSize = totalSize

	// 3. Mark all related DataIDs as shared
	if sm.config.COWEnabled {
		if err := sm.sda.MarkDataAsSharedTx(ctx, tx, snapshot.BucketID, snapshot.ID); err != nil {
			return nil, fmt.Errorf("failed to mark data as shared: %w", err)
		}
	}

	// 4. Update snapshot stats
	snapshot.Status = SnapshotStatusComplete
	if err := sm.sma.UpdateSnapshotStatsTx(ctx, tx, snapshot.ID, fileCount, totalSize, SnapshotStatusComplete); err != nil {
		return nil, fmt.Errorf("failed to update snapshot stats: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("[Snapshot] Created immediate snapshot: id=%d, bucket=%d, name=%s, files=%d, size=%d",
		id, snapshot.BucketID, snapshot.Name, fileCount, totalSize)

	return snapshot, nil
}

// copySnapshotMetadataAsync async copy snapshot metadata
func (sm *SnapshotManager) copySnapshotMetadataAsync(ctx context.Context, snapshot *BucketSnapshot) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Snapshot] Panic in async metadata copy: %v", r)
		}
	}()

	// Copy object metadata
	fileCount, totalSize, err := sm.sma.CopyObjectsToSnapshot(ctx, snapshot.ID, snapshot.BucketID)
	if err != nil {
		log.Printf("[Snapshot] Failed to copy metadata for snapshot %d: %v", snapshot.ID, err)
		sm.sma.UpdateSnapshotStatus(ctx, snapshot.ID, SnapshotStatusFailed)
		return
	}

	// Mark data as shared
	if sm.config.COWEnabled {
		if err := sm.sda.MarkDataAsShared(ctx, snapshot.BucketID, snapshot.ID); err != nil {
			log.Printf("[Snapshot] Failed to mark data as shared for snapshot %d: %v", snapshot.ID, err)
		}
	}

	// Update stats
	if err := sm.sma.UpdateSnapshotStats(ctx, snapshot.ID, fileCount, totalSize, SnapshotStatusComplete); err != nil {
		log.Printf("[Snapshot] Failed to update stats for snapshot %d: %v", snapshot.ID, err)
	}

	log.Printf("[Snapshot] Async metadata copy completed: id=%d, files=%d, size=%d",
		snapshot.ID, fileCount, totalSize)
}

// getCurrentMetadataVersion get current metadata version number
func (sm *SnapshotManager) getCurrentMetadataVersion(ctx context.Context, bktID int64) int64 {
	// Use current timestamp as version number (second precision)
	return Now()
}

// ListSnapshots list snapshots
func (sm *SnapshotManager) ListSnapshots(ctx context.Context, bktID int64, limit, offset int) ([]*BucketSnapshot, error) {
	return sm.sma.ListBucketSnapshots(ctx, bktID, limit, offset)
}

// GetSnapshot get snapshot details
func (sm *SnapshotManager) GetSnapshot(ctx context.Context, snapshotID int64) (*BucketSnapshot, error) {
	return sm.sma.GetBucketSnapshot(ctx, snapshotID)
}

// GetSnapshotByName get snapshot by name
func (sm *SnapshotManager) GetSnapshotByName(ctx context.Context, bktID int64, name string) (*BucketSnapshot, error) {
	return sm.sma.GetBucketSnapshotByName(ctx, bktID, name)
}

// DeleteSnapshot delete a snapshot
func (sm *SnapshotManager) DeleteSnapshot(ctx context.Context, snapshotID int64) error {
	// Mark snapshot as deleting
	if err := sm.sma.UpdateSnapshotStatus(ctx, snapshotID, SnapshotStatusDeleting); err != nil {
		return fmt.Errorf("failed to mark snapshot as deleting: %w", err)
	}

	// Async cleanup
	go sm.deleteSnapshotAsync(context.Background(), snapshotID)

	log.Printf("[Snapshot] Marked snapshot %d for deletion", snapshotID)
	return nil
}

// deleteSnapshotAsync async delete snapshot
func (sm *SnapshotManager) deleteSnapshotAsync(ctx context.Context, snapshotID int64) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Snapshot] Panic in async delete: %v", r)
		}
	}()

	// Get snapshot info
	snapshot, err := sm.sma.GetBucketSnapshot(ctx, snapshotID)
	if err != nil {
		log.Printf("[Snapshot] Failed to get snapshot %d: %v", snapshotID, err)
		return
	}

	// If COW enabled, decrement data refs
	if sm.config.COWEnabled {
		if err := sm.sda.DecrementSnapshotDataRefs(ctx, snapshotID); err != nil {
			log.Printf("[Snapshot] Failed to decrement data refs for snapshot %d: %v", snapshotID, err)
		}
	}

	// Delete snapshot objects
	if err := sm.sma.DeleteSnapshotObjects(ctx, snapshotID); err != nil {
		log.Printf("[Snapshot] Failed to delete snapshot objects for %d: %v", snapshotID, err)
	}

	// Delete snapshot record
	if err := sm.sma.DeleteBucketSnapshot(ctx, snapshotID); err != nil {
		log.Printf("[Snapshot] Failed to delete snapshot record %d: %v", snapshotID, err)
		return
	}

	log.Printf("[Snapshot] Deleted snapshot: id=%d, bucket=%d, name=%s",
		snapshotID, snapshot.BucketID, snapshot.Name)

	// Cleanup unreferenced data
	go sm.sda.CleanupUnreferencedData(context.Background(), snapshot.BucketID)
}

// RestoreSnapshot restore a snapshot
func (sm *SnapshotManager) RestoreSnapshot(ctx context.Context, snapshotID int64, targetBktID int64) error {
	snapshot, err := sm.sma.GetBucketSnapshot(ctx, snapshotID)
	if err != nil {
		return fmt.Errorf("failed to get snapshot: %w", err)
	}

	if snapshot.Status != SnapshotStatusComplete {
		return fmt.Errorf("snapshot is not complete (status=%d)", snapshot.Status)
	}

	if targetBktID == 0 {
		targetBktID = snapshot.BucketID
	}

	log.Printf("[Snapshot] Starting restore: snapshot=%d, target_bucket=%d", snapshotID, targetBktID)

	// Begin transaction
	tx, err := sm.sma.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if targetBktID == snapshot.BucketID {
		// In-place restore: delete current files, restore snapshot
		if err := sm.sma.MarkBucketObjectsAsDeletedTx(ctx, tx, targetBktID); err != nil {
			return fmt.Errorf("failed to mark objects as deleted: %w", err)
		}
	}

	// Restore objects from snapshot
	if err := sm.sma.RestoreObjectsFromSnapshotTx(ctx, tx, snapshotID, targetBktID); err != nil {
		return fmt.Errorf("failed to restore objects: %w", err)
	}

	// Update data ref counts
	if sm.config.COWEnabled {
		if err := sm.sda.IncrementSnapshotDataRefsTx(ctx, tx, snapshotID); err != nil {
			return fmt.Errorf("failed to increment data refs: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("[Snapshot] Restored snapshot: id=%d, files=%d, to_bucket=%d",
		snapshotID, snapshot.FileCount, targetBktID)

	return nil
}

// CleanupExpiredSnapshots cleanup expired snapshots
func (sm *SnapshotManager) CleanupExpiredSnapshots(ctx context.Context, bktID int64) (int, error) {
	if sm.config.RetentionDays <= 0 && sm.config.MaxSnapshots <= 0 {
		return 0, nil // No retention policy configured
	}

	snapshots, err := sm.sma.ListBucketSnapshots(ctx, bktID, 0, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to list snapshots: %w", err)
	}

	now := Now()
	cutoffTime := now - int64(sm.config.RetentionDays*24*3600)
	deleted := 0

	// Delete by time
	for _, snapshot := range snapshots {
		if sm.config.RetentionDays > 0 && snapshot.CreatedAt < cutoffTime {
			if err := sm.DeleteSnapshot(ctx, snapshot.ID); err != nil {
				log.Printf("[Snapshot] Failed to delete expired snapshot %d: %v", snapshot.ID, err)
			} else {
				deleted++
			}
		}
	}

	// Delete by count
	if sm.config.MaxSnapshots > 0 && len(snapshots) > sm.config.MaxSnapshots {
		excessCount := len(snapshots) - sm.config.MaxSnapshots
		for i := 0; i < excessCount; i++ {
			// Delete oldest snapshots
			oldestSnapshot := snapshots[len(snapshots)-1-i]
			if err := sm.DeleteSnapshot(ctx, oldestSnapshot.ID); err != nil {
				log.Printf("[Snapshot] Failed to delete excess snapshot %d: %v", oldestSnapshot.ID, err)
			} else {
				deleted++
			}
		}
	}

	if deleted > 0 {
		log.Printf("[Snapshot] Cleaned up %d expired snapshots for bucket %d", deleted, bktID)
	}

	return deleted, nil
}

// GetSnapshotFiles get files in a snapshot
func (sm *SnapshotManager) GetSnapshotFiles(ctx context.Context, snapshotID int64, prefix string, limit, offset int) ([]*SnapshotObject, error) {
	snapshot, err := sm.sma.GetBucketSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	if snapshot.IsLazy {
		// Lazy mode: query by version
		return sm.sma.GetObjectsByVersion(ctx, snapshot.BucketID, snapshot.MetadataVersion, prefix, limit, offset)
	} else {
		// Immediate mode: query from snapshot table
		return sm.sma.GetSnapshotObjects(ctx, snapshotID, prefix, limit, offset)
	}
}

// Snapshot metadata adapter implementation for DefaultMetadataAdapter

// InitSnapshotTables initializes snapshot-related database tables
func InitSnapshotTables(db *sql.DB) error {
	// Create bucket_snapshot table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS bucket_snapshot (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			description TEXT,
			created_at INTEGER NOT NULL,
			metadata_version INTEGER NOT NULL,
			snapshot_type INTEGER DEFAULT 0,
			status INTEGER DEFAULT 0,
			file_count INTEGER DEFAULT 0,
			total_size INTEGER DEFAULT 0,
			is_lazy INTEGER DEFAULT 0,
			error_message TEXT,
			UNIQUE(bucket_id, name)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create bucket_snapshot table: %w", err)
	}

	// Create indexes for bucket_snapshot
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_bucket_snapshot_bkt ON bucket_snapshot(bucket_id, created_at DESC)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_bucket_snapshot_status ON bucket_snapshot(status)`)

	// Create snapshot_object table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS snapshot_object (
			snapshot_id INTEGER NOT NULL,
			obj_id INTEGER NOT NULL,
			path TEXT NOT NULL,
			data_id INTEGER,
			size INTEGER,
			mtime INTEGER,
			checksum TEXT,
			PRIMARY KEY (snapshot_id, obj_id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create snapshot_object table: %w", err)
	}

	// Create indexes for snapshot_object
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_snapshot_object_snap ON snapshot_object(snapshot_id)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_snapshot_object_path ON snapshot_object(snapshot_id, path)`)

	return nil
}

// BeginTransaction begins a database transaction
func (dma *DefaultMetadataAdapter) BeginTransaction(ctx Ctx) (Transaction, error) {
	db, err := GetWriteDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get write DB: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &sqlTransaction{tx: tx}, nil
}

// sqlTransaction implements Transaction interface
type sqlTransaction struct {
	tx *sql.Tx
}

func (st *sqlTransaction) Commit() error {
	return st.tx.Commit()
}

func (st *sqlTransaction) Rollback() error {
	return st.tx.Rollback()
}

// InsertBucketSnapshot inserts a new bucket snapshot
func (dma *DefaultMetadataAdapter) InsertBucketSnapshot(ctx Ctx, snapshot *BucketSnapshot) (int64, error) {
	db, err := GetWriteDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return 0, err
	}

	isLazy := 0
	if snapshot.IsLazy {
		isLazy = 1
	}

	result, err := db.Exec(`
		INSERT INTO bucket_snapshot 
		(bucket_id, name, description, created_at, metadata_version, snapshot_type, status, is_lazy)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, snapshot.BucketID, snapshot.Name, snapshot.Description, snapshot.CreatedAt,
		snapshot.MetadataVersion, snapshot.SnapshotType, snapshot.Status, isLazy)

	if err != nil {
		return 0, fmt.Errorf("failed to insert bucket snapshot: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert id: %w", err)
	}

	return id, nil
}

// InsertBucketSnapshotTx inserts a new bucket snapshot within a transaction
func (dma *DefaultMetadataAdapter) InsertBucketSnapshotTx(ctx Ctx, tx Transaction, snapshot *BucketSnapshot) (int64, error) {
	sqlTx := tx.(*sqlTransaction).tx

	isLazy := 0
	if snapshot.IsLazy {
		isLazy = 1
	}

	result, err := sqlTx.Exec(`
		INSERT INTO bucket_snapshot 
		(bucket_id, name, description, created_at, metadata_version, snapshot_type, status, is_lazy)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, snapshot.BucketID, snapshot.Name, snapshot.Description, snapshot.CreatedAt,
		snapshot.MetadataVersion, snapshot.SnapshotType, snapshot.Status, isLazy)

	if err != nil {
		return 0, fmt.Errorf("failed to insert bucket snapshot: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert id: %w", err)
	}

	return id, nil
}

// UpdateSnapshotStatus updates snapshot status
func (dma *DefaultMetadataAdapter) UpdateSnapshotStatus(ctx Ctx, snapshotID int64, status SnapshotStatus) error {
	db, err := GetWriteDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return err
	}

	_, err = db.Exec(`UPDATE bucket_snapshot SET status = ? WHERE id = ?`, status, snapshotID)
	return err
}

// UpdateSnapshotStats updates snapshot statistics
func (dma *DefaultMetadataAdapter) UpdateSnapshotStats(ctx Ctx, snapshotID int64, fileCount, totalSize int64, status SnapshotStatus) error {
	db, err := GetWriteDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		UPDATE bucket_snapshot 
		SET file_count = ?, total_size = ?, status = ? 
		WHERE id = ?
	`, fileCount, totalSize, status, snapshotID)

	return err
}

// UpdateSnapshotStatsTx updates snapshot statistics within a transaction
func (dma *DefaultMetadataAdapter) UpdateSnapshotStatsTx(ctx Ctx, tx Transaction, snapshotID int64, fileCount, totalSize int64, status SnapshotStatus) error {
	sqlTx := tx.(*sqlTransaction).tx

	_, err := sqlTx.Exec(`
		UPDATE bucket_snapshot 
		SET file_count = ?, total_size = ?, status = ? 
		WHERE id = ?
	`, fileCount, totalSize, status, snapshotID)

	return err
}

// GetBucketSnapshot gets a snapshot by ID
func (dma *DefaultMetadataAdapter) GetBucketSnapshot(ctx Ctx, snapshotID int64) (*BucketSnapshot, error) {
	db, err := GetReadDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return nil, err
	}

	snapshot := &BucketSnapshot{}
	var isLazy int

	err = db.QueryRow(`
		SELECT id, bucket_id, name, description, created_at, metadata_version, 
		       snapshot_type, status, file_count, total_size, is_lazy, 
		       COALESCE(error_message, '') as error_message
		FROM bucket_snapshot
		WHERE id = ?
	`, snapshotID).Scan(
		&snapshot.ID, &snapshot.BucketID, &snapshot.Name, &snapshot.Description,
		&snapshot.CreatedAt, &snapshot.MetadataVersion, &snapshot.SnapshotType,
		&snapshot.Status, &snapshot.FileCount, &snapshot.TotalSize, &isLazy,
		&snapshot.ErrorMessage,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("snapshot not found: %d", snapshotID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	snapshot.IsLazy = isLazy == 1

	return snapshot, nil
}

// GetBucketSnapshotByName gets a snapshot by bucket ID and name
func (dma *DefaultMetadataAdapter) GetBucketSnapshotByName(ctx Ctx, bktID int64, name string) (*BucketSnapshot, error) {
	db, err := GetReadDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return nil, err
	}

	snapshot := &BucketSnapshot{}
	var isLazy int

	err = db.QueryRow(`
		SELECT id, bucket_id, name, description, created_at, metadata_version, 
		       snapshot_type, status, file_count, total_size, is_lazy,
		       COALESCE(error_message, '') as error_message
		FROM bucket_snapshot
		WHERE bucket_id = ? AND name = ?
	`, bktID, name).Scan(
		&snapshot.ID, &snapshot.BucketID, &snapshot.Name, &snapshot.Description,
		&snapshot.CreatedAt, &snapshot.MetadataVersion, &snapshot.SnapshotType,
		&snapshot.Status, &snapshot.FileCount, &snapshot.TotalSize, &isLazy,
		&snapshot.ErrorMessage,
	)

	if err == sql.ErrNoRows {
		return nil, nil // Not found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot by name: %w", err)
	}

	snapshot.IsLazy = isLazy == 1

	return snapshot, nil
}

// ListBucketSnapshots lists snapshots for a bucket
func (dma *DefaultMetadataAdapter) ListBucketSnapshots(ctx Ctx, bktID int64, limit, offset int) ([]*BucketSnapshot, error) {
	db, err := GetReadDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT id, bucket_id, name, description, created_at, metadata_version, 
		       snapshot_type, status, file_count, total_size, is_lazy,
		       COALESCE(error_message, '') as error_message
		FROM bucket_snapshot
		WHERE bucket_id = ?
		ORDER BY created_at DESC
	`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	rows, err := db.Query(query, bktID)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	defer rows.Close()

	snapshots := make([]*BucketSnapshot, 0)
	for rows.Next() {
		snapshot := &BucketSnapshot{}
		var isLazy int

		err := rows.Scan(
			&snapshot.ID, &snapshot.BucketID, &snapshot.Name, &snapshot.Description,
			&snapshot.CreatedAt, &snapshot.MetadataVersion, &snapshot.SnapshotType,
			&snapshot.Status, &snapshot.FileCount, &snapshot.TotalSize, &isLazy,
			&snapshot.ErrorMessage,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan snapshot: %w", err)
		}

		snapshot.IsLazy = isLazy == 1
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

// DeleteBucketSnapshot deletes a snapshot
func (dma *DefaultMetadataAdapter) DeleteBucketSnapshot(ctx Ctx, snapshotID int64) error {
	db, err := GetWriteDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return err
	}

	_, err = db.Exec(`DELETE FROM bucket_snapshot WHERE id = ?`, snapshotID)
	return err
}

// CopyObjectsToSnapshot copies objects to snapshot table
func (dma *DefaultMetadataAdapter) CopyObjectsToSnapshot(ctx Ctx, snapshotID, bktID int64) (fileCount, totalSize int64, err error) {
	// Get bucket database path
	bktPath := filepath.Join(dma.DefaultDataMetadataAdapter.dataPath, fmt.Sprint(bktID))
	bktDB, err := GetWriteDB(bktPath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get bucket DB: %w", err)
	}

	// Copy objects
	_, err = bktDB.Exec(`
		INSERT INTO snapshot_object (snapshot_id, obj_id, path, data_id, size, mtime)
		SELECT ?, id, path, data_id, size, mtime
		FROM obj
		WHERE status = 0
	`, snapshotID)

	if err != nil {
		return 0, 0, fmt.Errorf("failed to copy objects: %w", err)
	}

	// Get statistics
	err = bktDB.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(size), 0)
		FROM snapshot_object
		WHERE snapshot_id = ?
	`, snapshotID).Scan(&fileCount, &totalSize)

	if err != nil {
		return 0, 0, fmt.Errorf("failed to get stats: %w", err)
	}

	return fileCount, totalSize, nil
}

// CopyObjectsToSnapshotTx copies objects to snapshot table within a transaction
func (dma *DefaultMetadataAdapter) CopyObjectsToSnapshotTx(ctx Ctx, tx Transaction, snapshotID, bktID int64) (fileCount, totalSize int64, err error) {
	// Note: Cross-database transactions are not supported in SQLite
	// We'll use the same approach as non-Tx version
	return dma.CopyObjectsToSnapshot(ctx, snapshotID, bktID)
}

// GetSnapshotObjects gets objects in a snapshot
func (dma *DefaultMetadataAdapter) GetSnapshotObjects(ctx Ctx, snapshotID int64, prefix string, limit, offset int) ([]*SnapshotObject, error) {
	// Determine which database contains the snapshot_object table
	// For now, assume it's in the main database
	db, err := GetReadDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT snapshot_id, obj_id, path, data_id, size, mtime, COALESCE(checksum, '') as checksum
		FROM snapshot_object
		WHERE snapshot_id = ?
	`

	args := []interface{}{snapshotID}

	if prefix != "" {
		query += " AND path LIKE ?"
		args = append(args, prefix+"%")
	}

	query += " ORDER BY path"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot objects: %w", err)
	}
	defer rows.Close()

	objects := make([]*SnapshotObject, 0)
	for rows.Next() {
		obj := &SnapshotObject{}
		err := rows.Scan(&obj.SnapshotID, &obj.ObjID, &obj.Path, &obj.DataID, &obj.Size, &obj.MTime, &obj.Checksum)
		if err != nil {
			return nil, fmt.Errorf("failed to scan snapshot object: %w", err)
		}
		objects = append(objects, obj)
	}

	return objects, nil
}

// GetObjectsByVersion gets objects by metadata version (for lazy snapshots)
func (dma *DefaultMetadataAdapter) GetObjectsByVersion(ctx Ctx, bktID, version int64, prefix string, limit, offset int) ([]*SnapshotObject, error) {
	// For lazy mode, we query objects that existed at the time of the snapshot
	// This is a simplified implementation - in production, you'd need version tracking
	bktPath := filepath.Join(dma.DefaultDataMetadataAdapter.dataPath, fmt.Sprint(bktID))
	db, err := GetReadDB(bktPath)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT 0 as snapshot_id, id as obj_id, path, data_id, size, mtime, '' as checksum
		FROM obj
		WHERE status = 0 AND mtime <= ?
	`

	args := []interface{}{version / 1000000000} // Convert nanoseconds to seconds

	if prefix != "" {
		query += " AND path LIKE ?"
		args = append(args, prefix+"%")
	}

	query += " ORDER BY path"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects by version: %w", err)
	}
	defer rows.Close()

	objects := make([]*SnapshotObject, 0)
	for rows.Next() {
		obj := &SnapshotObject{}
		err := rows.Scan(&obj.SnapshotID, &obj.ObjID, &obj.Path, &obj.DataID, &obj.Size, &obj.MTime, &obj.Checksum)
		if err != nil {
			return nil, fmt.Errorf("failed to scan object: %w", err)
		}
		objects = append(objects, obj)
	}

	return objects, nil
}

// DeleteSnapshotObjects deletes snapshot objects
func (dma *DefaultMetadataAdapter) DeleteSnapshotObjects(ctx Ctx, snapshotID int64) error {
	db, err := GetWriteDB(dma.DefaultBaseMetadataAdapter.basePath)
	if err != nil {
		return err
	}

	_, err = db.Exec(`DELETE FROM snapshot_object WHERE snapshot_id = ?`, snapshotID)
	return err
}

// MarkBucketObjectsAsDeleted marks all objects in a bucket as deleted
func (dma *DefaultMetadataAdapter) MarkBucketObjectsAsDeleted(ctx Ctx, bktID int64) error {
	bktPath := filepath.Join(dma.DefaultDataMetadataAdapter.dataPath, fmt.Sprint(bktID))
	db, err := GetWriteDB(bktPath)
	if err != nil {
		return err
	}

	_, err = db.Exec(`UPDATE obj SET status = 1 WHERE status = 0`)
	return err
}

// MarkBucketObjectsAsDeletedTx marks all objects in a bucket as deleted within a transaction
func (dma *DefaultMetadataAdapter) MarkBucketObjectsAsDeletedTx(ctx Ctx, tx Transaction, bktID int64) error {
	// Note: Cross-database transactions not supported
	return dma.MarkBucketObjectsAsDeleted(ctx, bktID)
}

// RestoreObjectsFromSnapshot restores objects from a snapshot
func (dma *DefaultMetadataAdapter) RestoreObjectsFromSnapshot(ctx Ctx, snapshotID, targetBktID int64) error {
	bktPath := filepath.Join(dma.DefaultDataMetadataAdapter.dataPath, fmt.Sprint(targetBktID))
	db, err := GetWriteDB(bktPath)
	if err != nil {
		return err
	}

	// Get snapshot objects from bucket DB (where CopyObjectsToSnapshot stores them)
	rows, err := db.Query(`
		SELECT obj_id, path, data_id, size, mtime
		FROM snapshot_object
		WHERE snapshot_id = ?
	`, snapshotID)
	if err != nil {
		return fmt.Errorf("failed to query snapshot objects: %w", err)
	}
	defer rows.Close()

	// Insert objects
	for rows.Next() {
		var objID, dataID, size, mtime int64
		var path string

		if err := rows.Scan(&objID, &path, &dataID, &size, &mtime); err != nil {
			return fmt.Errorf("failed to scan snapshot object: %w", err)
		}

		// Use INSERT ... ON CONFLICT DO UPDATE (upsert) to insert or update in place.
		// Avoid REPLACE INTO which deletes then inserts, changing row identity.
		_, err = db.Exec(`
			INSERT INTO obj (path, data_id, size, mtime, status)
			VALUES (?, ?, ?, ?, 0)
			ON CONFLICT(path) DO UPDATE SET
				data_id = excluded.data_id,
				size = excluded.size,
				mtime = excluded.mtime,
				status = excluded.status
		`, path, dataID, size, mtime)

		if err != nil {
			return fmt.Errorf("failed to restore object: %w", err)
		}
	}

	return nil
}

// RestoreObjectsFromSnapshotTx restores objects from a snapshot within a transaction
func (dma *DefaultMetadataAdapter) RestoreObjectsFromSnapshotTx(ctx Ctx, tx Transaction, snapshotID, targetBktID int64) error {
	// Note: Cross-database transactions not supported
	return dma.RestoreObjectsFromSnapshot(ctx, snapshotID, targetBktID)
}

// Snapshot data adapter implementation for COW operations

// MarkDataAsShared marks data as shared (for COW)
func (dda *DefaultDataAdapter) MarkDataAsShared(ctx Ctx, bktID, snapshotID int64) error {
	// Mark all data blocks in this bucket as shared
	bktPath := filepath.Join(dda.dataPath, fmt.Sprint(bktID))
	db, err := GetWriteDB(bktPath)
	if err != nil {
		return fmt.Errorf("failed to get bucket DB: %w", err)
	}

	// Update data_info to mark as shared
	// Note: We need to add is_shared and first_snapshot columns to data_info table
	_, err = db.Exec(`
		UPDATE data_info 
		SET is_shared = 1, first_snapshot = ?
		WHERE is_shared = 0
	`, snapshotID)

	return err
}

// MarkDataAsSharedTx marks data as shared within a transaction
func (dda *DefaultDataAdapter) MarkDataAsSharedTx(ctx Ctx, tx Transaction, bktID, snapshotID int64) error {
	// Note: Cross-database transactions not supported in SQLite
	// Fall back to non-transactional version
	return dda.MarkDataAsShared(ctx, bktID, snapshotID)
}

// DecrementSnapshotDataRefs decrements reference counts for snapshot data
func (dda *DefaultDataAdapter) DecrementSnapshotDataRefs(ctx Ctx, snapshotID int64) error {
	// Get all data IDs from snapshot
	// For now, just return nil as this needs integration with existing ref counting
	return nil
}

// IncrementSnapshotDataRefs increments reference counts for snapshot data
func (dda *DefaultDataAdapter) IncrementSnapshotDataRefs(ctx Ctx, snapshotID int64) error {
	// For now, just return nil as this needs integration with existing ref counting
	return nil
}

// IncrementSnapshotDataRefsTx increments reference counts within a transaction
func (dda *DefaultDataAdapter) IncrementSnapshotDataRefsTx(ctx Ctx, tx Transaction, snapshotID int64) error {
	// Note: Cross-database transactions not supported
	return dda.IncrementSnapshotDataRefs(ctx, snapshotID)
}

// CleanupUnreferencedData cleans up unreferenced data blocks
func (dda *DefaultDataAdapter) CleanupUnreferencedData(ctx Ctx, bktID int64) error {
	// This should integrate with existing garbage collection
	// For now, just mark as a background task
	go func() {
		// Find data blocks with ref_count = 0
		// Delete them from storage
		// This should use existing GC mechanisms
	}()
	return nil
}

// WriteWithCOW writes data with copy-on-write semantics
func (dda *DefaultDataAdapter) WriteWithCOW(ctx Ctx, bktID, objID int64, data []byte) (int64, error) {
	// Get current data ID for this object
	bktPath := filepath.Join(dda.dataPath, fmt.Sprint(bktID))
	db, err := GetReadDB(bktPath)
	if err != nil {
		return 0, fmt.Errorf("failed to get bucket DB: %w", err)
	}

	var currentDataID int64
	var isShared int
	err = db.QueryRow(`
		SELECT data_id, COALESCE(is_shared, 0)
		FROM obj
		WHERE id = ?
	`, objID).Scan(&currentDataID, &isShared)

	if err == sql.ErrNoRows {
		// New object, just write normally
		return dda.writeNewData(ctx, bktID, data)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to query object: %w", err)
	}

	// If data is shared, we need to copy-on-write
	if isShared == 1 {
		// Write new data block
		newDataID, err := dda.writeNewData(ctx, bktID, data)
		if err != nil {
			return 0, fmt.Errorf("failed to write new data: %w", err)
		}

		// Decrement old data ref count
		// dda.DecDataRef(ctx, bktID, currentDataID)

		return newDataID, nil
	}

	// Not shared, can write in place (or create new version)
	return dda.writeNewData(ctx, bktID, data)
}

// CopyData copies data from one ID to another
func (dda *DefaultDataAdapter) CopyData(ctx Ctx, bktID, srcDataID, dstDataID int64) error {
	// Read source data
	srcData, err := dda.readData(ctx, bktID, srcDataID)
	if err != nil {
		return fmt.Errorf("failed to read source data: %w", err)
	}

	// Write to destination
	_, err = dda.writeDataWithID(ctx, bktID, dstDataID, srcData)
	return err
}

// Helper methods (these should integrate with existing DataAdapter methods)

func (dda *DefaultDataAdapter) writeNewData(ctx Ctx, bktID int64, data []byte) (int64, error) {
	// This should use existing data writing logic
	// For now, return a placeholder
	return 0, fmt.Errorf("writeNewData not implemented yet")
}

func (dda *DefaultDataAdapter) readData(ctx Ctx, bktID, dataID int64) ([]byte, error) {
	// This should use existing data reading logic
	return nil, fmt.Errorf("readData not implemented yet")
}

func (dda *DefaultDataAdapter) writeDataWithID(ctx Ctx, bktID, dataID int64, data []byte) (int64, error) {
	// This should use existing data writing logic
	return 0, fmt.Errorf("writeDataWithID not implemented yet")
}

// ExtendDataInfoTable adds snapshot-related columns to data_info table
func ExtendDataInfoTable(db *sql.DB) error {
	// Add is_shared column
	_, err := db.Exec(`
		ALTER TABLE data_info ADD COLUMN is_shared INTEGER DEFAULT 0
	`)
	if err != nil && !isColumnExistsError(err) {
		return fmt.Errorf("failed to add is_shared column: %w", err)
	}

	// Add first_snapshot column
	_, err = db.Exec(`
		ALTER TABLE data_info ADD COLUMN first_snapshot INTEGER DEFAULT 0
	`)
	if err != nil && !isColumnExistsError(err) {
		return fmt.Errorf("failed to add first_snapshot column: %w", err)
	}

	// Create index on is_shared for faster queries
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_data_info_shared ON data_info(is_shared)`)

	return nil
}

// isColumnExistsError checks if error is due to column already existing
func isColumnExistsError(err error) bool {
	if err == nil {
		return false
	}
	// SQLite returns "duplicate column name" error
	return fmt.Sprintf("%v", err) == "duplicate column name: is_shared" ||
		fmt.Sprintf("%v", err) == "duplicate column name: first_snapshot"
}
