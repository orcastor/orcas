package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestInitSnapshotTables tests snapshot table initialization
func TestInitSnapshotTables(t *testing.T) {
	// Create temporary directory for test database
	tmpDir, err := os.MkdirTemp("", "orcas_test_snapshot_init_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize main database
	dbPath := filepath.Join(tmpDir, ".db")
	db, err := GetWriteDB(tmpDir)
	if err != nil {
		t.Fatalf("Failed to get write DB: %v", err)
	}

	// Initialize snapshot tables
	if err := InitSnapshotTables(db); err != nil {
		t.Fatalf("Failed to initialize snapshot tables: %v", err)
	}

	// Verify bucket_snapshot table exists
	var tableName string
	err = db.QueryRow(`
		SELECT name FROM sqlite_master 
		WHERE type='table' AND name='bucket_snapshot'
	`).Scan(&tableName)
	if err != nil {
		t.Fatalf("bucket_snapshot table not found: %v", err)
	}
	if tableName != "bucket_snapshot" {
		t.Errorf("Expected table name 'bucket_snapshot', got '%s'", tableName)
	}

	// Verify snapshot_object table exists
	err = db.QueryRow(`
		SELECT name FROM sqlite_master 
		WHERE type='table' AND name='snapshot_object'
	`).Scan(&tableName)
	if err != nil {
		t.Fatalf("snapshot_object table not found: %v", err)
	}
	if tableName != "snapshot_object" {
		t.Errorf("Expected table name 'snapshot_object', got '%s'", tableName)
	}

	t.Logf("✅ Snapshot tables initialized successfully at %s", dbPath)
}

// TestSnapshotManager_CreateLazySnapshot tests lazy snapshot creation
func TestSnapshotManager_CreateLazySnapshot(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "orcas_test_snapshot_lazy_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize database
	if err := InitDB(tmpDir, ""); err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}

	// Initialize adapters
	dma := NewDefaultMetadataAdapter()
	dma.DefaultBaseMetadataAdapter.SetPath(tmpDir)
	dma.SetDataPath(tmpDir)

	dda := &DefaultDataAdapter{}
	dda.SetDataPath(tmpDir)

	// Initialize snapshot tables
	db, _ := GetWriteDB(tmpDir)
	if err := InitSnapshotTables(db); err != nil {
		t.Fatalf("Failed to init snapshot tables: %v", err)
	}

	// Create snapshot manager
	config := DefaultSnapshotConfig()
	config.LazyMode = true
	sm := NewSnapshotManager(dma, dda, config)

	// Create a test bucket
	bktID := int64(1000)
	ctx := context.Background()

	// Create lazy snapshot
	snapshot, err := sm.CreateSnapshot(ctx, bktID, "test_lazy_snapshot", "Test lazy snapshot", SnapshotTypeManual)
	if err != nil {
		t.Fatalf("Failed to create lazy snapshot: %v", err)
	}

	// Verify snapshot
	if snapshot.ID == 0 {
		t.Errorf("Snapshot ID should not be 0")
	}
	if snapshot.Name != "test_lazy_snapshot" {
		t.Errorf("Expected name 'test_lazy_snapshot', got '%s'", snapshot.Name)
	}
	if !snapshot.IsLazy {
		t.Errorf("Expected lazy snapshot, got IsLazy=false")
	}
	if snapshot.Status != SnapshotStatusComplete {
		t.Errorf("Expected status Complete, got %d", snapshot.Status)
	}

	// Query snapshot from database
	retrieved, err := sm.GetSnapshot(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}
	if retrieved.Name != snapshot.Name {
		t.Errorf("Expected name '%s', got '%s'", snapshot.Name, retrieved.Name)
	}

	t.Logf("✅ Lazy snapshot created successfully: id=%d, name=%s", snapshot.ID, snapshot.Name)
}

// TestSnapshotManager_ListSnapshots tests snapshot listing
func TestSnapshotManager_ListSnapshots(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "orcas_test_snapshot_list_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize database
	if err := InitDB(tmpDir, ""); err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}

	// Initialize adapters
	dma := NewDefaultMetadataAdapter()
	dma.DefaultBaseMetadataAdapter.SetPath(tmpDir)
	dma.SetDataPath(tmpDir)

	dda := &DefaultDataAdapter{}
	dda.SetDataPath(tmpDir)

	// Initialize snapshot tables
	db, _ := GetWriteDB(tmpDir)
	if err := InitSnapshotTables(db); err != nil {
		t.Fatalf("Failed to init snapshot tables: %v", err)
	}

	// Create snapshot manager
	config := DefaultSnapshotConfig()
	sm := NewSnapshotManager(dma, dda, config)

	bktID := int64(2000)
	ctx := context.Background()

	// Create multiple snapshots
	for i := 1; i <= 3; i++ {
		name := "snapshot_" + string(rune('0'+i))
		_, err := sm.CreateSnapshot(ctx, bktID, name, "Test snapshot "+string(rune('0'+i)), SnapshotTypeManual)
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", i, err)
		}
	}

	// List snapshots
	snapshots, err := sm.ListSnapshots(ctx, bktID, 0, 0)
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}

	if len(snapshots) != 3 {
		t.Errorf("Expected 3 snapshots, got %d", len(snapshots))
	}

	// Verify snapshots are sorted by created_at DESC
	for i := 0; i < len(snapshots)-1; i++ {
		if snapshots[i].CreatedAt < snapshots[i+1].CreatedAt {
			t.Errorf("Snapshots not sorted by created_at DESC")
		}
	}

	t.Logf("✅ Listed %d snapshots successfully", len(snapshots))
}

// TestSnapshotManager_DeleteSnapshot tests snapshot deletion
func TestSnapshotManager_DeleteSnapshot(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "orcas_test_snapshot_delete_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize database
	if err := InitDB(tmpDir, ""); err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}

	// Initialize adapters
	dma := NewDefaultMetadataAdapter()
	dma.DefaultBaseMetadataAdapter.SetPath(tmpDir)
	dma.SetDataPath(tmpDir)

	dda := &DefaultDataAdapter{}
	dda.SetDataPath(tmpDir)

	// Initialize snapshot tables
	db, _ := GetWriteDB(tmpDir)
	if err := InitSnapshotTables(db); err != nil {
		t.Fatalf("Failed to init snapshot tables: %v", err)
	}

	// Create snapshot manager
	config := DefaultSnapshotConfig()
	sm := NewSnapshotManager(dma, dda, config)

	bktID := int64(3000)
	ctx := context.Background()

	// Create snapshot
	snapshot, err := sm.CreateSnapshot(ctx, bktID, "test_delete", "Test delete", SnapshotTypeManual)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Delete snapshot
	if err := sm.DeleteSnapshot(ctx, snapshot.ID); err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}

	// Verify snapshot is marked as deleting or already deleted
	// Note: Async deletion may complete before we can check status
	retrieved, err := sm.GetSnapshot(ctx, snapshot.ID)
	if err == nil {
		// Still exists, should be marked as deleting
		if retrieved.Status != SnapshotStatusDeleting {
			t.Errorf("Expected status Deleting, got %d", retrieved.Status)
		}
		t.Logf("✅ Snapshot marked for deletion: id=%d", snapshot.ID)
	} else {
		// Already deleted by async process
		t.Logf("✅ Snapshot deleted successfully (async completed): id=%d", snapshot.ID)
	}
}

// TestSnapshotManager_CleanupExpiredSnapshots tests snapshot cleanup
func TestSnapshotManager_CleanupExpiredSnapshots(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "orcas_test_snapshot_cleanup_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize database
	if err := InitDB(tmpDir, ""); err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}

	// Initialize adapters
	dma := NewDefaultMetadataAdapter()
	dma.DefaultBaseMetadataAdapter.SetPath(tmpDir)
	dma.SetDataPath(tmpDir)

	dda := &DefaultDataAdapter{}
	dda.SetDataPath(tmpDir)

	// Initialize snapshot tables
	db, _ := GetWriteDB(tmpDir)
	if err := InitSnapshotTables(db); err != nil {
		t.Fatalf("Failed to init snapshot tables: %v", err)
	}

	// Create snapshot manager with max 2 snapshots
	config := DefaultSnapshotConfig()
	config.MaxSnapshots = 2
	sm := NewSnapshotManager(dma, dda, config)

	bktID := int64(4000)
	ctx := context.Background()

	// Create 5 snapshots
	for i := 1; i <= 5; i++ {
		name := "snapshot_" + string(rune('0'+i))
		_, err := sm.CreateSnapshot(ctx, bktID, name, "Test snapshot", SnapshotTypeManual)
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", i, err)
		}
	}

	// Cleanup expired snapshots
	deleted, err := sm.CleanupExpiredSnapshots(ctx, bktID)
	if err != nil {
		t.Fatalf("Failed to cleanup snapshots: %v", err)
	}

	if deleted != 3 {
		t.Errorf("Expected 3 deleted snapshots, got %d", deleted)
	}

	// Verify only 2 snapshots remain
	snapshots, err := sm.ListSnapshots(ctx, bktID, 0, 0)
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}

	// Note: Snapshots are marked for deletion asynchronously
	// So we check for deletion status rather than count
	deletingCount := 0
	for _, s := range snapshots {
		if s.Status == SnapshotStatusDeleting {
			deletingCount++
		}
	}

	if deletingCount < 3 {
		t.Errorf("Expected at least 3 snapshots marked for deletion, got %d", deletingCount)
	}

	t.Logf("✅ Cleanup completed: %d snapshots marked for deletion", deleted)
}

// TestSnapshotManager_RestoreSnapshot tests snapshot restore functionality
func TestSnapshotManager_RestoreSnapshot(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "orcas_test_snapshot_restore_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize database
	if err := InitDB(tmpDir, ""); err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}

	// Initialize adapters
	dma := NewDefaultMetadataAdapter()
	dma.DefaultBaseMetadataAdapter.SetPath(tmpDir)
	dma.SetDataPath(tmpDir)

	dda := &DefaultDataAdapter{}
	dda.SetDataPath(tmpDir)

	// Initialize snapshot tables
	db, _ := GetWriteDB(tmpDir)
	if err := InitSnapshotTables(db); err != nil {
		t.Fatalf("Failed to init snapshot tables: %v", err)
	}

	// Create bucket database
	bktID := int64(5000)
	bktPath := filepath.Join(tmpDir, "5000")
	if err := os.MkdirAll(bktPath, 0755); err != nil {
		t.Fatalf("Failed to create bucket dir: %v", err)
	}

	bktDB, err := GetWriteDB(bktPath)
	if err != nil {
		t.Fatalf("Failed to get bucket DB: %v", err)
	}

	// Create obj table
	_, err = bktDB.Exec(`
		CREATE TABLE IF NOT EXISTS obj (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT UNIQUE NOT NULL,
			data_id INTEGER,
			size INTEGER,
			mtime INTEGER,
			status INTEGER DEFAULT 0
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create obj table: %v", err)
	}

	// Create snapshot_object table in bucket DB
	_, err = bktDB.Exec(`
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
		t.Fatalf("Failed to create snapshot_object table: %v", err)
	}

	// Insert test objects
	testObjects := []struct {
		path   string
		dataID int64
		size   int64
		mtime  int64
	}{
		{"/file1.txt", 1001, 100, 1000000},
		{"/file2.txt", 1002, 200, 1000001},
		{"/file3.txt", 1003, 300, 1000002},
	}

	for _, obj := range testObjects {
		_, err = bktDB.Exec(`
			INSERT INTO obj (path, data_id, size, mtime, status)
			VALUES (?, ?, ?, ?, 0)
		`, obj.path, obj.dataID, obj.size, obj.mtime)
		if err != nil {
			t.Fatalf("Failed to insert test object: %v", err)
		}
	}

	// Create snapshot manager
	config := DefaultSnapshotConfig()
	config.LazyMode = false // Use immediate mode for testing
	config.COWEnabled = false
	sm := NewSnapshotManager(dma, dda, config)

	ctx := context.Background()

	// Create snapshot
	snapshot, err := sm.CreateSnapshot(ctx, bktID, "test_restore", "Test restore", SnapshotTypeManual)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	t.Logf("Created snapshot: id=%d, files=%d", snapshot.ID, snapshot.FileCount)

	// Modify objects in bucket (simulate changes)
	_, err = bktDB.Exec(`UPDATE obj SET size = 999, mtime = 2000000 WHERE path = ?`, "/file1.txt")
	if err != nil {
		t.Fatalf("Failed to update object: %v", err)
	}

	_, err = bktDB.Exec(`DELETE FROM obj WHERE path = ?`, "/file2.txt")
	if err != nil {
		t.Fatalf("Failed to delete object: %v", err)
	}

	// Verify changes
	var size, mtime int64
	err = bktDB.QueryRow(`SELECT size, mtime FROM obj WHERE path = ?`, "/file1.txt").Scan(&size, &mtime)
	if err != nil {
		t.Fatalf("Failed to query modified object: %v", err)
	}
	if size != 999 || mtime != 2000000 {
		t.Errorf("Object not modified correctly: size=%d, mtime=%d", size, mtime)
	}

	var count int
	err = bktDB.QueryRow(`SELECT COUNT(*) FROM obj WHERE path = ?`, "/file2.txt").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query deleted object: %v", err)
	}
	if count != 0 {
		t.Errorf("Object not deleted: count=%d", count)
	}

	// Restore snapshot
	if err := sm.RestoreSnapshot(ctx, snapshot.ID, bktID); err != nil {
		t.Fatalf("Failed to restore snapshot: %v", err)
	}

	t.Logf("Restored snapshot: id=%d", snapshot.ID)

	// Verify restoration - file1.txt should be restored to original values
	err = bktDB.QueryRow(`SELECT size, mtime FROM obj WHERE path = ?`, "/file1.txt").Scan(&size, &mtime)
	if err != nil {
		t.Fatalf("Failed to query restored object: %v", err)
	}
	if size != 100 || mtime != 1000000 {
		t.Errorf("Object not restored correctly: size=%d (expected 100), mtime=%d (expected 1000000)", size, mtime)
	}

	// Verify file2.txt is restored
	err = bktDB.QueryRow(`SELECT size, mtime FROM obj WHERE path = ?`, "/file2.txt").Scan(&size, &mtime)
	if err != nil {
		t.Fatalf("Failed to query restored object file2.txt: %v", err)
	}
	if size != 200 || mtime != 1000001 {
		t.Errorf("Object file2.txt not restored correctly: size=%d (expected 200), mtime=%d (expected 1000001)", size, mtime)
	}

	// Verify all 3 files exist
	err = bktDB.QueryRow(`SELECT COUNT(*) FROM obj WHERE status = 0`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count restored objects: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 restored objects, got %d", count)
	}

	t.Logf("✅ Snapshot restored successfully: all objects verified")
}

// TestRestoreObjectsFromSnapshot_ConflictHandling tests INSERT ON CONFLICT behavior
func TestRestoreObjectsFromSnapshot_ConflictHandling(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "orcas_test_restore_conflict_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize database
	if err := InitDB(tmpDir, ""); err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}

	// Initialize adapters
	dma := NewDefaultMetadataAdapter()
	dma.DefaultBaseMetadataAdapter.SetPath(tmpDir)
	dma.SetDataPath(tmpDir)

	// Initialize snapshot tables
	db, _ := GetWriteDB(tmpDir)
	if err := InitSnapshotTables(db); err != nil {
		t.Fatalf("Failed to init snapshot tables: %v", err)
	}

	// Create bucket database
	bktID := int64(6000)
	bktPath := filepath.Join(tmpDir, "6000")
	if err := os.MkdirAll(bktPath, 0755); err != nil {
		t.Fatalf("Failed to create bucket dir: %v", err)
	}

	bktDB, err := GetWriteDB(bktPath)
	if err != nil {
		t.Fatalf("Failed to get bucket DB: %v", err)
	}

	// Create obj table
	_, err = bktDB.Exec(`
		CREATE TABLE IF NOT EXISTS obj (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT UNIQUE NOT NULL,
			data_id INTEGER,
			size INTEGER,
			mtime INTEGER,
			status INTEGER DEFAULT 0
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create obj table: %v", err)
	}

	// Create snapshot_object table in bucket DB (not main DB)
	_, err = bktDB.Exec(`
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
		t.Fatalf("Failed to create snapshot_object table: %v", err)
	}

	// Insert existing object in bucket
	_, err = bktDB.Exec(`
		INSERT INTO obj (id, path, data_id, size, mtime, status)
		VALUES (1, '/conflict.txt', 2000, 500, 2000000, 0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert existing object: %v", err)
	}

	// Get the row ID before restore
	var originalID int64
	err = bktDB.QueryRow(`SELECT id FROM obj WHERE path = ?`, "/conflict.txt").Scan(&originalID)
	if err != nil {
		t.Fatalf("Failed to get original ID: %v", err)
	}

	// Insert snapshot object with same path but different data (in bucket DB)
	snapshotID := int64(1)
	_, err = bktDB.Exec(`
		INSERT INTO snapshot_object (snapshot_id, obj_id, path, data_id, size, mtime)
		VALUES (?, 1, '/conflict.txt', 1000, 100, 1000000)
	`, snapshotID)
	if err != nil {
		t.Fatalf("Failed to insert snapshot object: %v", err)
	}

	ctx := context.Background()

	// Restore objects from snapshot
	if err := dma.RestoreObjectsFromSnapshot(ctx, snapshotID, bktID); err != nil {
		t.Fatalf("Failed to restore objects: %v", err)
	}

	// Verify the object was updated (not deleted and re-inserted)
	var newID, dataID, size, mtime int64
	err = bktDB.QueryRow(`
		SELECT id, data_id, size, mtime FROM obj WHERE path = ?
	`, "/conflict.txt").Scan(&newID, &dataID, &size, &mtime)
	if err != nil {
		t.Fatalf("Failed to query restored object: %v", err)
	}

	// With INSERT ON CONFLICT DO UPDATE, the ID should remain the same
	if newID != originalID {
		t.Errorf("Row ID changed after restore: original=%d, new=%d (should be same with ON CONFLICT DO UPDATE)", originalID, newID)
	}

	// Verify data was updated
	if dataID != 1000 {
		t.Errorf("data_id not updated: got %d, expected 1000", dataID)
	}
	if size != 100 {
		t.Errorf("size not updated: got %d, expected 100", size)
	}
	if mtime != 1000000 {
		t.Errorf("mtime not updated: got %d, expected 1000000", mtime)
	}

	t.Logf("✅ Conflict handling verified: ID preserved (%d), data updated correctly", newID)
}

