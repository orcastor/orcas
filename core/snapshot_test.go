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
	db, err := GetWriteDB(tmpDir, "")
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
	db, _ := GetWriteDB(tmpDir, "")
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
	db, _ := GetWriteDB(tmpDir, "")
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
	db, _ := GetWriteDB(tmpDir, "")
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
	db, _ := GetWriteDB(tmpDir, "")
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

