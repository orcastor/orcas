package vfs

import (
	"fmt"
	"sync"
	"time"

	"github.com/orcastor/orcas/core"
)

// PendingDeletion represents a delayed deletion task
type PendingDeletion struct {
	BktID     int64     // Bucket ID
	PID       int64     // Parent directory ID
	Name      string    // File name
	FileID    int64     // File object ID to be deleted
	CreatedAt time.Time // When the deletion was requested
	Deadline  time.Time // When to actually delete
	Versions  []int64   // Version IDs of the file
}

// PendingDeletionKey uniquely identifies a deletion task
type PendingDeletionKey struct {
	BktID int64
	PID   int64
	Name  string
}

func (k PendingDeletionKey) String() string {
	return fmt.Sprintf("%d:%d:%s", k.BktID, k.PID, k.Name)
}

// AtomicReplaceMgr manages pending deletions for atomic replace adaptation
type AtomicReplaceMgr struct {
	mu      sync.RWMutex
	pending map[string]*PendingDeletion // key: PendingDeletionKey.String()
	fs      *OrcasFS
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Configuration
	delayDuration time.Duration // Default: 5 seconds
	maxPending    int           // Max pending tasks
}

// NewAtomicReplaceMgr creates a new atomic replace manager
func NewAtomicReplaceMgr(fs *OrcasFS, delayDuration time.Duration) *AtomicReplaceMgr {
	if delayDuration == 0 {
		delayDuration = 5 * time.Second
	}

	dm := &AtomicReplaceMgr{
		pending:       make(map[string]*PendingDeletion),
		fs:            fs,
		stopCh:        make(chan struct{}),
		delayDuration: delayDuration,
		maxPending:    1000,
	}

	// Start background worker
	dm.wg.Add(1)
	go dm.worker()

	DebugLog("[AtomicReplaceMgr] Started with delay=%v", delayDuration)

	return dm
}

// ScheduleDeletion schedules a file for delayed deletion
func (dm *AtomicReplaceMgr) ScheduleDeletion(bktID, pid int64, name string, fileID int64) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if we're at max capacity
	if len(dm.pending) >= dm.maxPending {
		// Force process some deletions
		go dm.processPendingDeletions()
	}

	key := PendingDeletionKey{BktID: bktID, PID: pid, Name: name}.String()

	// Get all versions of the file
	versions, err := dm.getFileVersions(fileID)
	if err != nil {
		DebugLog("[AtomicReplaceMgr] WARNING: Failed to get versions for fileID=%d: %v", fileID, err)
		versions = []int64{} // Continue with empty versions
	}

	pd := &PendingDeletion{
		BktID:     bktID,
		PID:       pid,
		Name:      name,
		FileID:    fileID,
		CreatedAt: time.Now(),
		Deadline:  time.Now().Add(dm.delayDuration),
		Versions:  versions,
	}

	dm.pending[key] = pd

	DebugLog("[AtomicReplaceMgr] Scheduled deletion: key=%s, fileID=%d, versions=%d, deadline=%v",
		key, fileID, len(versions), pd.Deadline)

	return nil
}

// CheckAndCancelDeletion checks if there's a pending deletion and cancels it
// Returns the old file info if found and canceled
func (dm *AtomicReplaceMgr) CheckAndCancelDeletion(bktID, pid int64, name string) (*PendingDeletion, bool) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	key := PendingDeletionKey{BktID: bktID, PID: pid, Name: name}.String()

	if pd, exists := dm.pending[key]; exists {
		delete(dm.pending, key)

		DebugLog("[AtomicReplaceMgr] Canceled deletion (atomic replace detected): key=%s, fileID=%d, versions=%d",
			key, pd.FileID, len(pd.Versions))

		return pd, true
	}

	return nil, false
}

// getFileVersions gets all version IDs for a file
func (dm *AtomicReplaceMgr) getFileVersions(fileID int64) ([]int64, error) {
	// Get all children of the file (versions have PID=fileID)
	objs, _, _, err := dm.fs.h.List(dm.fs.c, dm.fs.bktID, fileID, core.ListOptions{
		Count: 0, // Get all
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}

	versionIDs := make([]int64, 0, len(objs))
	for _, obj := range objs {
		if obj.Type == core.OBJ_TYPE_VERSION || obj.Type == core.OBJ_TYPE_JOURNAL {
			versionIDs = append(versionIDs, obj.ID)
		}
	}

	return versionIDs, nil
}

// worker processes pending deletions in background
func (dm *AtomicReplaceMgr) worker() {
	defer dm.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dm.processPendingDeletions()
		case <-dm.stopCh:
			DebugLog("[AtomicReplaceMgr] Worker stopped")
			return
		}
	}
}

// processPendingDeletions executes deletions that have passed their deadline
func (dm *AtomicReplaceMgr) processPendingDeletions() {
	dm.mu.Lock()

	now := time.Now()
	toDelete := make([]*PendingDeletion, 0)

	for key, pd := range dm.pending {
		if now.After(pd.Deadline) {
			toDelete = append(toDelete, pd)
			delete(dm.pending, key)
		}
	}

	dm.mu.Unlock()

	// Execute deletions outside the lock
	if len(toDelete) > 0 {
		DebugLog("[AtomicReplaceMgr] Processing %d expired deletions", len(toDelete))

		for _, pd := range toDelete {
			if err := dm.executeDeletion(pd); err != nil {
				DebugLog("[AtomicReplaceMgr] Failed to execute deletion: fileID=%d, error=%v",
					pd.FileID, err)
			}
		}
	}
}

// executeDeletion performs the actual deletion
func (dm *AtomicReplaceMgr) executeDeletion(pd *PendingDeletion) error {
	DebugLog("[AtomicReplaceMgr] Executing deletion: fileID=%d, name=%s, versions=%d",
		pd.FileID, pd.Name, len(pd.Versions))

	// Delete all versions first
	if len(pd.Versions) > 0 {
		for _, versionID := range pd.Versions {
			if err := dm.fs.h.Delete(dm.fs.c, pd.BktID, versionID); err != nil {
				DebugLog("[AtomicReplaceMgr] WARNING: Failed to delete version %d: %v", versionID, err)
				// Continue to delete other versions
			}
		}
	}

	// Delete file object
	if err := dm.fs.h.Delete(dm.fs.c, pd.BktID, pd.FileID); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	DebugLog("[AtomicReplaceMgr] Deletion completed: fileID=%d", pd.FileID)

	return nil
}

// GetPendingCount returns the number of pending deletions
func (dm *AtomicReplaceMgr) GetPendingCount() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return len(dm.pending)
}

// Stop stops the atomic replace manager and waits for worker to finish
func (dm *AtomicReplaceMgr) Stop() {
	close(dm.stopCh)
	dm.wg.Wait()

	DebugLog("[AtomicReplaceMgr] Stopped (pending deletions: %d)", len(dm.pending))
}
