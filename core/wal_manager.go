package core

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// WALManager manages SQLite WAL checkpoints and vacuuming
type WALManager struct {
	basePath string
	dbKey    string

	checkpointInterval time.Duration // How often to run checkpoint
	vacuumInterval     time.Duration // How often to run vacuum

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	running bool
}

// WALManagerConfig holds configuration for WAL management
type WALManagerConfig struct {
	// CheckpointInterval is how often to run WAL checkpoint (default: 5 minutes)
	CheckpointInterval time.Duration

	// VacuumInterval is how often to run VACUUM (default: 1 hour)
	VacuumInterval time.Duration
}

// DefaultWALManagerConfig returns default configuration
func DefaultWALManagerConfig() WALManagerConfig {
	return WALManagerConfig{
		CheckpointInterval: 5 * time.Minute,
		VacuumInterval:     1 * time.Hour,
	}
}

// NewWALManager creates a new WAL manager
func NewWALManager(basePath, dbKey string, config WALManagerConfig) *WALManager {
	if config.CheckpointInterval == 0 {
		config.CheckpointInterval = 5 * time.Minute
	}
	if config.VacuumInterval == 0 {
		config.VacuumInterval = 1 * time.Hour
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WALManager{
		basePath:           basePath,
		dbKey:              dbKey,
		checkpointInterval: config.CheckpointInterval,
		vacuumInterval:     config.VacuumInterval,
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start starts the WAL manager background tasks
func (wm *WALManager) Start() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.running {
		return fmt.Errorf("WAL manager already running")
	}

	wm.running = true

	// Start checkpoint goroutine
	wm.wg.Add(1)
	go wm.checkpointLoop()

	// Start vacuum goroutine
	wm.wg.Add(1)
	go wm.vacuumLoop()

	log.Printf("[WAL Manager] Started: checkpointInterval=%v, vacuumInterval=%v",
		wm.checkpointInterval, wm.vacuumInterval)

	return nil
}

// Stop stops the WAL manager
func (wm *WALManager) Stop() {
	wm.mu.Lock()
	if !wm.running {
		wm.mu.Unlock()
		return
	}
	wm.running = false
	wm.mu.Unlock()

	wm.cancel()
	wm.wg.Wait()

	log.Printf("[WAL Manager] Stopped")
}

// checkpointLoop runs periodic WAL checkpoints
func (wm *WALManager) checkpointLoop() {
	defer wm.wg.Done()

	ticker := time.NewTicker(wm.checkpointInterval)
	defer ticker.Stop()

	// Run checkpoint immediately on start
	wm.runCheckpoint()

	for {
		select {
		case <-wm.ctx.Done():
			return
		case <-ticker.C:
			wm.runCheckpoint()
		}
	}
}

// vacuumLoop runs periodic VACUUM
func (wm *WALManager) vacuumLoop() {
	defer wm.wg.Done()

	ticker := time.NewTicker(wm.vacuumInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wm.ctx.Done():
			return
		case <-ticker.C:
			wm.runVacuum()
		}
	}
}

// runCheckpoint executes a WAL checkpoint
func (wm *WALManager) runCheckpoint() {
	start := time.Now()

	db, err := GetWriteDB(wm.basePath, wm.dbKey)
	if err != nil {
		log.Printf("[WAL Manager] ERROR: Failed to get database connection for checkpoint: %v", err)
		return
	}
	// Don't close - it's from the pool
	
	// PRAGMA wal_checkpoint(TRUNCATE) will:
	// 1. Checkpoint all frames in the WAL file to the database
	// 2. Truncate the WAL file to zero bytes
	// This is more aggressive than PASSIVE or FULL, but ensures readers see latest data
	var busy, logPages, checkpointed int
	err = db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logPages, &checkpointed)
	if err != nil {
		log.Printf("[WAL Manager] ERROR: Failed to run checkpoint: %v", err)
		return
	}
	
	duration := time.Since(start)
	log.Printf("[WAL Manager] Checkpoint completed: busy=%d, log=%d, checkpointed=%d, duration=%v",
		busy, logPages, checkpointed, duration)
}

// runVacuum executes a VACUUM to reclaim space and optimize database
func (wm *WALManager) runVacuum() {
	start := time.Now()

	db, err := GetWriteDB(wm.basePath, wm.dbKey)
	if err != nil {
		log.Printf("[WAL Manager] ERROR: Failed to get database connection for vacuum: %v", err)
		return
	}
	// Don't close - it's from the pool
	
	// VACUUM rebuilds the database file, repacking it into a minimal amount of disk space
	// This also helps with read performance
	_, err = db.Exec("VACUUM")
	if err != nil {
		log.Printf("[WAL Manager] ERROR: Failed to run vacuum: %v", err)
		return
	}
	
	duration := time.Since(start)
	log.Printf("[WAL Manager] Vacuum completed: duration=%v", duration)
}

// ForceCheckpoint forces an immediate checkpoint
func (wm *WALManager) ForceCheckpoint() error {
	db, err := GetWriteDB(wm.basePath, wm.dbKey)
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	var busy, logPages, checkpointed int
	err = db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logPages, &checkpointed)
	if err != nil {
		return fmt.Errorf("failed to run checkpoint: %w", err)
	}
	
	log.Printf("[WAL Manager] Force checkpoint: busy=%d, log=%d, checkpointed=%d",
		busy, logPages, checkpointed)

	return nil
}

// ForceVacuum forces an immediate vacuum
func (wm *WALManager) ForceVacuum() error {
	db, err := GetWriteDB(wm.basePath, wm.dbKey)
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	_, err = db.Exec("VACUUM")
	if err != nil {
		return fmt.Errorf("failed to run vacuum: %w", err)
	}
	
	log.Printf("[WAL Manager] Force vacuum completed")

	return nil
}

// GetWALSize returns the current size of the WAL file in pages
func (wm *WALManager) GetWALSize() (int, error) {
	db, err := GetReadDB(wm.basePath, wm.dbKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get database connection: %w", err)
	}

	var walSize int
	err = db.QueryRow("PRAGMA wal_checkpoint").Scan(&walSize)
	if err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("failed to get WAL size: %w", err)
	}

	return walSize, nil
}

// GetDatabaseStats returns statistics about the database
func (wm *WALManager) GetDatabaseStats() (map[string]interface{}, error) {
	db, err := GetReadDB(wm.basePath, wm.dbKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}

	stats := make(map[string]interface{})

	// Get page count
	var pageCount int
	err = db.QueryRow("PRAGMA page_count").Scan(&pageCount)
	if err == nil {
		stats["page_count"] = pageCount
	}

	// Get page size
	var pageSize int
	err = db.QueryRow("PRAGMA page_size").Scan(&pageSize)
	if err == nil {
		stats["page_size"] = pageSize
		stats["database_size_bytes"] = pageCount * pageSize
	}

	// Get freelist count
	var freelistCount int
	err = db.QueryRow("PRAGMA freelist_count").Scan(&freelistCount)
	if err == nil {
		stats["freelist_count"] = freelistCount
		stats["wasted_bytes"] = freelistCount * pageSize
	}

	return stats, nil
}
