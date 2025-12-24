// Package core provides test helper functions for managing test environment
package core

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var (
	testBaseDir  string
	testDataDir  string
	testInitOnce sync.Once
)

// InitTestEnv initializes test environment with in-memory filesystem (tmpfs)
// This sets up test directories to use /dev/shm (shared memory) for faster tests
func InitTestEnv() {
	testInitOnce.Do(func() {
		// Use /dev/shm (shared memory) if available, otherwise use /tmp
		shmPath := "/dev/shm"
		if _, err := os.Stat(shmPath); os.IsNotExist(err) {
			shmPath = "/tmp"
		}

		// Create unique test directories
		testBaseDir = filepath.Join(shmPath, "orcas_test_base")
		testDataDir = filepath.Join(shmPath, "orcas_test_data")

		// Clean up any existing test directories
		os.RemoveAll(testBaseDir)
		os.RemoveAll(testDataDir)

		// Create directories
		os.MkdirAll(testBaseDir, 0o766)
		os.MkdirAll(testDataDir, 0o766)
	})
}

// CleanupTestEnv cleans up test environment
func CleanupTestEnv() {
	if testBaseDir != "" {
		os.RemoveAll(testBaseDir)
	}
	if testDataDir != "" {
		os.RemoveAll(testDataDir)
	}
}

// CleanTestDB cleans up database files for a specific bucket
// This also closes connection pools to ensure clean state
func CleanTestDB(bktID int64) error {
	// Close all connection pools first to release file locks
	// This ensures that database files can be safely deleted
	pool := GetDBPool()
	pool.Close() // Close all pools to release all connections

	// Clean main database
	if testBaseDir != "" {
		dbPath := filepath.Join(testBaseDir, "meta.db")
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
		os.Remove(dbPath + "-shm")
	}

	// Clean bucket database
	if testDataDir != "" && bktID > 0 {
		bktDir := filepath.Join(testDataDir, fmt.Sprint(bktID))
		dbPath := filepath.Join(bktDir, "meta.db")
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
		os.Remove(dbPath + "-shm")
		// Remove all files in bucket directory
		os.RemoveAll(bktDir)
	}

	return nil
}

// CleanTestBucketData cleans up all data files for a specific bucket
func CleanTestBucketData(bktID int64) error {
	if testDataDir != "" && bktID > 0 {
		bktDir := filepath.Join(testDataDir, fmt.Sprint(bktID))
		// Remove all files in bucket directory except meta.db
		entries, err := os.ReadDir(bktDir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != "meta.db" && entry.Name() != "meta.db-wal" && entry.Name() != "meta.db-shm" {
				os.Remove(filepath.Join(bktDir, entry.Name()))
			}
		}
	}
	return nil
}

// CleanAllTestData cleans up all test data
func CleanAllTestData() error {
	if testBaseDir != "" {
		os.RemoveAll(testBaseDir)
		os.MkdirAll(testBaseDir, 0o766)
	}
	if testDataDir != "" {
		os.RemoveAll(testDataDir)
		os.MkdirAll(testDataDir, 0o766)
	}
	return nil
}
