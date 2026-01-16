package core

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// DBConnectionType represents the type of database connection
type DBConnectionType int

const (
	// DBRead represents a read-only connection
	DBRead DBConnectionType = iota
	// DBWrite represents a write connection
	DBWrite
)

// DBPool manages database connection pools with read/write separation
type DBPool struct {
	// pools stores connection pools for different databases
	// key: database path, value: *DatabasePool
	pools sync.Map

	// Configuration
	maxReadConns    int           // Maximum read connections per database
	maxWriteConns   int           // Maximum write connections per database
	maxIdleConns    int           // Maximum idle connections per pool
	connMaxLifetime time.Duration // Maximum lifetime of a connection

	// Mutex for synchronizing pool creation
	createMu sync.Mutex
}

// DatabasePool manages read and write connection pools for a single database
type DatabasePool struct {
	readPool  *sql.DB
	writePool *sql.DB
	mu        sync.RWMutex
	path      string
	key       string
	refCount  int64 // Reference count for connection pool
}

var (
	// globalDBPool is the global database connection pool manager
	globalDBPool *DBPool
	poolOnce     sync.Once
)

// InitDBPool initializes the global database connection pool
func InitDBPool(maxReadConns, maxWriteConns, maxIdleConns int, connMaxLifetime time.Duration) {
	poolOnce.Do(func() {
		globalDBPool = &DBPool{
			maxReadConns:    maxReadConns,
			maxWriteConns:   maxWriteConns,
			maxIdleConns:    maxIdleConns,
			connMaxLifetime: connMaxLifetime,
		}
	})
}

// GetDBPool returns the global database connection pool
func GetDBPool() *DBPool {
	if globalDBPool == nil {
		// Initialize with default values if not initialized
		InitDBPool(10, 5, 5, 0)
	}
	return globalDBPool
}

// getDatabasePool gets or creates a database pool for the given path
func (dp *DBPool) getDatabasePool(dirPath string) (*DatabasePool, error) {
	// Create a unique key for this database (only use path)
	dbPath := filepath.Join(dirPath, ".db")
	poolKey := dbPath

	// Try to get existing pool (with double-check locking pattern)
	if pool, ok := dp.pools.Load(poolKey); ok {
		if dbPool, ok := pool.(*DatabasePool); ok {
			dbPool.mu.Lock()
			dbPool.refCount++
			dbPool.mu.Unlock()
			return dbPool, nil
		}
	}

	// Create new pool (need to synchronize to avoid race condition)
	// Use pool's mutex to ensure only one goroutine creates the pool
	dp.createMu.Lock()
	defer dp.createMu.Unlock()

	// Double-check after acquiring lock
	if pool, ok := dp.pools.Load(poolKey); ok {
		if dbPool, ok := pool.(*DatabasePool); ok {
			dbPool.mu.Lock()
			dbPool.refCount++
			dbPool.mu.Unlock()
			return dbPool, nil
		}
	}

	// Create new pool
	dbPool := &DatabasePool{
		path:     dbPath,
		key:      "", // No longer use dbKey for database operations
		refCount: 1,
	}

	// Create read pool
	readDB, err := dp.createConnection(dbPath, true)
	if err != nil {
		return nil, err
	}
	dbPool.readPool = readDB

	// Create write pool
	writeDB, err := dp.createConnection(dbPath, false)
	if err != nil {
		readDB.Close()
		return nil, err
	}
	dbPool.writePool = writeDB

	// Store pool
	dp.pools.Store(poolKey, dbPool)

	return dbPool, nil
}

// createConnection creates a database connection with appropriate settings
func (dp *DBPool) createConnection(dbPath string, readOnly bool) (*sql.DB, error) {
	// SQLite connection parameters optimized for performance
	// Note: For read-only connections, we still use rwc mode because:
	// 1. SQLite WAL mode allows concurrent reads even with rwc
	// 2. Read-only mode (ro) prevents temporary table creation which is needed in some operations
	// 3. WAL mode provides excellent read concurrency without blocking writes
	param := "?_journal=WAL&cache=shared&mode=rwc&_busy_timeout=10000&_txlock=immediate"

	// Ensure directory exists
	os.MkdirAll(filepath.Dir(dbPath), 0o766)

	db, err := sql.Open("sqlite3", dbPath+param)
	if err != nil {
		return nil, err
	}

	// Configure connection pool
	if readOnly {
		db.SetMaxOpenConns(dp.maxReadConns)
	} else {
		db.SetMaxOpenConns(dp.maxWriteConns)
	}
	db.SetMaxIdleConns(dp.maxIdleConns)
	db.SetConnMaxLifetime(dp.connMaxLifetime)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// GetDB gets a database connection from the pool
// connType specifies whether to use read or write connection
func (dp *DBPool) GetDB(connType DBConnectionType, dirPath string) (*sql.DB, error) {
	dbPool, err := dp.getDatabasePool(dirPath)
	if err != nil {
		return nil, err
	}

	if connType == DBRead {
		return dbPool.readPool, nil
	}
	return dbPool.writePool, nil
}

// ReleaseDB releases a reference to a database pool
func (dp *DBPool) ReleaseDB(dirPath string) {
	dbPath := filepath.Join(dirPath, ".db")
	poolKey := dbPath

	if pool, ok := dp.pools.Load(poolKey); ok {
		if dbPool, ok := pool.(*DatabasePool); ok {
			dbPool.mu.Lock()
			dbPool.refCount--
			if dbPool.refCount <= 0 {
				// Close connections and remove from pool
				// CRITICAL: Close connections before removing from pool
				// This ensures all file handles are released
				dbPool.readPool.Close()
				dbPool.writePool.Close()
				dp.pools.Delete(poolKey)
			}
			dbPool.mu.Unlock()
		}
	}
}

// ForceReleaseDB forcefully releases a database pool regardless of reference count
// This is useful when you need to ensure all connections are closed
func (dp *DBPool) ForceReleaseDB(dirPath string) {
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	needDelete := make([]string, 0)
	dp.pools.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), dirPath) {
			if dbPool, ok := value.(*DatabasePool); ok {
				dbPool.mu.Lock()
				dbPool.readPool.Close()
				dbPool.writePool.Close()
				needDelete = append(needDelete, key.(string))
				dbPool.mu.Unlock()
			}
		}
		return true
	})

	for _, key := range needDelete {
		dp.pools.Delete(key)
	}
}

// Close closes all database connections in the pool
// CRITICAL: This forcefully closes all connections regardless of reference count
// Use this when you need to ensure all database files are released
// This allows external components (like WAL checkpoint manager) to stop first
func (dp *DBPool) Close() {
	dp.pools.Range(func(key, value interface{}) bool {
		if dbPool, ok := value.(*DatabasePool); ok {
			// Force close connections regardless of reference count
			dbPool.readPool.Close()
			dbPool.writePool.Close()
		}
		dp.pools.Delete(key)
		return true
	})
}

// GetDBStats returns statistics about the connection pool
func (dp *DBPool) GetDBStats() map[string]interface{} {
	stats := make(map[string]interface{})
	poolCount := 0
	totalReadConns := 0
	totalWriteConns := 0
	totalIdleReadConns := 0
	totalIdleWriteConns := 0

	dp.pools.Range(func(key, value interface{}) bool {
		if dbPool, ok := value.(*DatabasePool); ok {
			poolCount++
			readStats := dbPool.readPool.Stats()
			writeStats := dbPool.writePool.Stats()
			totalReadConns += readStats.OpenConnections
			totalWriteConns += writeStats.OpenConnections
			totalIdleReadConns += readStats.Idle
			totalIdleWriteConns += writeStats.Idle
		}
		return true
	})

	stats["pool_count"] = poolCount
	stats["total_read_connections"] = totalReadConns
	stats["total_write_connections"] = totalWriteConns
	stats["total_idle_read_connections"] = totalIdleReadConns
	stats["total_idle_write_connections"] = totalIdleWriteConns

	return stats
}

// GetDBWithType gets a database connection from the pool with specified connection type
// dirPath: path for database directory (empty string defaults to current directory ".")
func GetDBWithType(connType DBConnectionType, dirPath string) (*sql.DB, error) {
	pool := GetDBPool()

	// Default path
	if dirPath == "" {
		dirPath = "."
	}

	return pool.GetDB(connType, dirPath)
}

// GetReadDB gets a read-only database connection
// dirPath: path for database directory (empty string defaults to current directory ".")
func GetReadDB(dirPath string) (*sql.DB, error) {
	return GetDBWithType(DBRead, dirPath)
}

// GetWriteDB gets a write database connection
// dirPath: path for database directory (empty string defaults to current directory ".")
func GetWriteDB(dirPath string) (*sql.DB, error) {
	return GetDBWithType(DBWrite, dirPath)
}
