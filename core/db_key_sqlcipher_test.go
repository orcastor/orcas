//go:build sqlcipher
// +build sqlcipher

// This test file is specifically for testing SQLCipher functionality
// Run with: CGO_ENABLED=1 go test -tags sqlcipher -v ./core -run TestCreateDBWithKey

package core

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// TestCreateDBWithKey tests creating database with encryption key using SQLCipher
func TestCreateDBWithKey(t *testing.T) {
	Convey("Create database with key (SQLCipher)", t, func() {
		tmpDir, err := os.MkdirTemp("", "orcas_sqlcipher_test_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		testKey := "test-encryption-key-12345678901234567890123456789012"

		Convey("Create encrypted database with key", func() {
			// Initialize database with encryption key
			err := InitDB(tmpDir, testKey)
			So(err, ShouldBeNil)

			// Verify database file exists
			dbPath := filepath.Join(tmpDir, ".db")
			info, err := os.Stat(dbPath)
			So(err, ShouldBeNil)
			So(info.Size(), ShouldBeGreaterThan, 0)

			// Verify we can access with correct key
			db, err := GetWriteDB(tmpDir, testKey)
			So(err, ShouldBeNil)
			defer db.Close()

			// Test query
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldBeGreaterThan, 0)

			// Test write and read
			_, err = db.Exec("INSERT INTO usr (id, role, usr, pwd, name, avatar) VALUES (999, 1, 'test', 'pwd', 'test', '')")
			So(err, ShouldBeNil)

			var username string
			err = db.QueryRow("SELECT usr FROM usr WHERE id = 999").Scan(&username)
			So(err, ShouldBeNil)
			So(username, ShouldEqual, "test")
		})

		Convey("Cannot access encrypted database with wrong key", func() {
			// Initialize database with encryption key
			err := InitDB(tmpDir, testKey)
			So(err, ShouldBeNil)

			// Try to access with wrong key
			wrongKey := "wrong-key-12345678901234567890123456789012"
			db, err := GetWriteDB(tmpDir, wrongKey)
			// Connection may succeed, but query should fail
			if err == nil {
				defer db.Close()

				// Try to query - should fail with wrong key
				var count int
				err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
				// With SQLCipher, wrong key should cause query to fail
				So(err, ShouldNotBeNil)
			}
		})

		Convey("Create unencrypted database with empty key", func() {
			// Initialize database without encryption
			err := InitDB(tmpDir, "")
			So(err, ShouldBeNil)

			// Verify database file exists
			dbPath := filepath.Join(tmpDir, ".db")
			_, err = os.Stat(dbPath)
			So(err, ShouldBeNil)

			// Verify we can access without key
			db, err := GetWriteDB(tmpDir, "")
			So(err, ShouldBeNil)
			defer db.Close()

			// Test query
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldBeGreaterThan, 0)
		})

		Convey("Database pool with encryption key", func() {
			// Initialize connection pool
			InitDBPool(5, 3, 2, 0)

			// Initialize database with key
			err := InitDB(tmpDir, testKey)
			So(err, ShouldBeNil)

			// Get read connection
			readDB, err := GetReadDB(tmpDir, testKey)
			So(err, ShouldBeNil)
			defer readDB.Close()

			// Get write connection
			writeDB, err := GetWriteDB(tmpDir, testKey)
			So(err, ShouldBeNil)
			defer writeDB.Close()

			// Test read
			var count int
			err = readDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldBeGreaterThan, 0)

			// Test write
			_, err = writeDB.Exec("INSERT INTO usr (id, role, usr, pwd, name, avatar) VALUES (888, 1, 'pool_test', 'pwd', 'pool_test', '')")
			So(err, ShouldBeNil)

			// Verify read after write
			var username string
			err = readDB.QueryRow("SELECT usr FROM usr WHERE id = 888").Scan(&username)
			So(err, ShouldBeNil)
			So(username, ShouldEqual, "pool_test")
		})
	})
}

// TestDBKeyOperations tests various database operations with encryption key
func TestDBKeyOperations(t *testing.T) {
	Convey("Database operations with key", t, func() {
		tmpDir, err := os.MkdirTemp("", "orcas_sqlcipher_ops_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		testKey := "operation-test-key-12345678901234567890123456789012"

		Convey("Create, write, and read with key", func() {
			// Create database
			err := InitDB(tmpDir, testKey)
			So(err, ShouldBeNil)

			// Get database connection
			db, err := GetWriteDB(tmpDir, testKey)
			So(err, ShouldBeNil)
			defer db.Close()

			// Write data
			_, err = db.Exec(`
				INSERT INTO usr (id, role, usr, pwd, name, avatar) 
				VALUES (1001, 1, 'user1', 'hash1', 'User One', '')
			`)
			So(err, ShouldBeNil)

			_, err = db.Exec(`
				INSERT INTO usr (id, role, usr, pwd, name, avatar) 
				VALUES (1002, 0, 'user2', 'hash2', 'User Two', '')
			`)
			So(err, ShouldBeNil)

			// Read data
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM usr WHERE id >= 1000").Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 2)

			// Read specific user
			var name string
			err = db.QueryRow("SELECT name FROM usr WHERE id = 1001").Scan(&name)
			So(err, ShouldBeNil)
			So(name, ShouldEqual, "User One")
		})

		Convey("Transaction with encrypted database", func() {
			// Create database
			err := InitDB(tmpDir, testKey)
			So(err, ShouldBeNil)

			// Get database connection
			db, err := GetWriteDB(tmpDir, testKey)
			So(err, ShouldBeNil)
			defer db.Close()

			// Start transaction
			tx, err := db.Begin()
			So(err, ShouldBeNil)

			// Insert in transaction
			_, err = tx.Exec("INSERT INTO usr (id, role, usr, pwd, name, avatar) VALUES (2001, 1, 'tx_user', 'pwd', 'TX User', '')")
			So(err, ShouldBeNil)

			// Commit transaction
			err = tx.Commit()
			So(err, ShouldBeNil)

			// Verify data
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM usr WHERE id = 2001").Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 1)
		})
	})
}

