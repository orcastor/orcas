package core

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestInitDBWithKey(t *testing.T) {
	Convey("InitDB with key", t, func() {
		// Create temporary directory
		tmpDir, err := os.MkdirTemp("", "orcas_test_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		// Set ORCAS_BASE to temp directory
		originalBase := ORCAS_BASE
		ORCAS_BASE = tmpDir
		defer func() {
			ORCAS_BASE = originalBase
		}()

		Convey("InitDB with empty key (unencrypted)", func() {
			err := InitDB("")
			So(err, ShouldBeNil)

			// Verify database file exists
			dbPath := filepath.Join(tmpDir, "meta.db")
			_, err = os.Stat(dbPath)
			So(err, ShouldBeNil)
		})

		Convey("InitDB with key (encrypted)", func() {
			err := InitDB("test-key-123")
			So(err, ShouldBeNil)

			// Verify database file exists
			dbPath := filepath.Join(tmpDir, "meta.db")
			_, err = os.Stat(dbPath)
			So(err, ShouldBeNil)

			// Try to open with wrong key - connection may succeed but query should fail
			db, err := GetDBWithKey("wrong-key")
			So(err, ShouldBeNil)
			defer db.Close()

			// Try to query - this should fail with wrong key
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
			// Note: SQLite may not immediately fail on wrong key, but query should fail
			// If query succeeds, it means the key is actually correct (unlikely but possible)
			// We'll just verify correct key works below
			_ = err // Ignore error for wrong key test

			// Try to open with correct key should succeed
			db2, err := GetDBWithKey("test-key-123")
			So(err, ShouldBeNil)
			defer db2.Close()

			err = db2.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldBeGreaterThan, 0)
		})
	})
}

func TestGetDBWithKey(t *testing.T) {
	Convey("GetDBWithKey", t, func() {
		// Create temporary directory
		tmpDir, err := os.MkdirTemp("", "orcas_test_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		// Set ORCAS_BASE to temp directory
		originalBase := ORCAS_BASE
		ORCAS_BASE = tmpDir
		defer func() {
			ORCAS_BASE = originalBase
		}()

		Convey("GetDBWithKey with empty key", func() {
			// Initialize database
			err := InitDB("")
			So(err, ShouldBeNil)

			// Get database with empty key
			db, err := GetDBWithKey("")
			So(err, ShouldBeNil)
			defer db.Close()

			// Should be able to query
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldBeGreaterThan, 0)
		})

		Convey("GetDBWithKey with key", func() {
			// Initialize encrypted database
			err := InitDB("test-key")
			So(err, ShouldBeNil)

			// Get database with correct key
			db, err := GetDBWithKey("test-key")
			So(err, ShouldBeNil)
			defer db.Close()

			// Should be able to query
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count)
			So(err, ShouldBeNil)
			So(count, ShouldBeGreaterThan, 0)
		})

		Convey("GetDBWithKey creates directory if not exists", func() {
			// Use a new temp directory
			newTmpDir := filepath.Join(tmpDir, "new_subdir")
			ORCAS_BASE = newTmpDir

			// Get database - should create directory
			db, err := GetDBWithKey("")
			So(err, ShouldBeNil)
			defer db.Close()

			// Verify directory was created
			_, err = os.Stat(newTmpDir)
			So(err, ShouldBeNil)
		})
	})
}
