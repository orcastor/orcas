package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
)

// TestDatabaseSizeEstimate estimates database size for 100,000 objects
// Disabled by default - rename to TestDatabaseSizeEstimate to enable
func TestDatabaseSizeEstimateDisabled(t *testing.T) {
	t.Skip("This test is disabled by default due to long execution time")
	// Create test database
	testID := int64(999999999)
	baseDir := filepath.Join(os.TempDir(), fmt.Sprintf("o_db_size_%d", testID))
	os.MkdirAll(baseDir, 0o755)
	os.Setenv("ORCAS_BASE", baseDir)
	ORCAS_BASE = baseDir

	// Initialize database
	InitDB()
	defer os.RemoveAll(baseDir)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	InitBucketDB(context.TODO(), testBktID)

	dma := &DefaultMetadataAdapter{}

	// Get initial database size
	dbPath := filepath.Join(ORCAS_BASE, fmt.Sprintf("bkt_%d.db", testBktID))
	initialSize := getFileSize(dbPath)

	// Create objects and data entries
	numObjects := 1000 // Reduced from 100000 for faster testing
	batchSize := 100

	t.Logf("Creating %d objects in batches of %d...", numObjects, batchSize)

	for i := 0; i < numObjects; i += batchSize {
		batch := make([]*ObjectInfo, 0, batchSize)
		dataBatch := make([]*DataInfo, 0, batchSize)

		for j := 0; j < batchSize && i+j < numObjects; j++ {
			objID, _ := ig.New()
			dataID, _ := ig.New()

			// Create object
			obj := &ObjectInfo{
				ID:     objID,
				PID:    0, // Root directory
				MTime:  Now(),
				DataID: dataID,
				Type:   OBJ_TYPE_FILE,
				Name:   fmt.Sprintf("file_%d.txt", i+j),
				Size:   1024 * 1024, // 1MB average file size
				Extra:  "{}",
			}
			batch = append(batch, obj)

			// Create corresponding data entry
			data := &DataInfo{
				ID:        dataID,
				Size:      1024 * 1024, // 1MB compressed/encrypted size
				OrigSize:  1024 * 1024, // 1MB original size
				HdrXXH3:   12345678,
				XXH3:      87654321,
				SHA256_0:  int64(i + j),
				SHA256_1:  int64(i + j + 1),
				SHA256_2:  int64(i + j + 2),
				SHA256_3:  int64(i + j + 3),
				Cksum:     11111111,
				Kind:      DATA_NORMAL,
				PkgID:     0,
				PkgOffset: 0,
			}
			dataBatch = append(dataBatch, data)
		}

		// Insert batch
		_, err := dma.PutObj(c, testBktID, batch)
		if err != nil {
			t.Fatalf("PutObj failed: %v", err)
		}

		err = dma.PutData(c, testBktID, dataBatch)
		if err != nil {
			t.Fatalf("PutData failed: %v", err)
		}

		if (i+batchSize)%500 == 0 {
			// Close and reopen DB to ensure data is flushed
			db, err := GetDB(c, testBktID)
			if err == nil {
				db.Close()
			}
			currentSize := getFileSize(dbPath)
			t.Logf("Progress: %d/%d objects, DB size: %.2f MB", i+batchSize, numObjects, float64(currentSize)/(1024*1024))
		}
	}

	// Get final database size
	// Close DB connection to ensure all data is flushed
	db, err := GetDB(c, testBktID)
	if err == nil {
		db.Close()
	}
	// Wait a bit for file system to sync
	time.Sleep(100 * time.Millisecond)

	finalSize := getFileSize(dbPath)
	actualSize := finalSize - initialSize

	// Calculate per-object size
	perObjectSize := float64(actualSize) / float64(numObjects)

	t.Logf("\n=== Database Size Estimation Results ===")
	t.Logf("Number of objects: %d", numObjects)
	t.Logf("Initial DB size: %.2f MB", float64(initialSize)/(1024*1024))
	t.Logf("Final DB size: %.2f MB", float64(finalSize)/(1024*1024))
	t.Logf("Actual data size: %.2f MB", float64(actualSize)/(1024*1024))
	t.Logf("Per-object size: %.2f bytes", perObjectSize)
	t.Logf("\nEstimated size for 100,000 objects: %.2f MB", float64(actualSize)/(1024*1024))

	// Breakdown estimation
	t.Logf("\n=== Size Breakdown (estimated) ===")
	objTableSize := float64(numObjects) * 85  // ~85 bytes per obj row
	dataTableSize := float64(numObjects) * 70 // ~70 bytes per data row
	indexSize := float64(numObjects) * 65     // ~65 bytes per index entry (2 indexes)
	overhead := float64(actualSize) - objTableSize - dataTableSize - indexSize

	t.Logf("obj table: %.2f MB (%.1f%%)", objTableSize/(1024*1024), objTableSize/float64(actualSize)*100)
	t.Logf("data table: %.2f MB (%.1f%%)", dataTableSize/(1024*1024), dataTableSize/float64(actualSize)*100)
	t.Logf("indexes: %.2f MB (%.1f%%)", indexSize/(1024*1024), indexSize/float64(actualSize)*100)
	t.Logf("overhead: %.2f MB (%.1f%%)", overhead/(1024*1024), overhead/float64(actualSize)*100)
}

// getFileSize returns file size in bytes
func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// TestDatabaseSizeEstimateWithVersions estimates database size with version history
// Disabled by default - rename to TestDatabaseSizeEstimateWithVersions to enable
func TestDatabaseSizeEstimateWithVersionsDisabled(t *testing.T) {
	t.Skip("This test is disabled by default due to long execution time")
	// Create test database
	testID := int64(888888888)
	baseDir := filepath.Join(os.TempDir(), fmt.Sprintf("o_db_size_v_%d", testID))
	os.MkdirAll(baseDir, 0o755)
	os.Setenv("ORCAS_BASE", baseDir)
	ORCAS_BASE = baseDir

	// Initialize database
	InitDB()
	defer os.RemoveAll(baseDir)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	InitBucketDB(context.TODO(), testBktID)

	dma := &DefaultMetadataAdapter{}

	// Get initial database size
	dbPath := filepath.Join(ORCAS_BASE, fmt.Sprintf("bkt_%d.db", testBktID))
	initialSize := getFileSize(dbPath)

	// Create objects with version history
	numFiles := 100                                  // Reduced from 10000 for faster testing
	versionsPerFile := 5                             // Reduced from 10 for faster testing
	totalObjects := numFiles * (1 + versionsPerFile) // files + versions

	t.Logf("Creating %d files with %d versions each (total %d objects)...", numFiles, versionsPerFile, totalObjects)

	for i := 0; i < numFiles; i++ {
		// Create file object
		fileObjID, _ := ig.New()

		fileObj := &ObjectInfo{
			ID:     fileObjID,
			PID:    0,
			MTime:  Now(),
			DataID: 0, // Will be set by latest version
			Type:   OBJ_TYPE_FILE,
			Name:   fmt.Sprintf("file_%d.txt", i),
			Size:   1024 * 1024,
			Extra:  "{}",
		}

		// Create versions
		objs := make([]*ObjectInfo, 0, versionsPerFile+1)
		dataObjs := make([]*DataInfo, 0, versionsPerFile)

		objs = append(objs, fileObj)

		for v := 0; v < versionsPerFile; v++ {
			versionID, _ := ig.New()
			dataID, _ := ig.New()

			version := &ObjectInfo{
				ID:     versionID,
				PID:    fileObjID,
				MTime:  Now() + int64(v),
				DataID: dataID,
				Type:   OBJ_TYPE_VERSION,
				Name:   fmt.Sprintf("%d", Now()+int64(v)),
				Size:   1024 * 1024,
				Extra:  "{}",
			}
			objs = append(objs, version)

			data := &DataInfo{
				ID:        dataID,
				Size:      1024 * 1024,
				OrigSize:  1024 * 1024,
				HdrXXH3:   uint64(i*1000 + v),
				XXH3:      uint64(i*2000 + v),
				SHA256_0:  int64(i*3000 + v),
				SHA256_1:  int64(i*3001 + v),
				SHA256_2:  int64(i*3002 + v),
				SHA256_3:  int64(i*3003 + v),
				Cksum:     uint64(i*4000 + v),
				Kind:      DATA_NORMAL,
				PkgID:     0,
				PkgOffset: 0,
			}
			dataObjs = append(dataObjs, data)
		}

		// Update file's DataID to latest version
		fileObj.DataID = dataObjs[len(dataObjs)-1].ID

		// Insert batch
		_, err := dma.PutObj(c, testBktID, objs)
		if err != nil {
			t.Fatalf("PutObj failed: %v", err)
		}

		err = dma.PutData(c, testBktID, dataObjs)
		if err != nil {
			t.Fatalf("PutData failed: %v", err)
		}

		if (i+1)%50 == 0 {
			currentSize := getFileSize(dbPath)
			t.Logf("Progress: %d/%d files, DB size: %.2f MB", i+1, numFiles, float64(currentSize)/(1024*1024))
		}
	}

	// Get final database size
	finalSize := getFileSize(dbPath)
	actualSize := finalSize - initialSize

	t.Logf("\n=== Database Size with Version History ===")
	t.Logf("Number of files: %d", numFiles)
	t.Logf("Versions per file: %d", versionsPerFile)
	t.Logf("Total objects: %d", totalObjects)
	t.Logf("Total data entries: %d", numFiles*versionsPerFile)
	t.Logf("Final DB size: %.2f MB", float64(finalSize)/(1024*1024))
	t.Logf("Actual data size: %.2f MB", float64(actualSize)/(1024*1024))
	t.Logf("Per-file size: %.2f KB", float64(actualSize)/(1024*float64(numFiles)))
	t.Logf("Per-object size: %.2f bytes", float64(actualSize)/float64(totalObjects))
}
