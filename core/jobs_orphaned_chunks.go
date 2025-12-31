package core

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ScanOrphanedChunks scans orphaned chunks (chunks without metadata or without object references)
// Scans all chunk files in the data directory, checks if DataInfo exists and if it's referenced by any ObjectInfo
// Every 1000 chunks, checks metadata and reference counts. If DataInfo doesn't exist or has no object references,
// delays and re-checks. If still orphaned after delay, deletes the chunk
// delaySeconds: delay time in seconds before re-checking and deleting orphaned chunks
func ScanOrphanedChunks(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter, delaySeconds int) (*ScanOrphanedChunksResult, error) {
	result := &ScanOrphanedChunksResult{
		OrphanedChunks: []int64{},
		Errors:         []string{},
	}

	// Get data path
	dataPath := getDataPathFromAdapter(ma)
	// Check if dataPath already contains bktID as the last component
	// If the dataPath ends with bktID, use it directly; otherwise join with bktID
	bktDataPath := dataPath
	if filepath.Base(dataPath) != fmt.Sprint(bktID) {
		bktDataPath = filepath.Join(dataPath, fmt.Sprint(bktID))
	}

	// Check if bucket data directory exists
	if _, err := os.Stat(bktDataPath); os.IsNotExist(err) {
		return result, nil // No data directory, nothing to scan
	}

	// Initialize resource controller
	rc := NewResourceController(GetResourceControlConfig())

	// Map to track all chunks by dataID: dataID -> []chunkInfo
	type chunkInfo struct {
		path string
		size int64
	}
	chunksByDataID := make(map[int64][]chunkInfo)
	// Map to track which dataIDs we've already checked
	checkedDataIDs := make(map[int64]bool)
	// Map to track orphaned dataIDs (metadata doesn't exist)
	orphanedDataIDs := make(map[int64]bool)

	// Scan all chunk files in the bucket directory
	// Chunk files are stored in: dataPath/bktID/hash[21:24]/hash[8:24]/dataID_sn
	scanCount := 0
	batchSize := 1000

	// Recursive function to scan directory using os.ReadDir
	var scanDir func(dirPath string) error
	scanDir = func(dirPath string) error {
		// Check if should stop
		if rc.ShouldStop() {
			return fmt.Errorf("resource controller requested stop")
		}

		entries, err := os.ReadDir(dirPath)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("error reading directory %s: %v", dirPath, err))
			return nil // Continue with other directories
		}

		for _, entry := range entries {
			// Check if should stop
			if rc.ShouldStop() {
				return fmt.Errorf("resource controller requested stop")
			}

			fullPath := filepath.Join(dirPath, entry.Name())

			// If it's a directory, recurse into it
			if entry.IsDir() {
				if err := scanDir(fullPath); err != nil {
					return err
				}
				continue
			}

			// It's a file, process it
			info, err := entry.Info()
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("error getting file info for %s: %v", fullPath, err))
				continue
			}

			// Extract dataID and sn from filename
			// Filename format: dataID_sn
			fileName := info.Name()
			// Skip database files and other non-chunk files
			if fileName == ".db" || fileName == ".db-shm" || fileName == ".db-wal" {
				continue
			}
			if !strings.Contains(fileName, "_") {
				continue // Not a chunk file, skip
			}

			parts := strings.Split(fileName, "_")
			if len(parts) != 2 {
				continue // Invalid format, skip
			}

			dataID, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				continue // Invalid dataID, skip
			}

			_, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				continue // Invalid sn, skip
			}

			// Verify this is actually a chunk file by checking path structure
			// Path should be: dataPath/bktID/hash[21:24]/hash[8:24]/dataID_sn
			expectedHash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
			if len(expectedHash) < 24 {
				continue // Invalid hash length, skip
			}
			expectedSubDir1 := expectedHash[21:24]
			expectedSubDir2 := expectedHash[8:24]
			dir := filepath.Dir(fullPath)
			// Check if path contains expected hash subdirectories
			// The path structure is: .../hash[21:24]/hash[8:24]/filename
			// So we check if the directory path contains both subdirectories
			// Use a more lenient check: verify the directory path contains both hash parts
			dirStr := dir
			if !strings.Contains(dirStr, expectedSubDir1) || !strings.Contains(dirStr, expectedSubDir2) {
				continue // Path doesn't match expected structure, skip
			}

			scanCount++
			result.TotalScanned++

			// Add chunk to dataID mapping
			if chunksByDataID[dataID] == nil {
				chunksByDataID[dataID] = []chunkInfo{}
			}
			chunksByDataID[dataID] = append(chunksByDataID[dataID], chunkInfo{
				path: fullPath,
				size: info.Size(),
			})

			// Every batchSize chunks, check metadata for collected dataIDs
			if scanCount%batchSize == 0 {
				// Get unique dataIDs that haven't been checked yet
				batchDataIDs := make([]int64, 0)
				for dataID := range chunksByDataID {
					if !checkedDataIDs[dataID] {
						batchDataIDs = append(batchDataIDs, dataID)
						checkedDataIDs[dataID] = true
					}
				}

				// Check metadata and references for all dataIDs in this batch
				if len(batchDataIDs) > 0 {
					// First, check which DataInfo exist
					existingDataIDs := make([]int64, 0)
					for _, checkDataID := range batchDataIDs {
						_, err := ma.GetData(c, bktID, checkDataID)
						if err != nil {
							// DataInfo doesn't exist, mark as orphaned
							orphanedDataIDs[checkDataID] = true
						} else {
							// DataInfo exists, need to check if it's referenced by any ObjectInfo
							existingDataIDs = append(existingDataIDs, checkDataID)
						}
					}

					// Check reference counts for existing DataInfo
					if len(existingDataIDs) > 0 {
						refCounts, err := ma.CountDataRefs(c, bktID, existingDataIDs)
						if err != nil {
							// If query fails, mark all as potentially orphaned (safer to re-check later)
							for _, dataID := range existingDataIDs {
								orphanedDataIDs[dataID] = true
							}
						} else {
							// Mark dataIDs with refCount == 0 as orphaned
							for _, dataID := range existingDataIDs {
								if refCounts[dataID] == 0 {
									orphanedDataIDs[dataID] = true
								}
							}
						}
					}
				}

				// Batch processing interval
				rc.WaitIfNeeded(batchSize)
			}
		}

		return nil
	}

	// Start scanning from bucket data directory
	err := scanDir(bktDataPath)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("error scanning directory: %v", err))
	}

	// Check remaining dataIDs that weren't checked in batches
	remainingDataIDs := make([]int64, 0)
	for dataID := range chunksByDataID {
		if !checkedDataIDs[dataID] {
			remainingDataIDs = append(remainingDataIDs, dataID)
			checkedDataIDs[dataID] = true
		}
	}

	// Check remaining dataIDs
	if len(remainingDataIDs) > 0 {
		// First, check which DataInfo exist
		existingDataIDs := make([]int64, 0)
		for _, dataID := range remainingDataIDs {
			_, err := ma.GetData(c, bktID, dataID)
			if err != nil {
				// DataInfo doesn't exist, mark as orphaned
				orphanedDataIDs[dataID] = true
			} else {
				// DataInfo exists, need to check if it's referenced by any ObjectInfo
				existingDataIDs = append(existingDataIDs, dataID)
			}
		}

		// Check reference counts for existing DataInfo
		if len(existingDataIDs) > 0 {
			refCounts, err := ma.CountDataRefs(c, bktID, existingDataIDs)
			if err != nil {
				// If query fails, mark all as potentially orphaned (safer to re-check later)
				for _, dataID := range existingDataIDs {
					orphanedDataIDs[dataID] = true
				}
			} else {
				// Mark dataIDs with refCount == 0 as orphaned
				for _, dataID := range existingDataIDs {
					if refCounts[dataID] == 0 {
						orphanedDataIDs[dataID] = true
					}
				}
			}
		}
	}

	// Collect all orphaned chunks
	orphanedChunkPaths := make(map[int64][]chunkInfo)
	for dataID := range orphanedDataIDs {
		if chunks, ok := chunksByDataID[dataID]; ok {
			orphanedChunkPaths[dataID] = chunks
		}
		result.OrphanedChunks = append(result.OrphanedChunks, dataID)
	}

	// After scanning, check delayed chunks if delay is configured
	if len(orphanedChunkPaths) > 0 && delaySeconds > 0 {
		result.DelayedChunks = len(orphanedChunkPaths)
		fmt.Printf("Found %d orphaned dataIDs, waiting %d seconds before re-checking...\n", len(orphanedChunkPaths), delaySeconds)
		time.Sleep(time.Duration(delaySeconds) * time.Second)

		// Re-check metadata and references for delayed chunks
		recheckDataIDs := make([]int64, 0, len(orphanedChunkPaths))
		for checkDataID := range orphanedChunkPaths {
			recheckDataIDs = append(recheckDataIDs, checkDataID)
		}

		// Check which DataInfo exist
		existingDataIDs := make([]int64, 0)
		missingDataIDs := make([]int64, 0)
		for _, checkDataID := range recheckDataIDs {
			_, err := ma.GetData(c, bktID, checkDataID)
			if err != nil {
				// DataInfo still doesn't exist, mark for deletion
				missingDataIDs = append(missingDataIDs, checkDataID)
			} else {
				// DataInfo exists, need to check references
				existingDataIDs = append(existingDataIDs, checkDataID)
			}
		}

		// Check reference counts for existing DataInfo
		if len(existingDataIDs) > 0 {
			refCounts, err := ma.CountDataRefs(c, bktID, existingDataIDs)
			if err != nil {
				// If query fails, mark all as potentially orphaned (safer to re-check later)
				missingDataIDs = append(missingDataIDs, existingDataIDs...)
			} else {
				// Mark dataIDs with refCount == 0 as orphaned
				for _, dataID := range existingDataIDs {
					if refCounts[dataID] == 0 {
						missingDataIDs = append(missingDataIDs, dataID)
					}
				}
			}
		}

		// Delete chunks for dataIDs that are still orphaned
		for _, checkDataID := range missingDataIDs {
			chunks, ok := orphanedChunkPaths[checkDataID]
			if !ok {
				continue
			}
			// Still orphaned, delete all chunks for this dataID
			result.StillOrphaned++
			for _, chunk := range chunks {
				result.FreedSize += chunk.size
				if err := os.Remove(chunk.path); err != nil {
					result.Errors = append(result.Errors, fmt.Sprintf("failed to delete chunk %s: %v", chunk.path, err))
				} else {
					result.DeletedChunks++
				}
			}
		}
	} else if len(orphanedChunkPaths) > 0 {
		// No delay configured, delete immediately
		for _, chunks := range orphanedChunkPaths {
			for _, chunk := range chunks {
				result.FreedSize += chunk.size
				if err := os.Remove(chunk.path); err != nil {
					result.Errors = append(result.Errors, fmt.Sprintf("failed to delete chunk %s: %v", chunk.path, err))
				} else {
					result.DeletedChunks++
				}
			}
		}
	}

	// Scan temporary write area for orphaned files
	scanTempWriteArea(dataPath, delaySeconds, result)

	return result, nil
}

// scanTempWriteArea scans the temporary write area for orphaned files
func scanTempWriteArea(dataPath string, delaySeconds int, result *ScanOrphanedChunksResult) {
	tempDir := filepath.Join(dataPath, ".temp_write")
	
	// Check if temp directory exists
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		return // No temp directory, nothing to scan
	}

	// Read all files in temp directory
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("error reading temp directory %s: %v", tempDir, err))
		return
	}

	now := time.Now()
	retentionPeriod := time.Duration(delaySeconds) * time.Second
	if retentionPeriod < 24*time.Hour {
		retentionPeriod = 24 * time.Hour // Minimum 24 hours for temp files
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse filename: fileID_dataID
		fileName := entry.Name()
		var fileID, dataID int64
		if _, err := fmt.Sscanf(fileName, "%d_%d", &fileID, &dataID); err != nil {
			// Invalid format, skip
			continue
		}

		// Get file info
		filePath := filepath.Join(tempDir, fileName)
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		// Check if file is old enough to be considered orphaned
		if now.Sub(fileInfo.ModTime()) > retentionPeriod {
			// File is orphaned, delete it
			if err := os.Remove(filePath); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("error removing orphaned temp file %s: %v", fileName, err))
			} else {
				result.DeletedChunks++
				result.OrphanedChunks = append(result.OrphanedChunks, dataID)
			}
		}
	}
}
