package vfs

import (
	"io"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

// calculateChecksums calculates HdrCRC32, CRC32, and MD5 checksums from data
// Delegates to sdk.CalculateChecksums for consistency and code reuse
func calculateChecksums(data []byte) (uint32, uint32, int64, error) {
	return sdk.CalculateChecksums(data)
}

// calculateChecksumsFromReader calculates checksums by reading from an io.Reader
// Delegates to sdk.CalculateChecksumsFromReader for consistency and code reuse
func calculateChecksumsFromReader(reader io.Reader, size int64) (uint32, uint32, int64, error) {
	return sdk.CalculateChecksumsFromReader(reader, size)
}

// tryInstantUpload attempts instant upload by calculating checksums and calling Ref
// Returns DataID if instant upload succeeds (> 0), 0 if it fails
func tryInstantUpload(fs *OrcasFS, data []byte, origSize int64, kind uint32) (int64, error) {
	// Calculate checksums
	hdrCRC32, crc32Val, md5Val, err := calculateChecksums(data)
	if err != nil {
		return 0, err
	}

	// Create DataInfo for Ref
	dataInfo := &core.DataInfo{
		OrigSize: origSize,
		HdrCRC32: hdrCRC32,
		CRC32:    crc32Val,
		MD5:      md5Val,
		Kind:     kind,
	}

	// Call Ref to check if data already exists
	refIDs, err := fs.h.Ref(fs.c, fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return 0, err
	}

	if len(refIDs) > 0 && refIDs[0] != 0 {
		if refIDs[0] > 0 {
			// Instant upload succeeded, return existing DataID from database
			return refIDs[0], nil
		} else {
			// Negative ID means reference to another element in current batch
			// This should not happen in VFS (single file write)
			// But we handle it for completeness: skip instant upload, return 0
			// The negative reference will be resolved in PutDataInfo
			return 0, nil
		}
	}

	// Instant upload failed, return 0
	return 0, nil
}
