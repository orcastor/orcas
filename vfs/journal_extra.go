package vfs

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// JournalExtraFormatVersion defines the format version for journal extra data
const (
	JournalExtraFormatBinary = 0 // Binary format (compact)
)

// JournalExtraData represents the data stored in journal/version Extra field
// Note: VersionType field removed - use object Type (OBJ_TYPE_VERSION vs OBJ_TYPE_JOURNAL) to distinguish
type JournalExtraData struct {
	JournalDataID int64 // Journal data ID (for journal snapshots)
	BaseVersionID int64 // Base version ID (for journal snapshots)
	EntryCount    int   // Entry count (for journal snapshots)
}

// EncodeJournalExtra encodes journal extra data to compact binary format
// Format: [version:1][journalDataID:8][baseVersionID:8][entryCount:4]
// Total: 21 bytes (vs ~80 bytes for JSON, reduced from 23 bytes by removing VersionType)
func EncodeJournalExtra(data *JournalExtraData) string {
	if data == nil {
		return ""
	}

	// Use binary format for new data
	buf := make([]byte, 21)
	buf[0] = JournalExtraFormatBinary // Format version
	binary.LittleEndian.PutUint64(buf[1:9], uint64(data.JournalDataID))
	binary.LittleEndian.PutUint64(buf[9:17], uint64(data.BaseVersionID))
	binary.LittleEndian.PutUint32(buf[17:21], uint32(data.EntryCount))

	// Return as base64-encoded string for SQLite TEXT compatibility
	// Base64 encoding ensures safe storage in TEXT field and is more compact than hex
	// Format: base64([version:1][journalDataID:8][baseVersionID:8][entryCount:4])
	// Total: 21 bytes binary -> ~28 bytes base64 (vs ~80 bytes for JSON)
	return base64.StdEncoding.EncodeToString(buf)
}

// DecodeJournalExtra decodes journal extra data from either JSON or binary format
// Supports backward compatibility with JSON format
func DecodeJournalExtra(extra string) (*JournalExtraData, error) {
	if extra == "" {
		return nil, fmt.Errorf("empty extra data")
	}

	// Check format by first byte: JSON format starts with '{' (0x7B)
	// Binary format is base64-encoded and never starts with '{'
	if len(extra) > 0 && extra[0] == '{' {
		// JSON format (backward compatibility)
		var jsonData map[string]interface{}
		if err := json.Unmarshal([]byte(extra), &jsonData); err != nil {
			return nil, fmt.Errorf("failed to parse JSON format: %w", err)
		}

		data := &JournalExtraData{}

		// Parse journalDataID (for journal snapshots)
		// Note: versionType field is ignored (removed) - use object Type to distinguish
		if jd, ok := jsonData["journalDataID"].(float64); ok {
			data.JournalDataID = int64(jd)
		}

		// Parse baseVersionID (for journal snapshots)
		if bv, ok := jsonData["baseVersionID"].(float64); ok {
			data.BaseVersionID = int64(bv)
		}

		// Parse entryCount (for journal snapshots)
		if ec, ok := jsonData["entryCount"].(float64); ok {
			data.EntryCount = int(ec)
		}

		return data, nil
	}

	// Binary format (base64-encoded)
	decoded, err := base64.StdEncoding.DecodeString(extra)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	if len(decoded) < 21 {
		return nil, fmt.Errorf("invalid binary format: too short (%d bytes, expected 21)", len(decoded))
	}

	if decoded[0] != JournalExtraFormatBinary {
		return nil, fmt.Errorf("invalid binary format: wrong format version (%d, expected %d)", decoded[0], JournalExtraFormatBinary)
	}

	data := &JournalExtraData{
		JournalDataID: int64(binary.LittleEndian.Uint64(decoded[1:9])),
		BaseVersionID: int64(binary.LittleEndian.Uint64(decoded[9:17])),
		EntryCount:    int(binary.LittleEndian.Uint32(decoded[17:21])),
	}

	return data, nil
}

// GetJournalDataID extracts journalDataID from Extra field (supports both formats)
func GetJournalDataID(extra string) (int64, error) {
	data, err := DecodeJournalExtra(extra)
	if err != nil {
		return 0, err
	}
	return data.JournalDataID, nil
}

// ParseBaseVersionID parses baseVersionID from Extra field (backward compatible)
// This is a convenience function that works with both JSON and binary formats
func ParseBaseVersionID(extra string) int64 {
	if extra == "" {
		return 0
	}

	data, err := DecodeJournalExtra(extra)
	if err != nil {
		return 0
	}

	return data.BaseVersionID
}
