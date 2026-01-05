package vfs

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// JournalExtraFormatVersion defines the format version for journal extra data
const (
	JournalExtraFormatJSON   = 0 // JSON format (legacy, for backward compatibility)
	JournalExtraFormatBinary = 1 // Binary format (compact)
)

// JournalExtraData represents the data stored in journal/version Extra field
type JournalExtraData struct {
	VersionType   int   // 1 = full version, 2 = journal snapshot
	JournalDataID int64 // Journal data ID (for journal snapshots)
	BaseVersionID int64 // Base version ID (for journal snapshots)
	EntryCount    int   // Entry count (for journal snapshots)
	Merged        bool  // Whether this version was merged (for full versions)
}

// EncodeJournalExtra encodes journal extra data to compact binary format
// Format: [version:1][versionType:1][journalDataID:8][baseVersionID:8][entryCount:4][merged:1]
// Total: 23 bytes (vs ~80 bytes for JSON)
func EncodeJournalExtra(data *JournalExtraData) string {
	if data == nil {
		return ""
	}

	// Use binary format for new data
	buf := make([]byte, 23)
	buf[0] = JournalExtraFormatBinary // Format version
	buf[1] = byte(data.VersionType)
	binary.LittleEndian.PutUint64(buf[2:10], uint64(data.JournalDataID))
	binary.LittleEndian.PutUint64(buf[10:18], uint64(data.BaseVersionID))
	binary.LittleEndian.PutUint32(buf[18:22], uint32(data.EntryCount))
	if data.Merged {
		buf[22] = 1
	} else {
		buf[22] = 0
	}

	// Return as base64-encoded string for SQLite TEXT compatibility
	// Base64 encoding ensures safe storage in TEXT field and is more compact than hex
	// Format: base64([version:1][versionType:1][journalDataID:8][baseVersionID:8][entryCount:4][merged:1])
	// Total: 23 bytes binary -> ~32 bytes base64 (vs ~80 bytes for JSON)
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

		// Parse versionType
		if vt, ok := jsonData["versionType"].(float64); ok {
			data.VersionType = int(vt)
		}

		// Parse journalDataID (for journal snapshots)
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

		// Parse merged (for full versions)
		if m, ok := jsonData["merged"].(bool); ok {
			data.Merged = m
		}

		return data, nil
	}

	// Binary format (base64-encoded)
	decoded, err := base64.StdEncoding.DecodeString(extra)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	if len(decoded) < 23 {
		return nil, fmt.Errorf("invalid binary format: too short (%d bytes, expected 23)", len(decoded))
	}

	if decoded[0] != JournalExtraFormatBinary {
		return nil, fmt.Errorf("invalid binary format: wrong format version (%d, expected %d)", decoded[0], JournalExtraFormatBinary)
	}

	data := &JournalExtraData{
		VersionType:   int(decoded[1]),
		JournalDataID: int64(binary.LittleEndian.Uint64(decoded[2:10])),
		BaseVersionID: int64(binary.LittleEndian.Uint64(decoded[10:18])),
		EntryCount:    int(binary.LittleEndian.Uint32(decoded[18:22])),
		Merged:        decoded[22] != 0,
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
