package vfs

import (
	"testing"
)

func TestEncodeDecodeJournalExtra(t *testing.T) {
	// Test journal snapshot format
	journalData := &JournalExtraData{
		VersionType:   2,
		JournalDataID: 123456789,
		BaseVersionID: 987654321,
		EntryCount:    42,
		Merged:        false,
	}

	encoded := EncodeJournalExtra(journalData)
	if encoded == "" {
		t.Fatal("Encoded data is empty")
	}

	decoded, err := DecodeJournalExtra(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.VersionType != journalData.VersionType {
		t.Errorf("VersionType mismatch: got %d, want %d", decoded.VersionType, journalData.VersionType)
	}
	if decoded.JournalDataID != journalData.JournalDataID {
		t.Errorf("JournalDataID mismatch: got %d, want %d", decoded.JournalDataID, journalData.JournalDataID)
	}
	if decoded.BaseVersionID != journalData.BaseVersionID {
		t.Errorf("BaseVersionID mismatch: got %d, want %d", decoded.BaseVersionID, journalData.BaseVersionID)
	}
	if decoded.EntryCount != journalData.EntryCount {
		t.Errorf("EntryCount mismatch: got %d, want %d", decoded.EntryCount, journalData.EntryCount)
	}
	if decoded.Merged != journalData.Merged {
		t.Errorf("Merged mismatch: got %v, want %v", decoded.Merged, journalData.Merged)
	}

	t.Logf("✓ Binary format encoding/decoding works correctly")
	t.Logf("  Encoded size: %d bytes (vs ~80 bytes for JSON)", len(encoded))
}

func TestDecodeJournalExtraJSON(t *testing.T) {
	// Test backward compatibility with JSON format
	jsonExtra := `{"versionType":2,"journalDataID":123456789,"baseVersionID":987654321,"entryCount":42}`

	decoded, err := DecodeJournalExtra(jsonExtra)
	if err != nil {
		t.Fatalf("Failed to decode JSON: %v", err)
	}

	if decoded.VersionType != 2 {
		t.Errorf("VersionType mismatch: got %d, want 2", decoded.VersionType)
	}
	if decoded.JournalDataID != 123456789 {
		t.Errorf("JournalDataID mismatch: got %d, want 123456789", decoded.JournalDataID)
	}
	if decoded.BaseVersionID != 987654321 {
		t.Errorf("BaseVersionID mismatch: got %d, want 987654321", decoded.BaseVersionID)
	}
	if decoded.EntryCount != 42 {
		t.Errorf("EntryCount mismatch: got %d, want 42", decoded.EntryCount)
	}

	t.Logf("✓ JSON format backward compatibility works correctly")
}

func TestDecodeJournalExtraFullVersion(t *testing.T) {
	// Test full version format
	fullVersionData := &JournalExtraData{
		VersionType:   1,
		JournalDataID: 0,
		BaseVersionID: 0,
		EntryCount:    0,
		Merged:        false,
	}

	encoded := EncodeJournalExtra(fullVersionData)
	decoded, err := DecodeJournalExtra(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.VersionType != 1 {
		t.Errorf("VersionType mismatch: got %d, want 1", decoded.VersionType)
	}
	if decoded.Merged {
		t.Errorf("Merged should be false")
	}

	t.Logf("✓ Full version format works correctly")
}

func TestDecodeJournalExtraMerged(t *testing.T) {
	// Test merged version format
	mergedData := &JournalExtraData{
		VersionType:   1,
		JournalDataID: 0,
		BaseVersionID: 0,
		EntryCount:    0,
		Merged:        true,
	}

	encoded := EncodeJournalExtra(mergedData)
	decoded, err := DecodeJournalExtra(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if !decoded.Merged {
		t.Errorf("Merged should be true")
	}

	t.Logf("✓ Merged version format works correctly")
}

func TestParseBaseVersionID(t *testing.T) {
	// Test ParseBaseVersionID with binary format
	journalData := &JournalExtraData{
		VersionType:   2,
		JournalDataID: 123456789,
		BaseVersionID: 987654321,
		EntryCount:    42,
		Merged:        false,
	}

	encoded := EncodeJournalExtra(journalData)
	baseVersionID := ParseBaseVersionID(encoded)
	if baseVersionID != 987654321 {
		t.Errorf("BaseVersionID mismatch: got %d, want 987654321", baseVersionID)
	}

	// Test with JSON format (backward compatibility)
	jsonExtra := `{"versionType":2,"journalDataID":123456789,"baseVersionID":987654321,"entryCount":42}`
	baseVersionID = ParseBaseVersionID(jsonExtra)
	if baseVersionID != 987654321 {
		t.Errorf("BaseVersionID mismatch (JSON): got %d, want 987654321", baseVersionID)
	}

	t.Logf("✓ ParseBaseVersionID works with both formats")
}

func TestGetJournalDataID(t *testing.T) {
	// Test GetJournalDataID with binary format
	journalData := &JournalExtraData{
		VersionType:   2,
		JournalDataID: 123456789,
		BaseVersionID: 987654321,
		EntryCount:    42,
		Merged:        false,
	}

	encoded := EncodeJournalExtra(journalData)
	journalDataID, err := GetJournalDataID(encoded)
	if err != nil {
		t.Fatalf("Failed to get journalDataID: %v", err)
	}
	if journalDataID != 123456789 {
		t.Errorf("JournalDataID mismatch: got %d, want 123456789", journalDataID)
	}

	// Test with JSON format (backward compatibility)
	jsonExtra := `{"versionType":2,"journalDataID":123456789,"baseVersionID":987654321,"entryCount":42}`
	journalDataID, err = GetJournalDataID(jsonExtra)
	if err != nil {
		t.Fatalf("Failed to get journalDataID (JSON): %v", err)
	}
	if journalDataID != 123456789 {
		t.Errorf("JournalDataID mismatch (JSON): got %d, want 123456789", journalDataID)
	}

	t.Logf("✓ GetJournalDataID works with both formats")
}

