package vfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/orcastor/orcas/core"
)

func newIndexedTestJournal() *Journal {
	config := DefaultJournalConfig()
	config.EnableMemoryLimit = false
	config.MaxEntriesSmall = 1 << 20
	config.SnapshotEntryCount = 1 << 20
	config.SnapshotMemorySize = 1 << 30
	return &Journal{fs: &OrcasFS{journalMgr: &JournalManager{config: config}}}
}

func zeroJournalBase(_, length int64) ([]byte, error) { return make([]byte, length), nil }

func TestJournalIndexLastWriteWins(t *testing.T) {
	j := newIndexedTestJournal()
	j.Write(10, bytes.Repeat([]byte("A"), 10))
	j.Write(0, bytes.Repeat([]byte("B"), 20))
	before, err := j.Read(0, 20, zeroJournalBase)
	if err != nil {
		t.Fatal(err)
	}
	j.MergeEntries()
	after, err := j.Read(0, 20, zeroJournalBase)
	if err != nil || !bytes.Equal(before, after) || string(after) != "BBBBBBBBBBBBBBBBBBBB" {
		t.Fatalf("merge changed newest write: before=%q after=%q err=%v", before, after, err)
	}
}

func TestJournalIndexRandomOverlapsRoundTrip(t *testing.T) {
	j := newIndexedTestJournal()
	want := make([]byte, 8192)
	rng := rand.New(rand.NewSource(42))
	for step := 0; step < 400; step++ {
		offset := rng.Intn(len(want) - 512)
		payload := bytes.Repeat([]byte{byte(step%255 + 1)}, 1+rng.Intn(512))
		if err := j.Write(int64(offset), payload); err != nil {
			t.Fatal(err)
		}
		copy(want[offset:], payload)
		if step%17 == 0 {
			j.MergeEntries()
		}
		got, err := j.Read(0, j.GetSize(), zeroJournalBase)
		if err != nil || !bytes.Equal(got, want[:j.GetSize()]) {
			t.Fatalf("step %d: visible bytes differ: %v", step, err)
		}
	}
	encoded, err := j.SerializeJournal()
	if err != nil {
		t.Fatal(err)
	}
	recovered := newIndexedTestJournal()
	if err := recovered.DeserializeJournal(encoded); err != nil {
		t.Fatal(err)
	}
	got, err := recovered.Read(0, recovered.GetSize(), zeroJournalBase)
	if err != nil || !bytes.Equal(got, want[:recovered.GetSize()]) {
		t.Fatalf("recovery changed visible bytes: %v", err)
	}
	if err := recovered.Truncate(1024); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Write(2048, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	got, err = recovered.Read(1024, 1028, zeroJournalBase)
	if err != nil || !bytes.Equal(got[:1024], make([]byte, 1024)) || string(got[1024:]) != "tail" {
		t.Fatalf("truncated intervals reappeared on extension: %v", err)
	}
}

func TestJournalIndexLegacyLogAndChunkOverlay(t *testing.T) {
	j := &Journal{entries: []JournalEntry{
		{Offset: 10, Length: 10, Data: bytes.Repeat([]byte("A"), 10)},
		{Offset: 0, Length: 20, Data: bytes.Repeat([]byte("B"), 20)},
	}, currentSize: 20, dataID: core.EmptyDataID}
	got, err := j.Read(0, 20, zeroJournalBase)
	if err != nil || string(got) != "BBBBBBBBBBBBBBBBBBBB" {
		t.Fatal(string(got), err)
	}
	j.entriesMu.Lock()
	chunk, err := j.generateChunkData(8, 8)
	j.entriesMu.Unlock()
	if err != nil || string(chunk) != "BBBBBBBB" {
		t.Fatal(string(chunk), err)
	}
}

func TestJournalIndexTruncateAndRecoveryAccounting(t *testing.T) {
	j := newIndexedTestJournal()
	if err := j.Write(0, bytes.Repeat([]byte("x"), 100)); err != nil {
		t.Fatal(err)
	}
	if err := j.Write(200, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := j.Truncate(50); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&j.memoryUsage); got != 66 || atomic.LoadInt64(&j.fs.journalMgr.totalMemory) != got {
		t.Fatalf("truncate memory accounting = %d", got)
	}
	encoded, err := j.SerializeJournal()
	if err != nil {
		t.Fatal(err)
	}
	if err := j.DeserializeJournal(encoded); err != nil {
		t.Fatal(err)
	}
	if err := j.MergeEntries(); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&j.memoryUsage); got != 66 || j.GetStats()["modifiedBytes"] != int64(50) {
		t.Fatalf("recovery memory/interval accounting = %d", got)
	}
	// Incomplete and invalid inputs must preserve the existing readable log.
	badCount := bytes.Clone(encoded)
	binary.LittleEndian.PutUint64(badCount, ^uint64(0))
	badLength := bytes.Clone(encoded)
	binary.LittleEndian.PutUint64(badLength[24:], ^uint64(0))
	for _, data := range [][]byte{encoded[:20], badCount, badLength} {
		if err := j.DeserializeJournal(data); err == nil {
			t.Fatal("accepted malformed journal")
		}
		got, err := j.Read(0, 50, zeroJournalBase)
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("x"), 50)) {
			t.Fatal("failed recovery changed active log", err)
		}
	}
	// Legacy sparse holes have no data buffer to slice during truncation.
	j.entriesMu.Lock()
	j.entries = append(j.entries, JournalEntry{Offset: 25, Length: 25})
	j.entriesMu.Unlock()
	if err := j.Truncate(30); err != nil {
		t.Fatal(err)
	}
	got, err := j.Read(20, 10, zeroJournalBase)
	if err != nil || !bytes.Equal(got, append([]byte("xxxxx"), make([]byte, 5)...)) {
		t.Fatalf("sparse truncate: %q %v", got, err)
	}
	if err := j.Truncate(-1); err == nil || j.GetSize() != 30 {
		t.Fatal("negative truncate mutated journal")
	}
}

func BenchmarkJournalIndexedRead(b *testing.B) {
	for _, count := range []int{10, 1000, 5000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			j := &Journal{currentSize: int64(count * 8192), entries: make([]JournalEntry, count)}
			data := bytes.Repeat([]byte("A"), 4096)
			for i := range j.entries {
				j.entries[i] = JournalEntry{Offset: int64(i * 8192), Length: 4096, Data: data}
			}
			j.indexEntriesLocked()
			offset := int64((count / 2) * 8192)
			baseReader := func(_, length int64) ([]byte, error) { return make([]byte, length), nil }
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := j.Read(offset, 4096, baseReader); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
