package vfs

import (
	"fmt"
	"sort"
)

// journalSpan references the newest write covering [start,end). Spans are
// sorted and disjoint; the byte buffers stay owned by the chronological log.
type journalSpan struct {
	start, end int64
	entry      int
}

// indexEntriesLocked incrementally overlays new writes. Keeping data separate
// from this compact index avoids copying file bytes when an interval splits.
func (j *Journal) indexEntriesLocked() {
	if !j.indexValid || j.indexEntryCount > len(j.entries) {
		j.entryIndex = nil
		j.indexEntryCount = 0
		j.indexValid = true
	}
	for j.indexEntryCount < len(j.entries) {
		i := j.indexEntryCount
		entry := &j.entries[i]
		j.indexEntryCount++
		if entry.Length <= 0 {
			continue
		}
		start, end := entry.Offset, entry.Offset+entry.Length
		lo := sort.Search(len(j.entryIndex), func(i int) bool { return j.entryIndex[i].end > start })
		hi := lo + sort.Search(len(j.entryIndex)-lo, func(i int) bool { return j.entryIndex[lo+i].start >= end })
		var replacement [3]journalSpan
		n := 0
		if lo < hi && j.entryIndex[lo].start < start {
			replacement[n] = j.entryIndex[lo]
			replacement[n].end = start
			n++
		}
		replacement[n] = journalSpan{start, end, i}
		n++
		if lo < hi && j.entryIndex[hi-1].end > end {
			replacement[n] = j.entryIndex[hi-1]
			replacement[n].start = end
			n++
		}
		oldLen := len(j.entryIndex)
		newLen := oldLen - (hi - lo) + n
		if newLen > oldLen {
			j.entryIndex = append(j.entryIndex, make([]journalSpan, newLen-oldLen)...)
		}
		copy(j.entryIndex[lo+n:], j.entryIndex[hi:oldLen])
		copy(j.entryIndex[lo:lo+n], replacement[:n])
		j.entryIndex = j.entryIndex[:newLen]
	}
}

// lockIndexedEntries returns with entriesMu held for reading. Recovered legacy
// logs are indexed on first access; ordinary writes maintain the index eagerly.
func (j *Journal) lockIndexedEntries() {
	for {
		j.entriesMu.RLock()
		if j.indexValid && j.indexEntryCount == len(j.entries) {
			return
		}
		j.entriesMu.RUnlock()
		j.entriesMu.Lock()
		j.indexEntriesLocked()
		j.entriesMu.Unlock()
	}
}

func (j *Journal) overlappingSpans(offset, end int64) []journalSpan {
	lo := sort.Search(len(j.entryIndex), func(i int) bool { return j.entryIndex[i].end > offset })
	hi := lo + sort.Search(len(j.entryIndex)-lo, func(i int) bool { return j.entryIndex[lo+i].start >= end })
	return j.entryIndex[lo:hi]
}

func (j *Journal) overlaySpans(dst []byte, offset int64, spans []journalSpan) error {
	end := offset + int64(len(dst))
	for _, span := range spans {
		start, stop := journalMax(offset, span.start), journalMin(end, span.end)
		entry := &j.entries[span.entry]
		if entry.Data == nil { // Sparse hole.
			clear(dst[start-offset : stop-offset])
			continue
		}
		if start < entry.Offset || stop-entry.Offset > int64(len(entry.Data)) {
			return fmt.Errorf("journal entry has incomplete data at offset %d", entry.Offset)
		}
		copy(dst[start-offset:stop-offset], entry.Data[start-entry.Offset:stop-entry.Offset])
	}
	return nil
}

func journalMin(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func journalMax(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
