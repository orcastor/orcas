package vfs

import (
	"sync/atomic"
	"testing"
)

func drainChunkBufferPoolForTest() {
	for {
		if chunkBufferPool.Get() == nil {
			return
		}
	}
}

func TestReleaseChunkBufferReuseAndZeroing(t *testing.T) {
	drainChunkBufferPoolForTest()

	const chunkSize = int64(64 << 10)
	buf := allocChunkBuffer(chunkSize)
	if len(buf.data) != int(chunkSize) {
		t.Fatalf("unexpected buffer length: got %d, want %d", len(buf.data), chunkSize)
	}

	for i := range buf.data {
		buf.data[i] = 0x7F
	}
	buf.offsetInChunk = chunkSize
	buf.ranges = []writeRange{{start: 0, end: chunkSize}}

	if !releaseChunkBuffer(buf, chunkSize, true) {
		t.Fatalf("expected complete chunk buffer to be accepted for pooling")
	}
	if got := atomic.LoadInt32(&chunkBufferPool.count); got <= 0 {
		t.Fatalf("expected pool count to increase, got %d", got)
	}

	reused := allocChunkBuffer(chunkSize)
	if len(reused.data) != int(chunkSize) {
		t.Fatalf("unexpected reused buffer length: got %d, want %d", len(reused.data), chunkSize)
	}
	if reused.offsetInChunk != 0 {
		t.Fatalf("expected reused offset reset to 0, got %d", reused.offsetInChunk)
	}
	if len(reused.ranges) != 0 {
		t.Fatalf("expected reused ranges reset, got %d", len(reused.ranges))
	}
	for i := range reused.data {
		if reused.data[i] != 0 {
			t.Fatalf("expected reused buffer zeroed at index %d", i)
		}
	}
}

func TestReleaseChunkBufferRequireCompleteRejectsIncomplete(t *testing.T) {
	drainChunkBufferPoolForTest()

	const chunkSize = int64(64 << 10)
	buf := allocChunkBuffer(chunkSize)
	buf.offsetInChunk = chunkSize / 2
	buf.ranges = []writeRange{{start: 0, end: chunkSize / 2}}

	if releaseChunkBuffer(buf, chunkSize, true) {
		t.Fatalf("expected incomplete chunk buffer to be rejected")
	}
	if got := atomic.LoadInt32(&chunkBufferPool.count); got != 0 {
		t.Fatalf("expected pool to stay empty, got %d", got)
	}
	if buf.data != nil {
		t.Fatalf("expected rejected buffer data to be released")
	}
}
