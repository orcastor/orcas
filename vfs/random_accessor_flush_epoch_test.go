package vfs

import (
	"sync/atomic"
	"testing"

	"github.com/orcastor/orcas/core"
)

func TestChunkedFileWriterFlushSkipsWhenAlreadySyncedAndNoPendingChunks(t *testing.T) {
	cw := &ChunkedFileWriter{
		fileID: 1,
		dataID: 2,
		size:   128,
		chunks: map[int]*chunkBuffer{},
		dataInfo: &core.DataInfo{
			ID:       2,
			OrigSize: 128,
			Size:     128,
			Kind:     core.DATA_NORMAL,
		},
	}
	atomic.StoreUint64(&cw.writeEpoch, 7)
	atomic.StoreUint64(&cw.syncedEpoch, 7)

	if err := cw.Flush(true); err != nil {
		t.Fatalf("expected flush to be skipped without error, got %v", err)
	}
	if got := atomic.LoadInt32(&cw.flushing); got != 0 {
		t.Fatalf("expected flushing flag reset to 0, got %d", got)
	}
}
