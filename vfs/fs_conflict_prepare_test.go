package vfs

import (
	"sync/atomic"
	"testing"

	"github.com/orcastor/orcas/core"
)

func TestPrepareFileForConflictDeleteClearsCacheWithoutFS(t *testing.T) {
	n := &OrcasNode{}
	const fileID = int64(9001)
	fileObjCache.Put(fileID, dummyObjForCache)

	if err := n.prepareFileForConflictDelete(fileID, "unit-no-fs"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := fileObjCache.Get(fileID); ok {
		t.Fatalf("expected cache to be cleared")
	}
}

func TestPrepareFileForConflictDeleteFailsOnPendingInvalidRA(t *testing.T) {
	fs := &OrcasFS{}
	n := &OrcasNode{fs: fs}
	const fileID = int64(9002)

	ra := &RandomAccessor{
		fileID:    fileID,
		buffer:    &WriteBuffer{},
		isTmpFile: true,
	}
	atomic.StoreInt64(&ra.buffer.writeIndex, 1) // mark pending so flush is attempted
	fs.raRegistry.Store(fileID, ra)
	defer fs.raRegistry.Delete(fileID)

	if err := n.prepareFileForConflictDelete(fileID, "unit-pending-invalid"); err == nil {
		t.Fatalf("expected flush error for pending invalid accessor")
	}
}

func TestPrepareFileForConflictDeleteSkipsCleanRAAndUnregisters(t *testing.T) {
	fs := &OrcasFS{}
	n := &OrcasNode{fs: fs}
	const fileID = int64(9003)

	ra := &RandomAccessor{fileID: fileID}
	fs.raRegistry.Store(fileID, ra)
	fileObjCache.Put(fileID, dummyObjForCache)

	if err := n.prepareFileForConflictDelete(fileID, "unit-clean"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := fs.raRegistry.Load(fileID); ok {
		t.Fatalf("expected accessor to be unregistered")
	}
	if _, ok := fileObjCache.Get(fileID); ok {
		t.Fatalf("expected cache to be cleared")
	}
}

var dummyObjForCache = &core.ObjectInfo{ID: 1}
