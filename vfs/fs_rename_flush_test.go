package vfs

import (
	"sync/atomic"
	"testing"
)

func TestForceFlushAccessorForRenameSkipsCleanAccessor(t *testing.T) {
	n := &OrcasNode{}
	ra := &RandomAccessor{fileID: 1}

	if err := n.forceFlushAccessorForRename(ra, 1, "unit-clean"); err != nil {
		t.Fatalf("expected clean accessor to skip flush without error, got: %v", err)
	}
}

func TestForceFlushAccessorForRenameFailsOnPendingInvalidAccessor(t *testing.T) {
	n := &OrcasNode{}
	ra := &RandomAccessor{
		fileID:    2,
		buffer:    &WriteBuffer{},
		isTmpFile: true,
	}
	atomic.StoreInt64(&ra.buffer.writeIndex, 1) // mark as pending

	err := n.forceFlushAccessorForRename(ra, 2, "unit-pending-invalid")
	if err == nil {
		t.Fatalf("expected flush helper to fail for pending invalid accessor")
	}
}
