package vfs

import (
	"testing"

	"github.com/orcastor/orcas/core"
)

func TestTmpRenameDetectionFlushFailureKeepsWriterState(t *testing.T) {
	ra := &RandomAccessor{
		fileID:    42,
		isTmpFile: true,
	}
	ra.fileObj.Store(&core.ObjectInfo{
		ID:   42,
		Type: core.OBJ_TYPE_FILE,
		Name: "final.docx",
	})

	// Invalid writer (fileID/dataID=0) makes Flush fail immediately.
	invalidWriter := &ChunkedFileWriter{}
	ra.chunkedWriter.Store(invalidWriter)

	err := ra.Write(0, []byte("x"))
	if err == nil {
		t.Fatalf("expected write to fail when tmp-writer flush fails after rename detection")
	}
	if !ra.isTmpFile {
		t.Fatalf("isTmpFile should remain true when flush fails")
	}

	got := ra.chunkedWriter.Load()
	if got != invalidWriter {
		t.Fatalf("chunkedWriter should stay untouched on flush failure")
	}
}
