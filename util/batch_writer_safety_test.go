package util

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orcastor/orcas/core"
)

type batchSafetyHandler struct {
	core.Handler
	mu       sync.Mutex
	packages map[int64][]byte
	files    map[int64][]byte
	fail     atomic.Bool
	flushed  chan struct{}
}

func (h *batchSafetyHandler) PutData(ctx core.Ctx, _, id int64, _ int, data []byte) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.packages[id] = bytes.Clone(data)
	return id, nil
}

func (h *batchSafetyHandler) PutDataInfoAndObj(_ core.Ctx, _ int64, data []*core.DataInfo, objects []*core.ObjectInfo) error {
	if h.fail.Load() {
		return errors.New("injected metadata failure")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, info := range data {
		pkg := h.packages[info.PkgID]
		start := int64(info.PkgOffset)
		if start < 0 || start+info.Size > int64(len(pkg)) {
			return fmt.Errorf("invalid package extent: %+v", info)
		}
		h.files[objects[i].ID] = bytes.Clone(pkg[start : start+info.Size])
	}
	select {
	case h.flushed <- struct{}{}:
	default:
	}
	return nil
}

func newBatchSafetyWriter(t *testing.T) (*BatchWriter, *batchSafetyHandler) {
	t.Helper()
	t.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	t.Setenv("ORCAS_MAX_WRITE_BUFFER_SIZE", "16384")
	t.Setenv("ORCAS_MAX_WRITE_BUFFER_COUNT", "64")
	h := &batchSafetyHandler{packages: make(map[int64][]byte), files: make(map[int64][]byte), flushed: make(chan struct{}, 1)}
	w := createBatchWriter(h, 1)
	t.Cleanup(w.cancelFlushTimer)
	return w, h
}

func TestBatchWriterSafetyConcurrentDataIntegrity(t *testing.T) {
	w, h := newBatchSafetyWriter(t)
	const workers, count = 16, 128
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				id := int64(worker*count + i + 1)
				payload := bytes.Repeat([]byte{byte(worker + 1)}, 64+i%19)
				for {
					ok, _, err := w.AddFile(id, payload, 1, "file", int64(len(payload)), 0)
					if err != nil {
						t.Error(err)
						return
					}
					if ok {
						break
					}
					if err := w.FlushAll(context.Background()); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	if err := w.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.files) != workers*count {
		t.Fatalf("persisted %d files, want %d", len(h.files), workers*count)
	}
	for worker := 0; worker < workers; worker++ {
		for i := 0; i < count; i++ {
			id := int64(worker*count + i + 1)
			want := bytes.Repeat([]byte{byte(worker + 1)}, 64+i%19)
			if !bytes.Equal(h.files[id], want) {
				t.Fatalf("file %d contains another write's bytes", id)
			}
		}
	}
}

func TestBatchWriterSafetyFailedFlushRetainsBothBuffers(t *testing.T) {
	w, h := newBatchSafetyWriter(t)
	w.AddFile(1, []byte("old-buffer"), 1, "old", 10, 0)
	h.fail.Store(true)
	if err := w.FlushAll(context.Background()); err == nil {
		t.Fatal("flush swallowed metadata error")
	}
	w.AddFile(2, []byte("new-buffer"), 1, "new", 10, 0)
	for id, want := range map[int64]string{1: "old-buffer", 2: "new-buffer"} {
		if got := string(w.ReadPendingData(id)); got != want {
			t.Fatalf("file %d: got %q, want %q", id, got, want)
		}
	}
	snapshot := w.ReadPendingData(1)
	h.fail.Store(false)
	if err := w.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(w.GetPendingObjects()) != 0 || string(snapshot) != "old-buffer" {
		t.Fatal("flush did not drain both buffers or invalidated a retained read")
	}
	if w.currentBuffer != nil || w.bgBuffer != nil {
		t.Fatal("idle writer retains backing arrays")
	}
}

func TestBatchWriterSafetyPeriodicFlushSurvivesRequest(t *testing.T) {
	w, h := newBatchSafetyWriter(t)
	w.flushWindow = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	w.SetFlushContext(ctx)
	cancel()
	if ok, _, err := w.AddFile(1, []byte("timed"), 1, "file", 5, 0); !ok || err != nil {
		t.Fatal(ok, err)
	}
	select {
	case <-h.flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic flush did not persist data after request cancellation")
	}
	// Wait for timer cleanup and exercise a second scheduling cycle.
	if err := w.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.AddFile(2, []byte("again"), 1, "file2", 5, 0)
	select {
	case <-h.flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic flush did not restart")
	}
	w.FlushAll(context.Background())
}

func TestBatchWriterSafetyRejectedWriteAndDeletedHole(t *testing.T) {
	w, h := newBatchSafetyWriter(t)
	w.maxFileInfos = 2
	w.AddFile(1, []byte("deleted"), 1, "old", 7, 0)
	w.AddFile(2, []byte("retained"), 1, "new", 8, 0)
	if ok, _, err := w.AddFile(3, []byte("reject"), 1, "third", 6, 0); ok || err != nil {
		t.Fatal("full metadata buffer accepted a write", ok, err)
	}
	if !w.RemovePendingObject(1) {
		t.Fatal("could not remove unflushed file")
	}
	if err := w.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if string(h.files[2]) != "retained" || len(h.files) != 1 {
		t.Fatal("deleted hole changed remaining package offsets")
	}
}
