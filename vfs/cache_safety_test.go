package vfs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orcastor/orcas/core"
)

func TestBudgetCacheLimitsAndReaderLifetime(t *testing.T) {
	c := newBudgetCache[int, []byte](8, 2, time.Minute, func(v []byte) int64 { return int64(cap(v)) })
	c.Put(1, []byte("abcd"))
	held, _ := c.Get(1)
	c.Put(2, []byte("efgh"))
	c.Get(1) // 2 is now the least recently used.
	c.Put(3, []byte("ijkl"))
	if _, ok := c.Get(2); ok {
		t.Fatal("byte budget did not evict the least recently used entry")
	}
	if c.Put(1, make([]byte, 1, 9)) {
		t.Fatal("capacity exceeding the byte budget was accepted")
	}
	if string(held) != "abcd" {
		t.Fatal("eviction changed bytes still held by a reader")
	}
	if used, entries := c.usage(); used != 4 || entries != 1 {
		t.Fatalf("replacement accounting: bytes=%d entries=%d", used, entries)
	}
	// Zero-sized values still obey the global entry bound.
	c.Put(4, nil)
	c.Put(5, nil)
	if used, entries := c.usage(); used != 0 || entries != 2 {
		t.Fatalf("entry limit: bytes=%d entries=%d", used, entries)
	}
	disabled := newBudgetCache[int, []byte](0, 2, 0, func(v []byte) int64 { return int64(len(v)) })
	if disabled.Put(1, nil) {
		t.Fatal("disabled cache accepted an entry")
	}
}

func TestBudgetCacheExpiryAndConcurrency(t *testing.T) {
	c := newBudgetCache[int, []byte](256, 16, time.Minute, func(v []byte) int64 { return int64(cap(v)) })
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := (worker + i) % 32
				c.Put(key, make([]byte, 16))
				c.Get(key)
				if i%7 == 0 {
					c.Del(key)
				}
			}
		}(worker)
	}
	wg.Wait()
	if used, count := c.usage(); used > 256 || count > 16 {
		t.Fatalf("concurrent budget exceeded: %d bytes, %d entries", used, count)
	}
	// Advance stored deadlines deterministically, without a flaky sleep.
	c.mu.Lock()
	for _, element := range c.items {
		element.Value.(*budgetEntry[int, []byte]).expires = time.Now().Add(-time.Second)
	}
	c.mu.Unlock()
	c.purgeExpired()
	if used, count := c.usage(); used != 0 || count != 0 {
		t.Fatalf("expired entries retained: %d bytes, %d entries", used, count)
	}
	c.Put(99, nil)
	c.mu.Lock()
	c.items[99].Value.(*budgetEntry[int, []byte]).expires = time.Now().Add(-time.Second)
	c.mu.Unlock()
	if _, ok := c.Get(99); ok {
		t.Fatal("expired entry returned by Get")
	}
}

func TestDirectorySnapshotOwnershipAndConcurrentLookup(t *testing.T) {
	const dirID, fileID = int64(920001), int64(920002)
	children := []*core.ObjectInfo{{ID: fileID, Name: "literal*?.txt", Type: core.OBJ_TYPE_FILE, Size: 1, Extra: "extra"}}
	dirListCache.Put(dirID, children)
	t.Cleanup(func() { dirListCache.Del(dirID); fileObjCache.Del(fileID) })
	children[0].Name = "changed"
	copyList, _ := dirListCache.Get(dirID)
	copyList.([]*core.ObjectInfo)[0].Size = 999
	n := &OrcasNode{fs: &OrcasFS{}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for step := 0; step < 200; step++ {
				fileObjCache.Put(fileID, &core.ObjectInfo{ID: fileID, DataID: 55, Size: int64(step), MTime: int64(step)})
				child, errno := n.getChildByName(dirID, "literal*?.txt")
				if errno != 0 || child.Name != "literal*?.txt" || child.DataID != 55 {
					t.Errorf("lookup: child=%+v errno=%v", child, errno)
					return
				}
				child.Size = -1 // Must never mutate the snapshot or cached object.
			}
		}(i)
	}
	wg.Wait()
	snapshot, _ := dirListCache.cache.Get(dirID)
	if snapshot.objects[0].Size != 1 || snapshot.objects[0].Extra != "extra" {
		t.Fatal("published snapshot was mutated")
	}
}

func TestDirectorySnapshotNamePriority(t *testing.T) {
	const dirID = int64(920005)
	objects := []*core.ObjectInfo{
		{ID: 1, Name: "same", Type: core.OBJ_TYPE_VERSION},
		{ID: 2, Name: "same", Type: core.OBJ_TYPE_DIR},
		{ID: 3, Name: "same", Type: core.OBJ_TYPE_FILE},
		{ID: 4, Name: "same", Type: core.OBJ_TYPE_FILE},
		{ID: 5, Name: "version-only", Type: core.OBJ_TYPE_VERSION},
	}
	dirListCache.Put(dirID, objects)
	t.Cleanup(func() { dirListCache.Del(dirID) })
	n := &OrcasNode{fs: &OrcasFS{}}
	child, errno := n.getChildByName(dirID, "same")
	if errno != 0 || child.ID != 3 {
		t.Fatalf("duplicate name preference: %+v %v", child, errno)
	}
	if _, errno := n.getChildByName(dirID, "version-only"); errno == 0 {
		t.Fatal("version exposed as a filesystem child")
	}
}

type directoryLoadHandler struct {
	core.Handler
	calls            atomic.Int32
	started, release chan struct{}
}

func (h *directoryLoadHandler) List(_ core.Ctx, _, _ int64, opt core.ListOptions) ([]*core.ObjectInfo, int64, string, error) {
	if !opt.SkipCount {
		return nil, 0, "", fmt.Errorf("unused totals were requested")
	}
	if h.calls.Add(1) == 1 {
		close(h.started)
		<-h.release
		return []*core.ObjectInfo{{Name: "stale"}}, 0, "", nil
	}
	return []*core.ObjectInfo{{Name: "fresh"}}, 0, "", nil
}

func TestDirectorySnapshotInvalidationDuringLoad(t *testing.T) {
	const dirID = int64(920010)
	dirListCache.Del(dirID)
	t.Cleanup(func() { dirListCache.Del(dirID) })
	h := &directoryLoadHandler{started: make(chan struct{}), release: make(chan struct{})}
	n := &OrcasNode{fs: &OrcasFS{h: h, c: context.Background()}}
	done := make(chan *directorySnapshot, 1)
	go func() {
		snapshot, errno := n.getDirectorySnapshot(dirID)
		if errno != 0 {
			t.Errorf("load: %v", errno)
		}
		done <- snapshot
	}()
	<-h.started
	dirListCache.Del(dirID)
	close(h.release)
	snapshot := <-done
	if snapshot == nil || len(snapshot.objects) != 1 || snapshot.objects[0].Name != "fresh" {
		t.Fatal("load resurrected an invalidated directory")
	}
}

func BenchmarkDirectoryIndexedLookup(b *testing.B) {
	for _, count := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			const dirID = int64(920100)
			children := make([]*core.ObjectInfo, count)
			for i := range children {
				children[i] = &core.ObjectInfo{ID: int64(i + 1), Name: fmt.Sprint(i), Type: core.OBJ_TYPE_FILE}
			}
			dirListCache.Put(dirID, children)
			defer dirListCache.Del(dirID)
			n := &OrcasNode{fs: &OrcasFS{}}
			name := fmt.Sprint(count - 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, errno := n.getChildByName(dirID, name); errno != 0 {
					b.Fatal(errno)
				}
			}
		})
	}
}
