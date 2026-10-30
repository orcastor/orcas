package vfs

import (
	"fmt"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/orcastor/orcas/core"
)

// directorySnapshot owns a flat object array and a name index. Neither is
// mutated after publication; mutable file sizes are overlaid on returned copies.
type directorySnapshot struct {
	objects []core.ObjectInfo
	byName  map[string]int
	bytes   int64
}

func newDirectorySnapshot(children []*core.ObjectInfo) *directorySnapshot {
	s := &directorySnapshot{objects: make([]core.ObjectInfo, 0, len(children)), byName: make(map[string]int, len(children))}
	for _, child := range children {
		if child == nil {
			continue
		}
		object := *child
		object.Name, object.Extra = strings.Clone(object.Name), strings.Clone(object.Extra)
		s.objects = append(s.objects, object)
		old, exists := s.byName[object.Name]
		if !exists || directoryObjectPriority(object.Type) > directoryObjectPriority(s.objects[old].Type) {
			s.byName[object.Name] = len(s.objects) - 1
		}
		s.bytes += int64(len(object.Name) + len(object.Extra))
	}
	// Include the object capacity and an allowance for map entries/buckets.
	s.bytes += int64(cap(s.objects))*int64(unsafe.Sizeof(core.ObjectInfo{})) + int64(len(s.byName))*64 + 128
	return s
}

func directoryObjectPriority(objectType int) int {
	switch objectType {
	case core.OBJ_TYPE_FILE:
		return 2
	case core.OBJ_TYPE_DIR:
		return 1
	default:
		return 0
	}
}

func (s *directorySnapshot) children() []*core.ObjectInfo {
	objects := append([]core.ObjectInfo(nil), s.objects...)
	children := make([]*core.ObjectInfo, len(objects))
	for i := range objects {
		children[i] = &objects[i]
	}
	return children
}

type directoryCache struct {
	cache *budgetCache[int64, *directorySnapshot]
	mu    sync.Mutex
	// Striped epochs prevent a DB load from resurrecting an invalidated view,
	// without an ever-growing map of per-directory mutexes or generations.
	epochs [64]uint64
}

func (c *directoryCache) version(key int64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epochs[uint64(key)%uint64(len(c.epochs))]
}

func (c *directoryCache) publish(key int64, snapshot *directorySnapshot, version uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epochs[uint64(key)%uint64(len(c.epochs))] != version {
		return false
	}
	c.cache.Put(key, snapshot)
	return true
}

func (c *directoryCache) Get(key int64) (interface{}, bool) {
	if snapshot, ok := c.cache.Get(key); ok {
		return snapshot.children(), true
	}
	return nil, false
}

func (c *directoryCache) Put(key int64, value interface{}) {
	children, ok := value.([]*core.ObjectInfo)
	if !ok {
		return
	}
	snapshot := newDirectorySnapshot(children)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epochs[uint64(key)%uint64(len(c.epochs))]++
	c.cache.Put(key, snapshot)
}

func (c *directoryCache) Del(key int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epochs[uint64(key)%uint64(len(c.epochs))]++
	c.cache.Del(key)
}

func (n *OrcasNode) getDirectorySnapshot(dirID int64) (*directorySnapshot, syscall.Errno) {
	if snapshot, ok := dirListCache.cache.Get(dirID); ok {
		return snapshot, 0
	}
	key := fmt.Sprintf("dir_%d", dirID)
	// Retry if a create/rename/delete raced with the database read.
	for attempts := 0; attempts < 3; attempts++ {
		result, err, _ := globalSingleFlight.Do(key, func() (interface{}, error) {
			if snapshot, ok := dirListCache.cache.Get(dirID); ok {
				return snapshot, nil
			}
			version := dirListCache.version(dirID)
			children, err := n.fs.listAllObjects(dirID, core.ListOptions{})
			if err != nil {
				return nil, err
			}
			snapshot := newDirectorySnapshot(children)
			if !dirListCache.publish(dirID, snapshot, version) {
				return nil, nil
			}
			return snapshot, nil
		})
		if err != nil {
			return nil, syscall.EIO
		}
		if snapshot, ok := result.(*directorySnapshot); ok && snapshot != nil {
			return snapshot, 0
		}
	}
	return nil, syscall.EAGAIN
}

func (n *OrcasNode) latestDirectoryObject(child *core.ObjectInfo) *core.ObjectInfo {
	object := *child
	var latest *core.ObjectInfo
	if cached, ok := fileObjCache.Get(child.ID); ok {
		latest, _ = cached.(*core.ObjectInfo)
	}
	if latest == nil || latest.ID != child.ID || latest.DataID <= 0 {
		latest = nil
		if ra := n.fs.getRandomAccessorByFileID(child.ID); ra != nil {
			latest, _ = ra.getFileObj()
		}
	}
	if latest != nil && latest.ID == child.ID && latest.DataID > 0 {
		object.DataID, object.Size, object.MTime = latest.DataID, latest.Size, latest.MTime
	}
	return &object
}

func (n *OrcasNode) getChildByName(dirID int64, name string) (*core.ObjectInfo, syscall.Errno) {
	snapshot, errno := n.getDirectorySnapshot(dirID)
	if errno != 0 {
		return nil, errno
	}
	index, ok := snapshot.byName[name]
	if !ok || directoryObjectPriority(snapshot.objects[index].Type) == 0 {
		return nil, syscall.ENOENT
	}
	return n.latestDirectoryObject(&snapshot.objects[index]), 0
}
