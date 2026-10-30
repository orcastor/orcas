package vfs

import (
	"container/list"
	"os"
	"strconv"
	"sync"
	"time"
)

// budgetCache bounds retained values by both bytes and entry count. Values
// handed to readers are immutable: eviction never recycles their backing data.
// Tracked bytes exclude allocator metadata and are not a process RSS limit.
type budgetCache[K comparable, V any] struct {
	mu         sync.Mutex
	items      map[K]*list.Element
	order      list.List
	limit      int64
	used       int64
	maxEntries int
	ttl        time.Duration
	weight     func(V) int64
}

type budgetEntry[K comparable, V any] struct {
	key     K
	value   V
	bytes   int64
	expires time.Time
}

func newBudgetCache[K comparable, V any](limit int64, maxEntries int, ttl time.Duration, weight func(V) int64) *budgetCache[K, V] {
	return &budgetCache[K, V]{items: make(map[K]*list.Element), limit: limit, maxEntries: maxEntries, ttl: ttl, weight: weight}
}

func cacheBudgetFromEnv(name string, fallback int64) int64 {
	if value, err := strconv.ParseInt(os.Getenv(name), 10, 64); err == nil && value >= 0 {
		return value
	}
	return fallback
}

func (c *budgetCache[K, V]) remove(element *list.Element) {
	entry := element.Value.(*budgetEntry[K, V])
	delete(c.items, entry.key)
	c.used -= entry.bytes
	c.order.Remove(element)
}

func (c *budgetCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key]; element != nil {
		entry := element.Value.(*budgetEntry[K, V])
		if entry.expires.IsZero() || time.Now().Before(entry.expires) {
			c.order.MoveToFront(element)
			return entry.value, true
		}
		c.remove(element)
	}
	var zero V
	return zero, false
}

func (c *budgetCache[K, V]) Put(key K, value V) bool {
	weight := c.weight(value)
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key]; element != nil {
		c.remove(element)
	}
	if weight < 0 || weight > c.limit || c.limit <= 0 || c.maxEntries <= 0 {
		return false
	}
	for c.order.Len() >= c.maxEntries || c.used > c.limit-weight {
		c.remove(c.order.Back())
	}
	entry := &budgetEntry[K, V]{key: key, value: value, bytes: weight}
	if c.ttl > 0 {
		entry.expires = time.Now().Add(c.ttl)
	}
	c.items[key] = c.order.PushFront(entry)
	c.used += weight
	return true
}

func (c *budgetCache[K, V]) Del(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key]; element != nil {
		c.remove(element)
	}
}

func (c *budgetCache[K, V]) purgeExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for element := c.order.Back(); element != nil; {
		previous := element.Prev()
		entry := element.Value.(*budgetEntry[K, V])
		if !entry.expires.IsZero() && !now.Before(entry.expires) {
			c.remove(element)
		}
		element = previous
	}
}

func (c *budgetCache[K, V]) usage() (int64, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.order.Len()
}
