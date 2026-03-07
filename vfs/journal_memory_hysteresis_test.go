package vfs

import (
	"testing"
	"time"
)

func TestMemoryLimitHardCeiling(t *testing.T) {
	small := int64(1 << 20) // 1MB
	if got := memoryLimitHardCeiling(small); got != small {
		t.Fatalf("small limit should keep strict ceiling: got=%d want=%d", got, small)
	}

	large := int64(50 << 20) // 50MB
	got := memoryLimitHardCeiling(large)
	if got <= large {
		t.Fatalf("large limit should have burst headroom: got=%d limit=%d", got, large)
	}
}

func TestShouldForceMemoryLimitFlush(t *testing.T) {
	now := time.Now().UnixNano()

	t.Run("strict mode for small limit", func(t *testing.T) {
		limit := int64(1 << 20) // 1MB
		projected := limit + 1
		last := now - int64(50*time.Millisecond)
		if !shouldForceMemoryLimitFlush(projected, limit, last, now) {
			t.Fatalf("expected strict small-limit flush")
		}
	})

	t.Run("first exceed always flush", func(t *testing.T) {
		limit := int64(50 << 20) // 50MB
		projected := limit + (1 << 20)
		if !shouldForceMemoryLimitFlush(projected, limit, 0, now) {
			t.Fatalf("expected flush on first exceed")
		}
	})

	t.Run("within cooldown below hard ceiling defer", func(t *testing.T) {
		limit := int64(50 << 20) // 50MB, hard ceiling ~60MB
		projected := limit + (5 << 20)
		last := now - int64(50*time.Millisecond)
		if shouldForceMemoryLimitFlush(projected, limit, last, now) {
			t.Fatalf("expected defer within cooldown below hard ceiling")
		}
	})

	t.Run("within cooldown above hard ceiling force", func(t *testing.T) {
		limit := int64(50 << 20) // 50MB, hard ceiling ~60MB
		projected := limit + (12 << 20)
		last := now - int64(50*time.Millisecond)
		if !shouldForceMemoryLimitFlush(projected, limit, last, now) {
			t.Fatalf("expected force flush above hard ceiling")
		}
	})

	t.Run("after cooldown below hard ceiling force", func(t *testing.T) {
		limit := int64(50 << 20) // 50MB, hard ceiling ~60MB
		projected := limit + (5 << 20)
		last := now - int64(600*time.Millisecond)
		if !shouldForceMemoryLimitFlush(projected, limit, last, now) {
			t.Fatalf("expected force flush after cooldown")
		}
	})
}
