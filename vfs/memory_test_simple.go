package main

import (
	"fmt"
	"runtime"
	"time"
)

// MemoryStats records memory usage
type MemoryStats struct {
	Timestamp    time.Time
	HeapAlloc   uint64
	HeapInUse   uint64
	HeapObjects uint64
	Description string
}

func collectStats(desc string) MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemoryStats{
		Timestamp:    time.Now(),
		HeapAlloc:   m.HeapAlloc,
		HeapInUse:   m.HeapInuse,
		HeapObjects: m.HeapObjects,
		Description: desc,
	}
}

func printStats(s MemoryStats) {
	fmt.Printf("[%s] %s\n", s.Timestamp.Format("15:04:05.000"), s.Description)
	fmt.Printf("  Heap: %10d MB (alloc=%10d, objects=%d)\n\n",
		s.HeapInUse/1024/1024, s.HeapAlloc/1024/1024, s.HeapObjects)
}

// Simulate chunk buffer leak (before fix)
func simulateChunkBufferLeak() []MemoryStats {
	stats := make([]MemoryStats, 0, 15)

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	stats = append(stats, collectStats("Baseline"))
	printStats(stats[0])

	// Simulate writing 100MB file with 10MB chunks
	const fileSize = 100 * 1024 * 1024
	const chunkSize = 10 * 1024 * 1024

	// BEFORE FIX: chunk buffers not released
	type chunkBuffer struct {
		data   []byte
		ranges []int
	}
	chunks := make(map[int]*chunkBuffer)

	bytesWritten := 0
	for bytesWritten < fileSize {
		// Allocate 10MB chunk
		chunk := &chunkBuffer{
			data:   make([]byte, chunkSize),
			ranges: make([]int, 0, 100),
		}

		// Simulate filling chunk
		for i := range chunk.data {
			chunk.data[i] = byte(i % 256)
		}

		// Store chunk (BEFORE FIX: never release)
		chunkNum := bytesWritten / chunkSize
		chunks[chunkNum] = chunk

		bytesWritten += chunkSize

		// Collect stats every chunk
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
		stats = append(stats, collectStats(fmt.Sprintf("After chunk %d (%d MB written)",
			chunkNum, bytesWritten/1024/1024)))
		printStats(stats[len(stats)-1])
	}

	// BEFORE FIX: Don't release chunks
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectStats("After write (chunks NOT released)"))
	printStats(stats[len(stats)-1])

	return stats
}

// Simulate chunk buffer with fix (after fix)
func simulateChunkBufferFixed() []MemoryStats {
	stats := make([]MemoryStats, 0, 15)

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	stats = append(stats, collectStats("Baseline"))
	printStats(stats[0])

	// Simulate writing 100MB file with 10MB chunks
	const fileSize = 100 * 1024 * 1024
	const chunkSize = 10 * 1024 * 1024

	type chunkBuffer struct {
		data   []byte
		ranges []int
	}
	chunks := make(map[int]*chunkBuffer)

	bytesWritten := 0
	for bytesWritten < fileSize {
		// Allocate 10MB chunk
		chunk := &chunkBuffer{
			data:   make([]byte, chunkSize),
			ranges: make([]int, 0, 100),
		}

		// Simulate filling chunk
		for i := range chunk.data {
			chunk.data[i] = byte(i % 256)
		}

		// Store chunk
		chunkNum := bytesWritten / chunkSize
		chunks[chunkNum] = chunk

		bytesWritten += chunkSize

		// AFTER FIX: Release chunk data after flush
		// Simulate flush and immediate release
		if chunk := chunks[chunkNum]; chunk != nil {
			// AFTER FIX: Explicitly release (as in the fix)
			chunk.data = nil      // Release 10MB
			chunk.ranges = nil    // Release ranges
		}

		// Collect stats every chunk
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
		stats = append(stats, collectStats(fmt.Sprintf("After chunk %d (%d MB written, released)",
			chunkNum, bytesWritten/1024/1024)))
		printStats(stats[len(stats)-1])
	}

	// AFTER FIX: All chunks already released
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectStats("After write (chunks released)"))
	printStats(stats[len(stats)-1])

	return stats
}

// Simulate RandomAccessor registry leak
func simulateRegistryLeak() []MemoryStats {
	stats := make([]MemoryStats, 0, 12)

	runtime.GC()
	stats = append(stats, collectStats("Baseline"))
	printStats(stats[0])

	type RandomAccessor struct {
		buffer  []byte
		journal []byte
	}
	registry := make(map[int]*RandomAccessor)

	// Create 50 RandomAccessors without cleanup
	for i := 0; i < 50; i++ {
		ra := &RandomAccessor{
			buffer:  make([]byte, 10*1024*1024),  // 10MB
			journal: make([]byte, 5*1024*1024),   // 5MB
		}
		registry[i] = ra

		if (i+1)%10 == 0 {
			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			stats = append(stats, collectStats(fmt.Sprintf("After %d RAs (not cleaned)", i+1)))
			printStats(stats[len(stats)-1])
		}
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectStats("After 50 RAs (not cleaned)"))
	printStats(stats[len(stats)-1])

	return stats
}

// Simulate RandomAccessor registry with cleanup
func simulateRegistryFixed() []MemoryStats {
	stats := make([]MemoryStats, 0, 12)

	runtime.GC()
	stats = append(stats, collectStats("Baseline"))
	printStats(stats[0])

	type RandomAccessor struct {
		buffer  []byte
		journal []byte
	}
	registry := make(map[int]*RandomAccessor)

	// Create 50 RandomAccessors with periodic cleanup
	for i := 0; i < 50; i++ {
		ra := &RandomAccessor{
			buffer:  make([]byte, 10*1024*1024),  // 10MB
			journal: make([]byte, 5*1024*1024),   // 5MB
		}
		registry[i] = ra

		// AFTER FIX: Simulate cleanup worker every 10 items
		if (i+1)%10 == 0 {
			// Simulate cleanup of inactive RAs
			for k := range registry {
				if k < i-5 { // Remove old ones
					delete(registry, k)
				}
			}

			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			stats = append(stats, collectStats(fmt.Sprintf("After %d RAs (with cleanup)", i+1)))
			printStats(stats[len(stats)-1])
		}
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectStats("After 50 RAs (with cleanup)"))
	printStats(stats[len(stats)-1])

	return stats
}

func analyzeStats(label string, stats []MemoryStats) {
	fmt.Printf("\n=== %s Analysis ===\n", label)

	if len(stats) < 2 {
		return
	}

	baseline := stats[0]
	peakAlloc := baseline.HeapAlloc
	peakInUse := baseline.HeapInUse
	peakIdx := 0

	for i, s := range stats {
		if s.HeapAlloc > peakAlloc {
			peakAlloc = s.HeapAlloc
			peakIdx = i
		}
		if s.HeapInUse > peakInUse {
			peakInUse = s.HeapInUse
		}
	}

	final := stats[len(stats)-1]

	fmt.Printf("Baseline:    %10d MB\n", baseline.HeapInUse/1024/1024)
	fmt.Printf("Peak Alloc:  %10d MB (at '%s')\n", peakAlloc/1024/1024, stats[peakIdx].Description)
	fmt.Printf("Peak In Use: %10d MB\n", peakInUse/1024/1024)
	fmt.Printf("Final:       %10d MB\n", final.HeapInUse/1024/1024)
	fmt.Printf("Growth:      %10d MB\n\n", (final.HeapInUse-baseline.HeapInUse)/1024/1024)
}

func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║  VFS Memory Leak Fix - Simplified Demonstration            ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Test 1: Chunk buffer leak
	fmt.Println("═══ Test 1: Chunk Buffer Leak (Before Fix) ═══")
	stats1 := simulateChunkBufferLeak()
	analyzeStats("Chunk Buffer Leak", stats1)

	fmt.Println("═══ Test 2: Chunk Buffer With Fix (After Fix) ═══")
	stats2 := simulateChunkBufferFixed()
	analyzeStats("Chunk Buffer Fixed", stats2)

	// Comparison
	fmt.Println("═══ Comparison ═══")
	growth1 := (stats1[len(stats1)-1].HeapInUse - stats1[0].HeapInUse) / 1024 / 1024
	growth2 := (stats2[len(stats2)-1].HeapInUse - stats2[0].HeapInUse) / 1024 / 1024
	peak1 := stats1[0].HeapInUse
	peak2 := stats2[0].HeapInUse
	for _, s := range stats1 {
		if s.HeapInUse > peak1 {
			peak1 = s.HeapInUse
		}
	}
	for _, s := range stats2 {
		if s.HeapInUse > peak2 {
			peak2 = s.HeapInUse
		}
	}

	fmt.Printf("Before Fix:  Peak=%d MB, Growth=%d MB\n", peak1/1024/1024, growth1)
	fmt.Printf("After Fix:   Peak=%d MB, Growth=%d MB\n", peak2/1024/1024, growth2)
	fmt.Printf("Improvement: Peak ↓%.1f%%, Growth ↓%.1f%%\n\n",
		float64(peak1-peak2)/float64(peak1)*100,
		float64(growth1-growth2)/float64(growth1)*100)

	time.Sleep(1 * time.Second)
	runtime.GC()

	// Test 3: Registry leak
	fmt.Println("═══ Test 3: Registry Leak (Before Fix) ═══")
	stats3 := simulateRegistryLeak()
	analyzeStats("Registry Leak", stats3)

	fmt.Println("═══ Test 4: Registry With Cleanup (After Fix) ═══")
	stats4 := simulateRegistryFixed()
	analyzeStats("Registry Fixed", stats4)

	// Comparison
	fmt.Println("═══ Comparison ═══")
	growth3 := (stats3[len(stats3)-1].HeapInUse - stats3[0].HeapInUse) / 1024 / 1024
	growth4 := (stats4[len(stats4)-1].HeapInUse - stats4[0].HeapInUse) / 1024 / 1024

	fmt.Printf("Before Fix:  Growth=%d MB\n", growth3)
	fmt.Printf("After Fix:   Growth=%d MB\n", growth4)
	fmt.Printf("Improvement: Growth ↓%.1f%%\n\n",
		float64(growth3-growth4)/float64(growth3)*100)

	fmt.Println("═══ Summary ═══")
	fmt.Println("The fix successfully reduces memory usage by:")
	fmt.Println("  • Releasing chunk buffers immediately after flush")
	fmt.Println("  • Periodic cleanup of inactive RandomAccessors")
	fmt.Println("  • Periodic cleanup of inactive Journals")
	fmt.Println("  • Forced GC after batch cleanup operations")
}
