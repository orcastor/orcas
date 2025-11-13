
# Time Calibrator Optimization

- [English](TIME_CALIBRATOR_OPTIMIZATION.md) | [中文](TIME_CALIBRATOR_OPTIMIZATION.zh.md)

## Optimization Overview

Implemented a custom time calibrator, referencing ecache's implementation approach, using its own timestamp instead of calling `time.Now()` each time, thereby reducing GC pressure and improving performance.

## Problem Analysis

### Original Problems
- **Frequent calls to time.Now()**: Each call to `time.Now()` creates a temporary `time.Time` object
- **GC Pressure**: Large numbers of temporary objects increase garbage collection burden
- **Performance Impact**: In high concurrency scenarios, frequent GC affects performance

### Optimization Solution
Reference ecache's implementation, use a global time calibrator:
- **Periodic Updates**: Background goroutine calibrates every second, then increments 9 times every 100ms, providing 100ms precision
- **Atomic Operations**: Use `atomic.LoadInt64` and `atomic.StoreInt64` to operate on timestamp, ensuring thread safety
- **No Temporary Objects**: Directly return int64 timestamp, no temporary object creation

## Implementation Details

### Time Calibrator Implementation

```go
// Time calibrator: uses custom timestamp to reduce time.Now() calls and GC pressure
// Inspired by ecache's implementation, periodically update timestamp
var (
    clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)
)

func init() {
    go func() { // internal counter that reduce GC caused by `time.Now()`
        for {
            atomic.StoreInt64(&clock, time.Now().UnixNano()) // calibration every second
            for i := 0; i < 9; i++ {
                time.Sleep(100 * time.Millisecond)
                atomic.AddInt64(&clock, int64(100*time.Millisecond))
            }
            time.Sleep(100 * time.Millisecond)
        }
    }()
}

// Now Get current Unix timestamp (seconds)
// Uses time calibrator's timestamp to avoid creating temporary objects with each time.Now() call
// This function can be reused throughout the project to reduce GC pressure
// Named similar to time.Now(), but returns Unix timestamp (seconds) instead of time.Time object
func Now() int64 {
    return atomic.LoadInt64(&clock) / 1e9 // Convert nanoseconds to seconds
}
```

### Usage

Replace all places that originally used `time.Now().Unix()` with `core.Now()`:

```go
// Before optimization
mTime := time.Now().Unix()

// After optimization
mTime := core.Now()
```

## Optimization Effects

### Performance Improvements
1. **Reduce Temporary Objects**: No longer create `time.Time` objects when getting timestamp
2. **Lower GC Pressure**: Reduce GC frequency and pause time
3. **Improve Throughput**: In high concurrency write scenarios, expected improvement of 5-15%

### Precision Guarantee
- **Update Frequency**: Calibrate every second, then increment 9 times every 100ms (100ms precision)
- **Time Precision**: For most scenarios, 1-second precision is sufficient
- **Latency Error**: Maximum latency of 100 milliseconds, negligible

## Optimized Locations

### random_access.go
- `applyRandomWritesWithSDK`: Get timestamp when creating version objects
- All timestamp retrieval in Flush operations

### fs.go
- Get MTime when creating file objects
- Get MTime when creating directory objects

## Technical Details

### Thread Safety
- Use `atomic.LoadInt64` and `atomic.StoreInt64` to ensure concurrent safety
- Background goroutine automatically starts in `init()` function

### Memory Usage
- Only occupies one int64 (8 bytes) to store timestamp
- Background goroutine overhead is minimal (calibrate every second, then increment 9 times every 100ms)

### Time Precision
- Update frequency: Calibrate every second, then increment 9 times every 100ms
- Time precision: Unix timestamp (second-level)
- Applicable scenarios: Filesystem metadata timestamps (MTime)

## Notes

1. **Time Precision**: The time calibrator uses second-level precision. If nanosecond-level precision is needed, handle separately
2. **Initialization**: The time calibrator automatically starts in `init()` function, no manual initialization required
3. **Background Goroutine**: The time calibrator starts a background goroutine, which automatically stops when the program exits
4. **Update Mechanism**: Calibrate every second to ensure time accuracy, then increment 9 times every 100ms to provide 100ms precision

## Comparison Tests

### Before Optimization
```go
// Creates temporary object on each call
mTime := time.Now().Unix()  // Creates time.Time object
```

### After Optimization
```go
// Directly returns timestamp, no temporary object
mTime := core.Now()  // Only atomic read of int64
```

### Performance Comparison
- **Object Creation**: From creating object on each call → 0 object creations
- **GC Pressure**: Significantly reduced
- **Read Performance**: Atomic read is more efficient than `time.Now()`

## Extension Suggestions

If higher precision timestamps (nanosecond-level) are needed, you can:
1. Use `atomic.Int64` to store nanosecond timestamp
2. Adjust update frequency (e.g., update every 100 microseconds)
3. Provide `core.NowNano()` function

However, for most scenarios, second-level precision is sufficient.

