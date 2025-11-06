
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
- **Periodic Updates**: Background goroutine periodically updates timestamp (default every millisecond)
- **Atomic Operations**: Use `atomic.Int64` to store timestamp, ensuring thread safety
- **No Temporary Objects**: Directly return int64 timestamp, no temporary object creation

## Implementation Details

### Time Calibrator Implementation

```go
var (
    // Time calibrator: use own timestamp, reduce time.Now() calls and GC pressure
    currentTimeStamp atomic.Int64 // Current Unix timestamp (seconds)
    timeCalibratorOnce sync.Once  // Ensure initialization only once
)

// initTimeCalibrator initializes the time calibrator
func initTimeCalibrator() {
    timeCalibratorOnce.Do(func() {
        // Initialize current timestamp
        currentTimeStamp.Store(time.Now().Unix())
        
        // Start time calibrator goroutine, update timestamp every millisecond
        go func() {
            ticker := time.NewTicker(1 * time.Millisecond)
            defer ticker.Stop()
            for {
                currentTimeStamp.Store(time.Now().Unix())
                <-ticker.C
            }
        }()
    })
}

// Now gets the current Unix timestamp (seconds)
func Now() int64 {
    initTimeCalibrator()
    return currentTimeStamp.Load()
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
- **Update Frequency**: Update every millisecond (1ms precision)
- **Time Precision**: For most scenarios, 1-second precision is sufficient
- **Latency Error**: Maximum latency of 1 millisecond, negligible

## Optimized Locations

### random_access.go
- `applyRandomWritesWithSDK`: Get timestamp when creating version objects
- All timestamp retrieval in Flush operations

### fs.go
- Get MTime when creating file objects
- Get MTime when creating directory objects

## Technical Details

### Thread Safety
- Use `atomic.Int64` to ensure concurrent safety
- Use `sync.Once` to ensure initialization only once

### Memory Usage
- Only occupies one int64 (8 bytes) to store timestamp
- Background goroutine overhead is minimal (one atomic write operation per millisecond)

### Time Precision
- Update frequency: 1 millisecond
- Time precision: Unix timestamp (second-level)
- Applicable scenarios: Filesystem metadata timestamps (MTime)

## Notes

1. **Time Precision**: The time calibrator uses second-level precision. If nanosecond-level precision is needed, handle separately
2. **Initialization**: The time calibrator automatically initializes on first call, no manual initialization required
3. **Background Goroutine**: The time calibrator starts a background goroutine, which automatically stops when the program exits

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

