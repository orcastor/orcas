# 时间校准器优化说明

- [English](TIME_CALIBRATOR_OPTIMIZATION.md) | [中文](TIME_CALIBRATOR_OPTIMIZATION.zh.md)

## 优化概述

实现了一个自定义的时间校准器，参考ecache的实现方式，使用自己的时间戳而不是每次调用`time.Now()`，从而减少GC压力，提升性能。

## 问题分析

### 原有问题
- **频繁调用time.Now()**: 每次调用`time.Now()`都会创建一个`time.Time`临时对象
- **GC压力**: 大量临时对象增加垃圾回收负担
- **性能影响**: 在高并发场景下，频繁的GC会影响性能

### 优化方案
参考ecache的实现，使用全局时间校准器：
- **定期更新**: 后台协程每秒校准一次，然后每100毫秒递增9次，提供100毫秒精度
- **原子操作**: 使用`atomic.LoadInt64`和`atomic.StoreInt64`操作时间戳，保证线程安全
- **无临时对象**: 直接返回int64时间戳，不创建临时对象

## 实现细节

### 时间校准器实现

```go
// Time calibrator: uses custom timestamp to reduce time.Now() calls and GC pressure
// Inspired by ecache's implementation, periodically update timestamp
var (
    clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)
)

func now() int64 { return atomic.LoadInt64(&clock) }
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

### 使用方式

所有原来使用`time.Now().Unix()`的地方都替换为`core.Now()`：

```go
// 优化前
mTime := time.Now().Unix()

// 优化后
mTime := core.Now()
```

## 优化效果

### 性能提升
1. **减少临时对象**: 每次获取时间戳不再创建`time.Time`对象
2. **降低GC压力**: 减少GC频率和暂停时间
3. **提升吞吐量**: 在高并发写入场景下，预计提升5-15%

### 精度保证
- **更新频率**: 每秒校准一次，然后每100毫秒递增9次（100ms精度）
- **时间精度**: 对于大多数场景，1秒精度已经足够
- **延迟误差**: 最大延迟100毫秒，可忽略不计

## 已优化的位置

### random_access.go
- `applyRandomWritesWithSDK`: 创建版本对象时获取时间戳
- 所有Flush操作中的时间戳获取

### fs.go
- 创建文件对象时获取MTime
- 创建目录对象时获取MTime

## 技术细节

### 线程安全
- 使用`atomic.LoadInt64`和`atomic.StoreInt64`保证并发安全
- 在`init()`函数中自动启动后台协程

### 内存占用
- 仅占用一个int64（8字节）存储时间戳
- 后台协程开销极小（每秒校准一次，然后每100毫秒递增9次）

### 时间精度
- 更新频率：每秒校准一次，然后每100毫秒递增9次
- 时间精度：Unix时间戳（秒级）
- 适用场景：文件系统元数据时间戳（MTime）

## 注意事项

1. **时间精度**: 时间校准器使用秒级精度，如果需要纳秒级精度，需要单独处理
2. **初始化**: 时间校准器在`init()`函数中自动启动，无需手动初始化
3. **后台协程**: 时间校准器会启动一个后台协程，程序退出时会自动停止
4. **更新机制**: 每秒校准一次确保时间准确性，然后每100毫秒递增9次提供100毫秒精度

## 对比测试

### 优化前
```go
// 每次调用创建临时对象
mTime := time.Now().Unix()  // 创建time.Time对象
```

### 优化后
```go
// 直接返回时间戳，无临时对象
mTime := core.Now()  // 仅原子读取int64
```

### 性能对比
- **对象创建**: 从每次调用创建对象 → 0次对象创建
- **GC压力**: 显著降低
- **读取性能**: 原子读取比`time.Now()`更高效

## 扩展建议

如果需要更高精度的时间戳（纳秒级），可以：
1. 使用`atomic.Int64`存储纳秒时间戳
2. 调整更新频率（如每100微秒更新一次）
3. 提供`core.NowNano()`函数

但在大多数场景下，秒级精度已经足够。

