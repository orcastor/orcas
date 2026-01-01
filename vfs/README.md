# ORCAS VFS - FUSE Filesystem

ORCAS VFS uses FUSE (Filesystem in Userspace) technology to map the ORCAS object storage system as a local filesystem, allowing you to operate ORCAS storage like a regular filesystem.

- [English](README.md) | [中文](README.zh.md)
- **[Mount Guide](MOUNT_GUIDE.md)** - Complete guide for mounting OrcaS filesystem

## Platform Support

- **Linux/macOS**: Uses FUSE (Filesystem in Userspace) technology, supports full filesystem mounting
- **Windows**: 
  - Supports `RandomAccessor` API for programmatic file access
  - **Dokany Support**: Supports filesystem mounting using [Dokany](https://github.com/dokan-dev/dokany)
    - Dokany is a FUSE alternative for Windows, allowing custom filesystems in user space
    - Requires Dokany driver to be installed

## Features

### Core Features

- **Directory and File Operations**: Create, delete, rename files and directories
- **File Read/Write**: Support random read/write of file content
- **Directory Traversal**: List directory contents
- **Attribute Management**: Get and set file attributes (size, modification time, etc.)

### Advanced Features

#### 1. Atomic Replace

The atomic replace mechanism ensures atomicity and data safety during file updates, preventing data corruption during writes.

**Core Features**:
- **Atomicity Guarantee**: File updates either completely succeed or completely fail, no partial writes
- **Zero-Downtime Updates**: Old file remains readable during replacement until new file is fully ready
- **Automatic Rollback**: Automatically recovers to original state if replacement fails
- **Concurrency Safe**: Supports multiple processes accessing simultaneously without data races

**Workflow**:
```
1. Create temporary file (*.tmp)
2. Write new data to temporary file
3. Atomically replace old file when temp file is ready
4. Clean up old file data (delayed delete protects concurrent reads)
```

**Use Cases**:
- Configuration file updates
- Database file replacement
- Log rotation
- Any file update requiring data integrity guarantee

**API Example**:
```go
// Atomic replace automatically enabled through RandomAccessor
ra, _ := fs.GetOrCreateRandomAccessor(fileID)
ra.Write(0, newData)
ra.Flush()  // Automatically uses atomic replace to complete update
```

#### 2. Journal System (Journaled Writes)

The Journal system provides efficient incremental writes and version management, especially suitable for frequently modified files.

**Core Concepts**:
- **Incremental Writes**: Records only modified portions, not the entire file
- **COW (Copy-On-Write)**: Copy-on-write, keeps history versions intact
- **Smart Merging**: Automatically merges multiple small modifications, reducing storage overhead

**Journal Strategies**:

1. **Small File Strategy** (< 10MB)
   - Uses memory buffering
   - Fast merging
   - Low latency

2. **Large File Strategy** (>= 10MB)
   - Chunked processing
   - Periodic merging
   - Memory friendly

**Features**:
- **Automatic Version Creation**: Creates new version on each Flush
- **Smart Snapshots**: Automatically creates snapshots based on:
  - Entry count threshold (default 100)
  - Memory usage threshold (default 10MB)
  - Time interval threshold (default 5 minutes)
- **Memory Limits**: 
  - Max memory per Journal: 50MB
  - Global max memory: 200MB
  - Auto-flush when exceeded

**Configuration Options**:
```go
journalConfig := vfs.JournalConfig{
    Enabled:             true,
    SmallFileThreshold:  10 << 20,  // 10MB
    MergeInterval:       30 * time.Second,
    MaxEntriesSmall:     100,
    MaxEntriesLarge:     1000,
    EnableAutoMerge:     true,
    MaxMemoryPerJournal: 50 << 20,  // 50MB
    MaxTotalMemory:      200 << 20, // 200MB
}
```

#### 3. WAL (Write-Ahead Log) - Crash Recovery

WAL mechanism ensures no data loss during system crashes, providing durability guarantees.

**Core Functionality**:
- **Crash Recovery**: Automatically recovers uncommitted writes after system crash
- **Durability Guarantee**: Write operations logged to WAL before applied to actual data
- **Auto Snapshots**: Periodically creates Journal snapshots to disk

**WAL Modes**:

1. **FULL Mode** (Default)
   ```
   Write → WAL Persist → Return Success
   - Highest safety
   - Absolutely no data loss
   - Slightly slower performance
   ```

2. **ASYNC Mode**
   ```
   Write → Async WAL → Immediate Return
   - High performance
   - Minimal chance of losing last few writes
   - Suitable for logs
   ```

3. **NONE Mode**
   ```
   Write → Memory → Periodic Flush
   - Highest performance
   - May lose data on crash
   - Only for temporary data
   ```

**WAL Configuration**:
```go
walConfig := vfs.JournalWALConfig{
    Enabled:              true,
    SyncMode:            "FULL",  // FULL/ASYNC/NONE
    SnapshotInterval:    5 * time.Minute,
    MaxWALSize:          100 << 20,  // 100MB
    AutoCheckpoint:      true,
    CheckpointInterval:  1 * time.Minute,
}
```

**Recovery Example**:
```go
// Automatically recovers on system startup
jm := vfs.NewJournalManager(fs, config)
// JournalManager automatically recovers uncommitted Journals from WAL snapshots
```

#### 4. Version Retention Policy

Intelligent version management system that automatically cleans up old versions while maintaining sufficient history for rollback.

**Retention Policies**:

1. **Time Window Merging**
   - Within time window (default 5 minutes), keeps only last full version
   - Reduces version bloat from frequent modifications

2. **Maximum Version Limit**
   - Total version count limit (default 10)
   - Automatically deletes oldest versions when exceeded

3. **Minimum Full Version Protection**
   - Always keeps minimum number of full versions (default 3)
   - Ensures rollback capability at any time

4. **Journal Count Control**
   - Max Journals per base version (default 10)
   - Automatically merges Journals into new base when exceeded

**Configuration Example**:
```go
policy := vfs.VersionRetentionPolicy{
    Enabled:                true,
    TimeWindowSeconds:      5 * 60,   // 5 minutes
    MaxVersions:            10,       // Max 10 versions
    MinFullVersions:        3,        // At least 3 full versions
    MaxJournalsPerBase:     10,       // Max 10 journals per base
    CleanupIntervalSeconds: 5 * 60,   // Cleanup every 5 minutes
}

vrm := vfs.NewVersionRetentionManager(fs, policy)
defer vrm.Stop()
```

**Journal Merge Strategies**:

System provides three intelligent merge strategies, automatically selected based on file size and Journal count:

1. **In-Memory Merge**
   - For: Small files (< 50MB)
   - Memory: = file size
   - Performance: Fastest

2. **Streaming Merge**
   - For: Large files (> 50MB)
   - Memory: Fixed ~10MB
   - Performance: Moderate, memory safe
   - Supports GB-level files

3. **Batch Merge**
   - For: Many Journals (> 5)
   - Batch processing, prevents memory overflow
   - Progressive merging

**Merge Configuration**:
```go
mergeConfig := vfs.MergeConfig{
    MaxMemoryBuffer:     100 << 20,  // 100MB
    StreamChunkSize:     10 << 20,   // 10MB
    StreamingThreshold:  50 << 20,   // 50MB triggers streaming
    MaxJournalsPerMerge: 5,          // Max 5 per batch
}
```

**Version Statistics**:
```go
stats := vrm.GetRetentionStats()
fmt.Printf("Total Cleanups: %d\n", stats["totalCleanups"])
fmt.Printf("Deleted Versions: %d\n", stats["totalDeleted"])
fmt.Printf("Last Cleanup: %d\n", stats["lastCleanup"])
```

### Configuration Support

- **Encryption**: Support AES256 and SM4 encryption
- **Compression**: Support smart compression (automatically decide based on file type)
- **Compression Algorithms**: Support Snappy, Zstd, Gzip, Brotli
- **Instant Upload**: Support instant upload (via RefLevel configuration)

## Usage

### Basic Mounting

```go
package main

import (
    "context"
    "github.com/orcastor/orcas/core"
    "github.com/orcastor/orcas/vfs"
)

func main() {
    // Create Handler
    h := core.NewLocalHandler()
    
    // Login to get Context
    ctx, _, _, err := h.Login(context.Background(), "username", "password")
    if err != nil {
        panic(err)
    }
    
    // Configure options (encryption, compression, instant upload, etc.)
    cfg := &core.Config{
        RefLevel:  core.REF_LEVEL_FULL,         // Instant upload level: FULL (full file instant upload)
        CmprWay:   core.DATA_CMPR_GZIP,         // Compression: Gzip (smart compression by default)
        CmprQlty:  5,                           // Compression level: 5
        EndecWay:  core.DATA_ENDEC_AES256,      // Encryption method: AES256
        EndecKey:  "your-encryption-key-here",   // Encryption key (AES256 requires >16 characters)
    }
    
    // Mount filesystem
    server, err := vfs.Mount(h, ctx, bucketID, &vfs.MountOptions{
        MountPoint: "/mnt/orcas",
        Foreground: true,
        AllowOther: false,
        Config:     cfg,  // Pass configuration
    })
    if err != nil {
        panic(err)
    }
    
    // Run service
    vfs.Serve(server, true)
}
```

### Mount Options

- `MountPoint`: Mount point path
- `Foreground`: Run in foreground (true means block until signal received)
- `AllowOther`: Allow other users to access
- `AllowRoot`: Allow root access
- `DefaultPermissions`: Use default permission checks
- `FuseOptions`: Custom FUSE options
- `Config`: Configuration (for encryption, compression, instant upload, etc.)
  - `RefLevel`: Instant upload level (REF_LEVEL_OFF/REF_LEVEL_FULL/REF_LEVEL_FAST)
  - `CmprWay`: Compression method (smart compression by default, automatically decides based on file type)
  - `CmprQlty`: Compression level
  - `EndecWay`: Encryption method (AES256/SM4)
  - `EndecKey`: Encryption key

## Implementation Details

### Filesystem Structure

- **Root Node** (`/`): Maps to ORCAS ROOT_OID
- **Directory Nodes**: Map to `OBJ_TYPE_DIR` type objects
- **File Nodes**: Map to `OBJ_TYPE_FILE` type objects

### File Read/Write

- **Reading**: Directly read, decrypt, decompress data by chunk, without using SDK's DataReader
  - Support random access reading (via offset)
  - Automatically handle packaged files (if file is packaged)
  - Automatically decrypt encrypted data
  - Automatically decompress compressed data
- **Writing**: Use `RandomAccessor` for random writes, support COW (Copy-On-Write)
  - Support random writes (can write data at any position)
  - Write buffer, delayed flush for better performance
  - Automatic version management (create new version on each Flush)

### Caching Mechanism

- Object info cache: Each node caches object information, reducing database queries
- Cache invalidation: Automatically invalidate cache after create, delete, rename operations

## Notes

1. **Permissions**: Ensure sufficient permissions to create mount point and mount filesystem
2. **Performance**: Filesystem operations are converted to ORCAS API calls, may be slower than local filesystem
3. **Concurrency**: Supports multi-threaded concurrent access, but need to pay attention to ORCAS Handler's thread safety

## Performance Testing

### Run Benchmark Tests

```bash
# Run all benchmark tests
go test -bench=BenchmarkRandomAccessor -benchmem ./vfs

# Run specific test
go test -bench=BenchmarkRandomAccessor_Read_1MB -benchmem ./vfs

# Run and generate performance profile
go test -bench=BenchmarkRandomAccessor -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof ./vfs
```

### Performance Test Cases

Benchmark test file `random_access_bench_test.go` includes the following tests:

1. **Write Performance Tests**
   - Different data sizes: 1KB, 10KB, 100KB, 1MB
   - Write and flush performance
   - Multiple write performance

2. **Read Performance Tests**
   - Different data sizes: 1KB, 10KB, 100KB, 1MB
   - Compressed data reading
   - Encrypted data reading
   - Compressed and encrypted data reading

3. **Random Access Performance Tests**
   - Random offset reading
   - Exact size reading (verify not exceeding requested size)
   - Streaming read performance

4. **Buffer Operation Performance Tests**
   - Read performance with buffered writes

### Performance Optimization Features

- **Exact Size Control**: Read data does not exceed requested size, reducing memory usage
- **Streaming Processing**: Stream read by chunk, process while reading, avoid loading all data at once
- **Memory Optimization**: Use object pool to reuse buffers, reduce GC pressure
- **Concurrency Optimization**: Use atomic operations to reduce lock contention

## Environment Information

### System Requirements

- **Operating System**:
  - Linux (Recommended): Supports full FUSE functionality
  - macOS: Supports full FUSE functionality (requires macOSFUSE installation)
  - Windows:
    - Current: Only supports RandomAccessor API (programmatic access)
    - Future: Plans to support Dokany filesystem mounting (requires Dokany driver installation)

- **Go Version**: Go 1.18 or higher

- **Dependencies**:
  - `github.com/hanwen/go-fuse/v2`: FUSE library (Linux/macOS only)
  - `github.com/orcastor/orcas/core`: ORCAS core library

### Environment Variables

- `BasePath`: Base path for metadata (database storage location), defaults to current directory (.)
- `DataPath`: Data path for file data storage location, defaults to current directory (.)

If not set, temporary directories are automatically used during testing.

### macOS FUSE Installation

To use FUSE functionality on macOS, install macOSFUSE:

```bash
# Install using Homebrew
brew install --cask macfuse

# Or use official installer
# Download: https://github.com/osxfuse/osxfuse/releases
```

After installation, restart the system or log out and log back in.

### Windows Dokany Installation

To use filesystem mounting functionality on Windows, install Dokany:

1. **Download and Install Dokany Driver**:
   - Visit [Dokany Official GitHub](https://github.com/dokan-dev/dokany/releases)
   - Download the latest version installer (DokanSetup_*.exe)
   - Run the installer and restart the system

2. **Usage Example**:
   ```go
   package main

   import (
       "context"
       "github.com/orcastor/orcas/core"
       "github.com/orcastor/orcas/vfs"
   )

   func main() {
       h := core.NewLocalHandler()
       ctx, _, _, _ := h.Login(context.Background(), "username", "password")
       
       cfg := &core.Config{}
       instance, err := vfs.Mount(h, ctx, bucketID, &vfs.MountOptions{
           MountPoint:  "M:\\",
           Foreground:  true,
           Config:      cfg,
       })
       if err != nil {
           panic(err)
       }
       
       // Run service
       vfs.Serve(instance, true)
   }
   ```

3. **Notes**:
   - Dokany requires administrator privileges for installation
   - System restart required after installation
   - Compatibility differences may exist across Windows versions, recommended for Windows 10/11

### Test Environment

Tests and benchmarks use the following environment:

- **Temporary Directory**: Uses system temporary directory (`os.TempDir()`)
- **Database**: Each test creates an independent database instance
- **Data Isolation**: Each test uses an independent bucket ID

## Dependencies

- `github.com/hanwen/go-fuse/v2`: FUSE library (Linux/macOS only)

## Limitations

- Current implementation is a simplified version, some advanced features (such as symbolic links, hard links) are not yet supported
- File truncation operations need improvement
- Large file chunk reading needs optimization
- Windows platform supports both RandomAccessor API and Dokany filesystem mounting
  - Dokany mounting requires Dokany driver to be installed

## Windows Platform Support

### Current Status
- ✅ Supports `RandomAccessor` API (programmatic access)
- ✅ Supports Dokany filesystem mounting

### Implementation Details

**Dokany Integration**:
- Dynamically loads `dokan2.dll` at runtime
- Implements full filesystem operations:
  - CreateFile, ReadFile, WriteFile
  - GetFileInformation, FindFiles
  - DeleteFile, DeleteDirectory
  - MoveFile (rename/move)
  - SetFileAttributes, SetFileTime
  - And more...

**Advantages**:
- Similar to FUSE's userspace filesystem implementation
- No need to write kernel drivers
- Supports complete filesystem operations

**Notes**:
- Dokany requires administrator privileges for installation
- Compatibility differences may exist across Windows versions
- Tested on Windows 10/11

## Documentation

### Performance Optimization Documentation

- [Performance Optimization Final Report (English)](PERFORMANCE_OPTIMIZATION_FINAL.md) | [中文](PERFORMANCE_OPTIMIZATION_FINAL.zh.md)

### Other Documentation

- [Mount Guide (English)](MOUNT_GUIDE.md) | [中文](MOUNT_GUIDE.zh.md) - Complete mounting tutorial

