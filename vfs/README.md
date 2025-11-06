# ORCAS VFS - FUSE Filesystem

ORCAS VFS uses FUSE (Filesystem in Userspace) technology to map the ORCAS object storage system as a local filesystem, allowing you to operate ORCAS storage like a regular filesystem.

- [English](README.md) | [中文](README.zh.md)

## Platform Support

- **Linux/macOS**: Uses FUSE (Filesystem in Userspace) technology, supports full filesystem mounting
- **Windows**: 
  - Current version: Only supports `RandomAccessor` API, filesystem mounting not supported
  - Future plan: Consider using [Dokany](https://github.com/dokan-dev/dokany) for Windows filesystem mounting
    - Dokany is a FUSE alternative for Windows, allowing custom filesystems in user space
    - Requires Dokany driver and Go bindings

## Features

- **Directory and File Operations**: Create, delete, rename files and directories
- **File Read/Write**: Support random read/write of file content
- **Directory Traversal**: List directory contents
- **Attribute Management**: Get and set file attributes (size, modification time, etc.)
- **SDK Integration**: Support SDK features like encryption, compression, instant upload
  - **Encryption**: Support AES256 and SM4 encryption
  - **Compression**: Support smart compression (automatically decide based on file type)
  - **Compression Algorithms**: Support Snappy, Zstd, Gzip, Brotli
  - **Instant Upload**: Support instant upload (via RefLevel configuration)
  - **Data Sync**: Support power failure protection strategy (DataSync)

## Usage

### Basic Mounting

```go
package main

import (
    "context"
    "github.com/orcastor/orcas/core"
    "github.com/orcastor/orcas/sdk"
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
    
    // Configure SDK options (encryption, compression, instant upload, etc.)
    sdkCfg := &sdk.Config{
        RefLevel:  sdk.FULL,                    // Instant upload level: FULL (full file instant upload)
        WiseCmpr:  core.DATA_CMPR_GZIP,         // Smart compression: Gzip
        CmprQlty:  5,                           // Compression level: 5
        EndecWay:  core.DATA_ENDEC_AES256,      // Encryption method: AES256
        EndecKey:  "your-encryption-key-here",   // Encryption key (AES256 requires >16 characters)
        DataSync:  true,                        // Power failure protection: flush to disk after each write
    }
    
    // Mount filesystem
    server, err := vfs.Mount(h, ctx, bucketID, &vfs.MountOptions{
        MountPoint: "/mnt/orcas",
        Foreground: true,
        AllowOther: false,
        SDKConfig:  sdkCfg,  // Pass SDK configuration
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
- `SDKConfig`: SDK configuration (for encryption, compression, instant upload, etc.)
  - `RefLevel`: Instant upload level (OFF/FULL/FAST)
  - `WiseCmpr`: Smart compression (automatically decide based on file type)
  - `CmprQlty`: Compression level
  - `EndecWay`: Encryption method (AES256/SM4)
  - `EndecKey`: Encryption key
  - `DataSync`: Power failure protection strategy

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
  - `github.com/orcastor/orcas/sdk`: ORCAS SDK
  - **Windows Mounting (Future)**: Requires Dokany driver and Go bindings

### Environment Variables

- `ORCAS_BASE`: ORCAS base directory (database storage location)
- `ORCAS_DATA`: ORCAS data directory (file data storage location)

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

### Windows Dokany Installation (Future Support)

To use filesystem mounting functionality on Windows, install Dokany:

1. **Download and Install Dokany Driver**:
   - Visit [Dokany Official GitHub](https://github.com/dokan-dev/dokany/releases)
   - Download the latest version installer (DokanSetup_*.exe)
   - Run the installer and restart the system

2. **Install Go Bindings** (To be developed):
   ```bash
   # Wait for Dokany Go bindings to be available
   # go get github.com/dokan-dev/dokany-go
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
- Windows platform currently only supports RandomAccessor API, filesystem mounting not supported
  - Future plans to implement Windows filesystem mounting via Dokany

## Windows Platform Support Plan

### Current Status
- ✅ Supports `RandomAccessor` API (programmatic access)
- ❌ Filesystem mounting not supported

### Future Plans
Considering using [Dokany](https://github.com/dokan-dev/dokany) to implement Windows filesystem mounting:

**Advantages**:
- Similar to FUSE's userspace filesystem implementation
- No need to write kernel drivers
- Supports complete filesystem operations

**Implementation Requirements**:
1. Install Dokany driver
2. Use or develop Dokany Go bindings
3. Implement Dokany filesystem interface (similar to FUSE interface)

**Notes**:
- Dokany requires administrator privileges for installation
- Compatibility differences may exist across Windows versions
- Need to test compatibility with common software (e.g., Office, WPS, etc.)

## Documentation

- [Performance Optimization Report (English)](./PERFORMANCE_OPTIMIZATION_FINAL.en.md) | [中文](./PERFORMANCE_OPTIMIZATION_FINAL.zh.md)
- [Time Calibrator Optimization (English)](./TIME_CALIBRATOR_OPTIMIZATION.en.md) | [中文](./TIME_CALIBRATOR_OPTIMIZATION.zh.md)
- [Optimization Summary (English)](./OPTIMIZATION_SUMMARY.en.md) | [中文](./OPTIMIZATION_SUMMARY.zh.md)
