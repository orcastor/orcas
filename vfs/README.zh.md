# ORCAS VFS - FUSE文件系统

ORCAS VFS 使用 FUSE (Filesystem in Userspace) 技术，将 ORCAS 对象存储系统映射为本地文件系统，允许像操作普通文件系统一样操作 ORCAS 存储。

- [English](README.md) | [中文](README.zh.md)
- **[挂载指南](MOUNT_GUIDE.zh.md)** - OrcaS 文件系统挂载完整指南

## 平台支持

- **Linux/macOS**: 使用 FUSE (Filesystem in Userspace) 技术，支持完整文件系统挂载功能
- **Windows**: 
  - 支持 `RandomAccessor` API 用于程序化文件访问
  - **Dokany 支持**：支持使用 [Dokany](https://github.com/dokan-dev/dokany) 进行文件系统挂载
    - Dokany 是 Windows 上的 FUSE 替代方案，允许在用户空间创建自定义文件系统
    - 需要安装 Dokany 驱动程序

## 功能特性

### 核心功能

- **目录和文件操作**：支持创建、删除、重命名文件和目录
- **文件读写**：支持随机读写文件内容
- **目录遍历**：支持列出目录内容
- **属性管理**：支持获取和设置文件属性（大小、修改时间等）

### 高级特性

#### 1. 原子替换 (Atomic Replace)

原子替换机制确保文件更新的原子性和数据安全性，避免写入过程中的数据损坏。

**核心特性**:
- **原子性保证**: 文件更新要么完全成功，要么完全失败，不会出现部分写入
- **零停机更新**: 在替换过程中，旧文件保持可读，直到新文件完全准备好
- **自动回滚**: 如果替换失败，自动恢复到原始状态
- **并发安全**: 支持多个进程同时访问，不会出现数据竞争

**工作流程**:
```
1. 创建临时文件 (*.tmp)
2. 写入新数据到临时文件
3. 临时文件准备就绪后，原子性地替换旧文件
4. 清理旧文件数据（延迟删除保护并发读取）
```

**使用场景**:
- 配置文件更新
- 数据库文件替换
- 日志轮转
- 任何需要保证数据完整性的文件更新操作

**API示例**:
```go
// 通过 RandomAccessor 自动启用原子替换
ra, _ := fs.GetOrCreateRandomAccessor(fileID)
ra.Write(0, newData)
ra.Flush()  // 自动使用原子替换完成更新
```

#### 2. Journal 系统（日志式写入）

Journal 系统提供高效的增量写入和版本管理能力，特别适合频繁修改的文件。

**核心概念**:
- **增量写入**: 只记录修改的部分，而不是整个文件
- **COW (Copy-On-Write)**: 写入时复制，保持历史版本完整
- **智能合并**: 自动合并多个小的修改，减少存储开销

**Journal 类型**:

1. **小文件策略** (< 10MB)
   - 使用内存缓冲
   - 快速合并
   - 低延迟

2. **大文件策略** (>= 10MB)
   - 分块处理
   - 定期合并
   - 内存友好

**特性**:
- **自动版本创建**: 每次 Flush 自动创建新版本
- **智能快照**: 基于以下条件自动创建快照
  - Entry 数量达到阈值（默认 100）
  - 内存使用达到阈值（默认 10MB）
  - 时间间隔达到阈值（默认 5分钟）
- **内存限制**: 
  - 单个 Journal 最大内存：50MB
  - 全局最大内存：200MB
  - 超限自动刷新

**配置选项**:
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

#### 3. WAL (Write-Ahead Log) - 崩溃恢复

WAL 机制保证在系统崩溃时不丢失数据，提供持久化保证。

**核心功能**:
- **崩溃恢复**: 系统崩溃后自动恢复未提交的写入
- **持久化保证**: 写入操作先记录到 WAL，再应用到实际数据
- **自动快照**: 定期创建 Journal 快照到磁盘

**WAL 模式**:

1. **FULL 模式** (默认)
   ```
   写入 → WAL 持久化 → 返回成功
   - 最高安全性
   - 绝对不丢数据
   - 性能稍慢
   ```

2. **ASYNC 模式**
   ```
   写入 → 异步 WAL → 立即返回
   - 高性能
   - 极小概率丢失最后几次写入
   - 适合日志等场景
   ```

3. **NONE 模式**
   ```
   写入 → 内存 → 定期刷新
   - 最高性能
   - 崩溃可能丢失数据
   - 仅用于临时数据
   ```

**WAL 配置**:
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

**恢复示例**:
```go
// 系统启动时自动恢复
jm := vfs.NewJournalManager(fs, config)
// JournalManager 会自动从 WAL 快照恢复未提交的 Journal
```

#### 4. 版本保留策略 (Version Retention)

智能版本管理系统，自动清理旧版本，同时保持足够的历史记录用于回滚。

**保留策略**:

1. **时间窗口合并**
   - 在时间窗口内（默认 5 分钟）只保留最后一个完整版本
   - 减少频繁修改带来的版本膨胀

2. **最大版本数限制**
   - 总版本数限制（默认 10 个）
   - 超出限制自动删除最老版本

3. **最小完整版本保护**
   - 始终保留最少数量的完整版本（默认 3 个）
   - 保证随时可以回滚

4. **Journal 数量控制**
   - 每个基础版本最多保留的 Journal 数（默认 10 个）
   - 超出时自动合并 Journal 到新的基础版本

**配置示例**:
```go
policy := vfs.VersionRetentionPolicy{
    Enabled:                true,
    TimeWindowSeconds:      5 * 60,   // 5 minutes
    MaxVersions:            10,       // 最多 10 个版本
    MinFullVersions:        3,        // 至少 3 个完整版本
    MaxJournalsPerBase:     10,       // 每个 base 最多 10 个 journals
    CleanupIntervalSeconds: 5 * 60,   // 每 5 分钟清理一次
}

vrm := vfs.NewVersionRetentionManager(fs, policy)
defer vrm.Stop()
```

**Journal 合并策略**:

系统提供三种智能合并策略，根据文件大小和 Journal 数量自动选择:

1. **内存合并** (In-Memory Merge)
   - 适用: 小文件 (< 50MB)
   - 内存占用: = 文件大小
   - 性能: 最快

2. **流式合并** (Streaming Merge)
   - 适用: 大文件 (> 50MB)
   - 内存占用: 固定 ~10MB
   - 性能: 适中，内存安全
   - 支持 GB 级文件

3. **批处理合并** (Batch Merge)
   - 适用: 大量 Journals (> 5 个)
   - 分批处理，避免内存溢出
   - 渐进式合并

**合并配置**:
```go
mergeConfig := vfs.MergeConfig{
    MaxMemoryBuffer:     100 << 20,  // 100MB
    StreamChunkSize:     10 << 20,   // 10MB
    StreamingThreshold:  50 << 20,   // 50MB 触发流式
    MaxJournalsPerMerge: 5,          // 每批最多 5 个
}
```

**版本统计**:
```go
stats := vrm.GetRetentionStats()
fmt.Printf("清理次数: %d\n", stats["totalCleanups"])
fmt.Printf("删除版本: %d\n", stats["totalDeleted"])
fmt.Printf("最后清理: %d\n", stats["lastCleanup"])
```

### 配置支持

- **加密**：支持AES256和SM4加密
- **压缩**：支持智能压缩（根据文件类型自动决定是否压缩）
- **压缩算法**：支持Snappy、Zstd、Gzip、Brotli
- **秒传**：支持文件秒传功能（通过RefLevel配置）

## 使用方法

### 基本挂载

```go
package main

import (
    "context"
    "github.com/orcastor/orcas/core"
    "github.com/orcastor/orcas/vfs"
)

func main() {
    // 创建Handler
    h := core.NewLocalHandler()
    
    // 登录获取Context
    ctx, _, _, err := h.Login(context.Background(), "username", "password")
    if err != nil {
        panic(err)
    }
    
    // 配置选项（加密、压缩、秒传等）
    cfg := &core.Config{
        RefLevel:  core.REF_LEVEL_FULL,         // 秒传级别：FULL（完整文件秒传）
        CmprWay:   core.DATA_CMPR_GZIP,         // 压缩方式：Gzip（默认智能压缩）
        CmprQlty:  5,                           // 压缩级别：5
        EndecWay:  core.DATA_ENDEC_AES256,      // 加密方式：AES256
        EndecKey:  "your-encryption-key-here",   // 加密密钥（AES256需要>16字符）
    }
    
    // 挂载文件系统
    server, err := vfs.Mount(h, ctx, bucketID, &vfs.MountOptions{
        MountPoint: "/mnt/orcas",
        Foreground: true,
        AllowOther: false,
        Config:     cfg,  // 传入配置
    })
    if err != nil {
        panic(err)
    }
    
    // 运行服务
    vfs.Serve(server, true)
}
```

### 挂载选项

- `MountPoint`: 挂载点路径
- `Foreground`: 是否前台运行（true表示阻塞直到收到信号）
- `AllowOther`: 是否允许其他用户访问
- `AllowRoot`: 是否允许root访问
- `DefaultPermissions`: 使用默认权限检查
- `FuseOptions`: 自定义FUSE选项
- `Config`: 配置（用于加密、压缩、秒传等特性）
  - `RefLevel`: 秒传级别（REF_LEVEL_OFF/REF_LEVEL_FULL/REF_LEVEL_FAST）
  - `CmprWay`: 压缩方式（默认智能压缩，根据文件类型自动决定）
  - `CmprQlty`: 压缩级别
  - `EndecWay`: 加密方式（AES256/SM4）
  - `EndecKey`: 加密密钥

## 实现细节

### 文件系统结构

- **根节点** (`/`): 映射到 ORCAS 的 ROOT_OID
- **目录节点**: 映射到 `OBJ_TYPE_DIR` 类型的对象
- **文件节点**: 映射到 `OBJ_TYPE_FILE` 类型的对象

### 文件读写

- **读取**: 直接按chunk读取、解密、解压数据，不使用SDK的DataReader
  - 支持随机访问读取（通过偏移量）
  - 自动处理打包文件（如果文件被打包）
  - 自动处理加密数据的解密
  - 自动处理压缩数据的解压缩
- **写入**: 使用 `RandomAccessor` 进行随机写入，支持COW（Copy-On-Write）
  - 支持随机写入（可以在任意位置写入数据）
  - 写入缓冲区，延迟刷新以提高性能
  - 自动版本管理（每次Flush创建新版本）

### 缓存机制

- 对象信息缓存：每个节点缓存对象信息，减少数据库查询
- 缓存失效：在创建、删除、重命名等操作后自动失效缓存

## 注意事项

1. **权限**: 需要确保有足够的权限创建挂载点和挂载文件系统
2. **性能**: 文件系统操作会转换为ORCAS API调用，可能比本地文件系统慢
3. **并发**: 支持多线程并发访问，但需要注意ORCAS Handler的线程安全性

## 性能测试

### 运行基准测试

```bash
# 运行所有基准测试
go test -bench=BenchmarkRandomAccessor -benchmem ./vfs

# 运行特定测试
go test -bench=BenchmarkRandomAccessor_Read_1MB -benchmem ./vfs

# 运行并生成性能分析报告
go test -bench=BenchmarkRandomAccessor -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof ./vfs
```

### 性能测试用例

基准测试文件 `random_access_bench_test.go` 包含以下测试：

1. **写入性能测试**
   - 不同数据大小：1KB、10KB、100KB、1MB
   - 写入并刷新性能
   - 多次写入性能

2. **读取性能测试**
   - 不同数据大小：1KB、10KB、100KB、1MB
   - 压缩数据读取
   - 加密数据读取
   - 压缩加密数据读取

3. **随机访问性能测试**
   - 随机偏移读取
   - 精确大小读取（验证不超过请求大小）
   - 流式读取性能

4. **缓冲区操作性能测试**
   - 带缓冲区写入的读取性能

### 性能优化特性

- **精确大小控制**：读取数据不超过请求的 size，减少内存占用
- **流式处理**：按 chunk 流式读取，边读边处理，避免一次性加载全部数据
- **内存优化**：使用对象池重用缓冲区，减少 GC 压力
- **并发优化**：使用原子操作减少锁竞争

## 环境信息

### 系统要求

- **操作系统**：
  - Linux（推荐）：支持完整 FUSE 功能
  - macOS：支持完整 FUSE 功能（需要安装 macOSFUSE）
  - Windows：
    - 当前：仅支持 RandomAccessor API（程序化访问）
    - 未来：计划支持 Dokany 文件系统挂载（需要安装 Dokany 驱动）

- **Go 版本**：Go 1.18 或更高版本

- **依赖项**：
  - `github.com/hanwen/go-fuse/v2`：FUSE 库（仅 Linux/macOS）
  - `github.com/orcastor/orcas/core`：ORCAS 核心库

### 环境变量

- `BasePath`：基础路径，用于存储元数据（数据库存储位置），默认为当前目录（.）
- `DataPath`：数据路径，用于存储文件数据，默认为当前目录（.）

如果未设置，测试时会自动使用临时目录。

### macOS FUSE 安装

在 macOS 上使用 FUSE 功能，需要安装 macOSFUSE：

```bash
# 使用 Homebrew 安装
brew install --cask macfuse

# 或使用官方安装包
# 下载地址：https://github.com/osxfuse/osxfuse/releases
```

安装后需要重启系统或重新登录。

### Windows Dokany 安装

在 Windows 上使用文件系统挂载功能，需要安装 Dokany：

1. **下载并安装 Dokany 驱动**：
   - 访问 [Dokany 官方 GitHub](https://github.com/dokan-dev/dokany/releases)
   - 下载最新版本的安装包（DokanSetup_*.exe）
   - 运行安装程序并重启系统

2. **使用示例**：
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
       
       // 运行服务
       vfs.Serve(instance, true)
   }
   ```

3. **注意事项**：
   - Dokany 需要管理员权限进行安装
   - 安装后需要重启系统
   - 不同版本的 Windows 可能存在兼容性差异，建议在 Windows 10/11 上使用

### 测试环境

测试和基准测试使用以下环境：

- **临时目录**：使用系统临时目录（`os.TempDir()`）
- **数据库**：每次测试创建独立的数据库实例
- **数据隔离**：每个测试使用独立的 bucket ID

## 依赖

- `github.com/hanwen/go-fuse/v2`: FUSE库（仅 Linux/macOS）

## 限制

- 当前实现为简化版本，某些高级特性（如符号链接、硬链接）尚未支持
- 文件截断操作需要完善
- 大文件的分片读取需要优化
- Windows 平台支持 RandomAccessor API 和 Dokany 文件系统挂载
  - Dokany 挂载需要安装 Dokany 驱动程序

## Windows 平台支持

### 当前状态
- ✅ 支持 `RandomAccessor` API（程序化访问）
- ✅ 支持 Dokany 文件系统挂载

### 实现细节

**Dokany 集成**：
- 运行时动态加载 `dokan2.dll`
- 实现完整的文件系统操作：
  - CreateFile, ReadFile, WriteFile
  - GetFileInformation, FindFiles
  - DeleteFile, DeleteDirectory
  - MoveFile（重命名/移动）
  - SetFileAttributes, SetFileTime
  - 以及更多...

**优势**：
- 类似 FUSE 的用户空间文件系统实现
- 无需编写内核驱动
- 支持完整的文件系统操作

**注意事项**：
- Dokany 需要管理员权限进行安装
- 不同 Windows 版本可能存在兼容性差异
- 已在 Windows 10/11 上测试

## 相关文档

### 性能优化文档

- [性能优化最终报告 (中文)](PERFORMANCE_OPTIMIZATION_FINAL.zh.md)

### 其他文档

- [挂载指南 (中文)](MOUNT_GUIDE.zh.md) - 完整挂载教程

