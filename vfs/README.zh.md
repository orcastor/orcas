# ORCAS VFS - FUSE文件系统

ORCAS VFS 使用 FUSE (Filesystem in Userspace) 技术，将 ORCAS 对象存储系统映射为本地文件系统，允许像操作普通文件系统一样操作 ORCAS 存储。

## 平台支持

- **Linux/macOS**: 使用 FUSE (Filesystem in Userspace) 技术，支持完整文件系统挂载功能
- **Windows**: 
  - 当前版本：仅支持 `RandomAccessor` API，不支持文件系统挂载
  - 未来计划：考虑使用 [Dokany](https://github.com/dokan-dev/dokany) 实现 Windows 平台的文件系统挂载功能
    - Dokany 是 Windows 上的 FUSE 替代方案，允许在用户空间创建自定义文件系统
    - 需要安装 Dokany 驱动程序和 Go 绑定库

## 功能特性

- **目录和文件操作**：支持创建、删除、重命名文件和目录
- **文件读写**：支持随机读写文件内容
- **目录遍历**：支持列出目录内容
- **属性管理**：支持获取和设置文件属性（大小、修改时间等）
- **SDK集成**：支持使用SDK的加密、压缩、秒传等特性
  - **加密**：支持AES256和SM4加密
  - **压缩**：支持智能压缩（根据文件类型自动决定是否压缩）
  - **压缩算法**：支持Snappy、Zstd、Gzip、Brotli
  - **秒传**：支持文件秒传功能（通过RefLevel配置）
  - **数据同步**：支持断电保护策略（DataSync）

## 使用方法

### 基本挂载

```go
package main

import (
    "context"
    "github.com/orcastor/orcas/core"
    "github.com/orcastor/orcas/sdk"
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
    
    // 配置SDK选项（加密、压缩、秒传等）
    sdkCfg := &sdk.Config{
        RefLevel:  sdk.FULL,                    // 秒传级别：FULL（完整文件秒传）
        WiseCmpr:  core.DATA_CMPR_GZIP,         // 智能压缩：Gzip
        CmprQlty:  5,                           // 压缩级别：5
        EndecWay:  core.DATA_ENDEC_AES256,      // 加密方式：AES256
        EndecKey:  "your-encryption-key-here",   // 加密密钥（AES256需要>16字符）
        DataSync:  true,                        // 断电保护：每次写入后刷盘
    }
    
    // 挂载文件系统
    server, err := vfs.Mount(h, ctx, bucketID, &vfs.MountOptions{
        MountPoint: "/mnt/orcas",
        Foreground: true,
        AllowOther: false,
        SDKConfig:  sdkCfg,  // 传入SDK配置
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
- `SDKConfig`: SDK配置（用于加密、压缩、秒传等特性）
  - `RefLevel`: 秒传级别（OFF/FULL/FAST）
  - `WiseCmpr`: 智能压缩（根据文件类型自动决定）
  - `CmprQlty`: 压缩级别
  - `EndecWay`: 加密方式（AES256/SM4）
  - `EndecKey`: 加密密钥
  - `DataSync`: 断电保护策略

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
  - **注意**: 随机写入目前暂不支持SDK的压缩和加密（因为压缩算法需要看到完整文件）
  - 如果需要压缩和加密，建议使用完整文件上传（通过SDK的Upload方法）

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
  - `github.com/orcastor/orcas/sdk`：ORCAS SDK
  - **Windows 挂载（未来）**：需要 Dokany 驱动程序和 Go 绑定库

### 环境变量

- `ORCAS_BASE`：ORCAS 基础目录（数据库存储位置）
- `ORCAS_DATA`：ORCAS 数据目录（文件数据存储位置）

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

### Windows Dokany 安装（未来支持）

在 Windows 上使用文件系统挂载功能，需要安装 Dokany：

1. **下载并安装 Dokany 驱动**：
   - 访问 [Dokany 官方 GitHub](https://github.com/dokan-dev/dokany/releases)
   - 下载最新版本的安装包（DokanSetup_*.exe）
   - 运行安装程序并重启系统

2. **安装 Go 绑定库**（待开发）：
   ```bash
   # 等待 Dokany Go 绑定库可用
   # go get github.com/dokan-dev/dokany-go
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
- Windows 平台当前仅支持 RandomAccessor API，不支持文件系统挂载
  - 未来计划通过 Dokany 实现 Windows 平台的文件系统挂载功能

## Windows 平台支持计划

### 当前状态
- ✅ 支持 `RandomAccessor` API（程序化访问）
- ❌ 不支持文件系统挂载

### 未来计划
考虑使用 [Dokany](https://github.com/dokan-dev/dokany) 实现 Windows 平台的文件系统挂载：

**优势**：
- 类似 FUSE 的用户空间文件系统实现
- 无需编写内核驱动
- 支持完整的文件系统操作

**实现要求**：
1. 安装 Dokany 驱动程序
2. 使用或开发 Dokany 的 Go 绑定库
3. 实现 Dokany 的文件系统接口（类似 FUSE 接口）

**注意事项**：
- Dokany 需要管理员权限安装
- 不同 Windows 版本可能存在兼容性差异
- 需要测试与常用软件的兼容性（如 Office、WPS 等）

