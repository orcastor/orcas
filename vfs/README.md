# ORCAS VFS - FUSE文件系统

ORCAS VFS 使用 FUSE (Filesystem in Userspace) 技术，将 ORCAS 对象存储系统映射为本地文件系统，允许像操作普通文件系统一样操作 ORCAS 存储。

## 平台支持

**注意**: FUSE 是 Unix/Linux 系统特有的功能，**不支持 Windows**。此功能只能在 Linux、macOS 等 Unix-like 系统上使用。

在 Windows 上编译或运行此代码会失败，因为 `go-fuse` 库依赖 Unix 系统调用。

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

- **读取**: 使用 SDK 的 `NewDataReader` 读取文件数据，自动处理解密和解压缩
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

## 依赖

- `github.com/hanwen/go-fuse/v2`: FUSE库

## 限制

- 当前实现为简化版本，某些高级特性（如符号链接、硬链接）尚未支持
- 文件截断操作需要完善
- 大文件的分片读取需要优化

