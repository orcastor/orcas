<p align="center">
  <a href="https://orcastor.github.io/doc/">
    <img src="https://orcastor.github.io/doc/logo.svg">
  </a>
</p>

<p align="center"><strong>OrcaS：开放可信赖内容寻址存储</strong></p>
<p align="center"><em>一款开箱即用的轻量级对象存储系统</em></p>

<p align="center">
  <a href="/go.mod#L3" alt="go version">
    <img src="https://img.shields.io/badge/go%20version-%3E=1.18-brightgreen?style=flat"/>
  </a>
  <a href="https://goreportcard.com/badge/github.com/orcastor/orcas" alt="goreport">
    <img src="https://goreportcard.com/badge/github.com/orcastor/orcas?v=20220901">
  </a>
  <a href="https://app.fossa.com/projects/git%2Bgithub.com%2Forcastor%2Forcas?ref=badge_shield" alt="FOSSA Status">
    <img src="https://app.fossa.com/api/projects/git%2Bgithub.com%2Forcastor%2Forcas.svg?type=shield"/>
  </a>
  <a href='https://orca-zhang.semaphoreci.com/branches/733643c4-54d7-4cc4-9b1e-c3c77c8ff7db' alt='Build Status'>
    <img src='https://orca-zhang.semaphoreci.com/badges/orcas/branches/master.svg?style=shields&key=78c39699-2da0-4322-9372-0839f9dcde76'>
  </a>
  <a href="https://github.com/orcastor/orcas/blob/master/LICENSE" alt='MIT license'>
    <img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat">
  </a>
  <a href="https://orcastor.github.io/doc/" alt='docs'>
    <img src="https://img.shields.io/badge/docs-master-blue.svg?style=flat">
  </a>
</p>

- [English](README.md) | [中文](README.zh.md)

## 🚀 什么是 OrcaS？

**OrcaS**（开放可信赖内容寻址存储，Open Reliable Content Addressable Storage）是一个**轻量级、高性能的对象存储系统**，以**内容寻址存储（CAS）**为核心构建。它提供了企业级功能，如即时去重、多版本管理、零知识加密和智能压缩——所有这些都集成在一个可立即部署的二进制文件中。

### 为什么选择 OrcaS？

- 🌐 **开放**：开源（MIT 许可证），透明，社区驱动开发
- 🛡️ **可信赖**：内容寻址存储确保数据完整性和自动去重
- 🎯 **内容寻址存储**：数据通过内容哈希存储，实现自动去重和完整性验证
- ⚡ **秒传（去重）**：文件秒级上传，而非分钟级——相同文件可即时检测，无需上传
- 🔒 **零知识加密**：您的数据，您的密钥——采用行业标准算法的端到端加密
- 📦 **生产就绪**：S3 兼容 API、VFS 挂载支持和完善的文档
- 🚀 **高性能**：针对大小文件优化，采用智能打包和分片

## ✨ 核心特性

### ⏱ 秒传（对象级去重）

**功能说明**：相同文件可瞬间上传，无需传输数据。

**工作原理**：
- 为每个文件计算多个校验和（XXH3、SHA-256）
- 上传前检查是否已存在相同内容
- 如果找到，创建对现有数据的引用，而非重新上传
- **结果**：重复文件的上传时间从分钟级降至毫秒级

**应用场景**：
- 备份系统（多个备份中的相同文件）
- 版本控制系统（不同版本中的相似文件）
- 多用户环境（共享文件）
- CDN 边缘存储（缓存内容）

**优势**：
- 🚀 **99%+ 更快**的重复文件上传速度
- 💾 **巨大的存储节省**——存储 1 份，引用 N 次
- ⚡ **带宽节省**——无冗余数据传输
- 🔍 **自动完整性验证**——内容哈希确保数据正确性

![去重优势](assets/deduplication-benefits.png)

### 📦 小对象打包

**功能说明**：高效地将多个小文件打包存储。

**工作原理**：
- 将小文件（< 64KB）分组打包
- 减少元数据开销和 I/O 操作
- 在优化存储的同时保持单个文件访问

**优势**：
- 📈 **10 倍以上性能提升**的小文件操作
- 💰 **降低存储成本**——更少的元数据开销
- ⚡ **更快的操作**——批量元数据写入

### 🔪 大对象分片

**功能说明**：将大文件分割为可管理的块。

**工作原理**：
- 自动将大于配置阈值（默认 10MB）的文件分片
- 每个分片独立存储，拥有自己的校验和
- 支持并行上传/下载和高效更新

**优势**：
- 🔄 **并行处理**——并发上传/下载分片
- 🛡️ **可恢复传输**——独立重试失败的分片
- ✏️ **高效更新**——只需重新上传修改的分片
- 📊 **更好的资源利用**——高效处理大文件

### 🗂 对象多版本管理

**功能说明**：自动维护文件版本历史。

**工作原理**：
- 每次文件修改创建新版本
- 自动保留旧版本
- 可配置的保留策略
- 通过内容去重实现空间高效

**优势**：
- 🔙 **时间点恢复**——恢复任何历史版本
- 🛡️ **数据保护**——意外删除可恢复
- 📚 **审计追踪**——跟踪所有历史变更
- 💾 **空间高效**——未更改的数据在版本间共享

### 🔐 零知识加密

**功能说明**：端到端加密，只有您持有密钥。

**工作原理**：
- AES-256 加密（行业标准）
- 加密密钥永不离开您的控制
- 可选的每桶加密密钥
- 透明的加密/解密

**优势**：
- 🔒 **最高安全性**——即使存储管理员也无法读取您的数据
- ✅ **合规就绪**——满足严格的安全要求
- 🛡️ **数据隐私**——您的数据，您的控制
- 🌍 **国际标准**——AES-256 加密

### 🗜 智能压缩

**功能说明**：自动压缩数据以节省空间。

**工作原理**：
- 可配置的压缩算法（zstd、gzip 等）
- 在加密前应用压缩
- 自动检测已压缩数据
- 每桶压缩设置

**优势**：
- 💾 **存储节省**——通常减少 30-70%
- ⚡ **带宽节省**——传输数据更少
- 🎯 **智能默认值**——开箱即用
- ⚙️ **可配置**——根据需求调整

## 🏗️ 架构与设计

### 内容寻址存储（CAS）核心

OrcaS 基于**内容寻址存储**原则构建，数据通过内容哈希而非位置进行存储和检索。

![内容寻址存储架构](assets/cas-architecture.png)

**CAS 的核心优势**：
1. **自动去重**：相同内容只存储一次，可多次引用
2. **完整性验证**：内容哈希确保数据未被损坏
3. **高效版本管理**：新版本仅存储更改的内容
4. **简化备份**：相同内容 = 相同哈希 = 无需重新上传

### 系统架构

![系统架构](assets/system-architecture.png)

### 秒传流程

![秒传流程](assets/instant-upload-flow.png)

### 数据存储结构

```
存储布局:
├── 元数据 (SQLite)
│   ├── 对象 (文件, 目录)
│   ├── 数据信息 (内容元数据)
│   ├── 版本 (版本历史)
│   └── 引用 (去重)
│
└── 数据块 (文件系统)
    └── <桶ID>/
        └── <哈希前缀>/
            └── <哈希>/
                └── <数据ID>_<分片编号>
```

## 📊 性能亮点

- **秒传**：重复文件上传速度提升 99%+（毫秒级 vs 分钟级）
- **小文件**：打包后性能提升 10 倍以上
- **大文件**：并行分片处理，实现最优吞吐量
- **存储效率**：压缩 + 去重节省 30-70% 空间
- **并发操作**：针对高并发优化

## 🚀 快速开始

```bash
# 构建
go build -o orcas ./cmd

# 运行
./orcas

# 使用 S3 API
aws --endpoint-url=http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url=http://localhost:9000 s3 cp file.txt s3://my-bucket/

# 挂载为文件系统 (Linux/Mac)
./mount_vfs.sh
```

## 📚 文档

- [完整文档](https://orcastor.github.io/doc/)
- [快速开始指南](QUICK_START_MOUNT.md)
- [S3 API 文档](s3/README.md)
- [VFS 挂载指南](MOUNT_README.md)

## 🤝 贡献

欢迎贡献！请随时提交 Pull Request。

## 📄 许可证

MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## ⭐ 为什么给这个项目 Star？

- 🎯 **生产就绪**：经过实战测试，积极维护
- 🚀 **高性能**：针对真实工作负载优化
- 🔒 **安全优先**：内置零知识加密
- 💾 **存储高效**：自动去重节省空间和成本
- 🛠️ **易于使用**：S3 兼容 API、VFS 挂载、完善文档
- 🌟 **创新性**：内容寻址存储与即时去重
- 📈 **积极开发**：定期更新和改进
- 🤝 **开源**：MIT 许可，社区驱动

**如果您觉得这个项目有用，请给我们一个 Star！** ⭐

---

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Forcastor%2Forcas.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Forcastor%2Forcas?ref=badge_large)
