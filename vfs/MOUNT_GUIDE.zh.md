# ORCAS VFS 挂载指南

本指南介绍如何使用 OrcaS VFS（虚拟文件系统）功能，将 OrcaS 对象存储挂载为本地文件系统。

- [English](MOUNT_GUIDE.md) | [中文](MOUNT_GUIDE.zh.md)

## 目录

- [快速开始](#快速开始)
- [创建桶](#创建桶)
- [挂载文件系统](#挂载文件系统)
- [无主数据库模式](#无主数据库模式)
- [参数说明](#参数说明)
- [卸载](#卸载)
- [常见问题](#常见问题)

## 快速开始

### 方法1: 使用挂载脚本（推荐）

```bash
cd /DATA/orcas
./vfs/mount_vfs.sh
```

脚本会提示输入用户名和密码，然后自动挂载到 `/DATA/orcas_test`。

### 方法2: 直接使用命令行

```bash
cd /DATA/orcas

# 编译（如果还没有编译）
go build -o main ./cmd/main.go

# 挂载（需要输入用户名和密码）
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user <用户名> \
  -pass <密码> \
  -endecway AES256 \
  -endeckey "your-encryption-key-here-must-be-longer-than-16-chars" \
  -cmprway ZSTD \
  -cmprqlty 5
```

### 方法3: 使用配置文件

创建配置文件 `config.json`:

```json
{
  "user_name": "your_username",
  "password": "your_password",
  "endec_way": "AES256",
  "endec_key": "your-encryption-key-here-must-be-longer-than-16-chars",
  "cmpr_way": "ZSTD",
  "cmpr_qlty": 5
}
```

然后运行:

```bash
./main -action mount -mountpoint /DATA/orcas_test -config config.json
```

### 完整示例（加密 + 智能压缩）

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -endecway AES256 \
  -endeckey "orcas-encryption-key-12345678901234567890" \
  -cmprway ZSTD \
  -cmprqlty 5
```

### 默认配置

如果未指定加密和压缩，程序会自动启用默认值：

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas
```

默认配置：
- **加密**: AES256（使用默认密钥，**不推荐用于生产环境**）
- **智能压缩**: ZSTD，级别 5

## 创建桶

在挂载文件系统之前，您需要先创建一个桶（Bucket）。桶是存储数据的基本容器，每个桶可以配置独立的加密、压缩和配额设置。

### 创建桶（需要管理员权限）

```bash
cd /DATA/orcas

# 编译（如果还没有编译）
go build -o main ./cmd/main.go

# 创建桶（基本用法）
./main -action create-bucket \
  -user <管理员用户名> \
  -pass <管理员密码> \
  -bucketname <桶名称>
```

### 创建桶参数说明

#### 必需参数

- `-action create-bucket`: 指定操作为创建桶
- `-bucketname <名称>`: 桶名称
- `-user <用户名>`: 管理员用户名（需要 ADMIN 权限）
- `-pass <密码>`: 管理员密码

#### 可选参数

- `-quota <字节数>`: 桶配额（字节数，`-1` 表示无限制）
  - 例如：`-quota 1073741824` 表示 1GB
  - 例如：`-quota -1` 表示无限制（默认）
- `-owner <用户名或用户ID>`: 桶所有者（默认是当前用户）
  - 可以是用户名或用户ID
  - 例如：`-owner user1` 或 `-owner 12345`
- `-cmprway <算法>`: 压缩方式（默认智能压缩）
  - `SNAPPY`: Snappy 压缩（快速）
  - `ZSTD`: Zstd 压缩（推荐，平衡速度和压缩率）
  - `GZIP`: Gzip 压缩（兼容性好）
  - `BR`: Brotli 压缩（高压缩率）
- `-cmprqlty <级别>`: 压缩级别（0-11，默认5）
- `-endecway <方式>`: 加密方式
  - `AES256`: AES-256 加密（推荐，密钥长度需 >16 字符）
  - `SM4`: SM4 加密（密钥长度需 =16 字符）
- `-endeckey <密钥>`: 加密密钥
  - AES256: 长度必须 >16 字符
  - SM4: 长度必须 =16 字符
- `-reflevel <级别>`: 即时上传（去重）级别
  - `FULL`: 完整去重（推荐）
  - `FAST`: 快速去重
  - `OFF`: 关闭去重

### 创建桶示例

#### 基本桶（无加密和压缩）

```bash
./main -action create-bucket \
  -user orcas \
  -pass orcas \
  -bucketname my-bucket
```

#### 带配额的桶

```bash
./main -action create-bucket \
  -user orcas \
  -pass orcas \
  -bucketname my-bucket \
  -quota 10737418240
```

（创建 10GB 配额的桶）

#### 完整配置的桶（加密+压缩+去重）

```bash
./main -action create-bucket \
  -user orcas \
  -pass orcas \
  -bucketname secure-bucket \
  -quota -1 \
  -endecway AES256 \
  -endeckey "my-secure-encryption-key-12345678901234567890" \
  -cmprway ZSTD \
  -cmprqlty 5 \
  -reflevel FULL
```

#### 为指定用户创建桶

```bash
./main -action create-bucket \
  -user orcas \
  -pass orcas \
  -bucketname user-bucket \
  -owner user1
```

### 查看桶列表

```bash
./main -action list-buckets \
  -user <用户名> \
  -pass <密码>
```

### 更新桶配置

```bash
./main -action update-bucket \
  -user <管理员用户名> \
  -pass <管理员密码> \
  -bucket <桶ID> \
  -cmprway ZSTD \
  -cmprqlty 6
```

### 删除桶

```bash
./main -action delete-bucket \
  -user <管理员用户名> \
  -pass <管理员密码> \
  -bucket <桶ID>
```

## 挂载文件系统

### 参数说明

#### 必需参数

- `-action mount`: 指定操作为挂载
- `-mountpoint <路径>`: 挂载点路径（例如: `/DATA/orcas_test`）
- `-user <用户名>`: ORCAS 用户名（使用 `-noauth` 时可省略）
- `-pass <密码>`: ORCAS 密码（使用 `-noauth` 时可省略）

#### 加密参数

- `-endecway`: 加密方式
  - `AES256`: AES-256 加密（推荐，密钥长度需 >16 字符）
  - `SM4`: SM4 加密（密钥长度需 =16 字符）
- `-endeckey`: 加密密钥
  - AES256: 长度必须 >16 字符
  - SM4: 长度必须 =16 字符

#### 压缩参数

- `-cmprway`: 压缩方式（默认智能压缩）
  - `SNAPPY`: Snappy 压缩（快速）
  - `ZSTD`: Zstd 压缩（推荐，平衡速度和压缩率）
  - `GZIP`: Gzip 压缩（兼容性好）
  - `BR`: Brotli 压缩（高压缩率）
- `-cmprqlty`: 压缩级别（0-11，默认5）

#### 其他参数

- `-config <文件路径>`: 配置文件路径（JSON格式）
- `-bucket <ID>`: 指定Bucket ID（默认使用第一个Bucket）
- `-noauth`: 无认证模式（不需要主数据库，见下方说明）
- `-debug`: 启用调试模式（详细输出）
- `-requirekey`: 要求 KEY 在上下文中，否则返回 EPERM 错误

### 挂载示例

#### 仅启用智能压缩（不加密）

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -cmprway ZSTD \
  -cmprqlty 5
```

#### 仅启用加密（不压缩）

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -endecway AES256 \
  -endeckey "my-secure-encryption-key-12345678901234567890"
```

## 无主数据库模式

OrcaS 支持完全无主数据库的运行模式，适用于不需要用户管理和 ACL 权限控制的场景。

### 配置方法

**无主数据库模式：**
```bash
# 不设置 ORCAS_BASE（或设置为空）
# export ORCAS_BASE=""  # 可选，不设置即可

# 必须设置 ORCAS_DATA（bucket 数据库存储位置）
export ORCAS_DATA=/var/orcas/data
```

### 使用示例

```bash
# 挂载文件系统，使用 -noauth 参数，不需要主数据库
./main -action mount \
  -noauth \
  -bucket 123456789 \
  -mountpoint /mnt/orcas \
  -endeckey "your-encryption-key" \
  -cmprway ZSTD \
  -cmprqlty 3

# 现在可以像普通文件系统一样使用
echo "Hello, World!" > /mnt/orcas/test.txt
cat /mnt/orcas/test.txt
```

**注意事项：**
- 使用 `-noauth` 时必须指定 `-bucket <桶ID>`
- 不需要设置 `ORCAS_BASE` 环境变量
- 不需要用户认证，直接使用 bucket ID

更多详细信息请参考：[无主数据库模式指南](../docs/NO_BASE_DB_ANALYSIS.md)

## 卸载

按 `Ctrl+C` 停止挂载，或使用系统命令：

```bash
sudo fusermount -u /DATA/orcas_test
```

## 注意事项

1. **权限**: 挂载可能需要 root 权限，或确保用户有权限访问挂载点
2. **加密密钥**: 生产环境请使用强密钥，不要使用默认密钥
3. **挂载点**: 确保挂载点目录存在且为空（或可接受挂载）
4. **FUSE**: 需要系统支持 FUSE（Linux/macOS）
5. **AES256 加密密钥长度必须 >16 字符**
6. **SM4 加密密钥长度必须 =16 字符**

## 常见问题

### Q: 挂载失败，提示权限错误？

A: 确保挂载点目录存在且有写权限：
```bash
sudo mkdir -p /DATA/orcas_test
sudo chmod 755 /DATA/orcas_test
```

### Q: 如何查看挂载状态？

A: 使用系统命令：
```bash
mount | grep orcas
# 或
df -h | grep orcas
```

### Q: 如何设置开机自动挂载？

A: 可以将挂载命令添加到 `/etc/fstab` 或使用 systemd service。注意：OrcaS VFS 不支持直接添加到 fstab，建议使用 systemd service。

### Q: 无主数据库模式下如何管理 bucket？

A: 在无主数据库模式下，需要手动管理 bucket ID。建议：
- 使用配置文件存储 bucket ID
- 使用环境变量存储 bucket ID
- 在应用程序中硬编码 bucket ID

### Q: 如何备份和恢复？

A: 
- **备份**: 备份 `ORCAS_DATA` 目录（包含所有 bucket 数据库和数据文件）
- **恢复**: 恢复 `ORCAS_DATA` 目录到新位置，设置 `ORCAS_DATA` 环境变量指向新位置

## 相关文档

- [无主数据库模式指南](../docs/NO_BASE_DB_ANALYSIS.md) - 了解如何不使用主数据库
- [VFS 性能优化报告](PERFORMANCE_OPTIMIZATION_FINAL.md) - VFS 性能优化详情
- [架构重构提案](../docs/ARCHITECTURE_REFACTOR_PROPOSAL.md) - 了解数据库架构设计

