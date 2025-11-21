# ORCAS VFS 挂载说明

## 快速开始

### 方法1: 使用挂载脚本（推荐）

```bash
cd /DATA/orcas
./mount_vfs.sh
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
  -wisecmpr ZSTD \
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
  "wise_cmpr": "ZSTD",
  "cmpr_qlty": 5
}
```

然后运行:

```bash
./main -action mount -mountpoint /DATA/orcas_test -config config.json
```

## 参数说明

### 必需参数

- `-action mount`: 指定操作为挂载
- `-mountpoint <路径>`: 挂载点路径（例如: `/DATA/orcas_test`）
- `-user <用户名>`: ORCAS 用户名
- `-pass <密码>`: ORCAS 密码

### 加密参数

- `-endecway`: 加密方式
  - `AES256`: AES-256 加密（推荐，密钥长度需 >16 字符）
  - `SM4`: SM4 加密（密钥长度需 =16 字符）
- `-endeckey`: 加密密钥
  - AES256: 长度必须 >16 字符
  - SM4: 长度必须 =16 字符

### 压缩参数

- `-wisecmpr`: 智能压缩算法
  - `SNAPPY`: Snappy 压缩（快速）
  - `ZSTD`: Zstd 压缩（推荐，平衡速度和压缩率）
  - `GZIP`: Gzip 压缩（兼容性好）
  - `BR`: Brotli 压缩（高压缩率）
- `-cmprqlty`: 压缩级别（0-11，默认5）

### 其他参数

- `-config <文件路径>`: 配置文件路径（JSON格式）
- `-bucket <ID>`: 指定Bucket ID（默认使用第一个Bucket）
- `-datasync true/false`: 是否启用数据同步（断电保护）

## 默认配置

如果不指定加密和压缩参数，程序会使用以下默认值：

- **加密**: AES256（如果未提供密钥，会使用默认密钥，**不推荐用于生产环境**）
- **智能压缩**: ZSTD，级别 5

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

## 示例

### 完整示例（启用加密和智能压缩）

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -endecway AES256 \
  -endeckey "my-secure-encryption-key-12345678901234567890" \
  -wisecmpr ZSTD \
  -cmprqlty 5
```

### 仅启用智能压缩（不加密）

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -wisecmpr ZSTD \
  -cmprqlty 5
```

### 仅启用加密（不压缩）

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -endecway AES256 \
  -endeckey "my-secure-encryption-key-12345678901234567890"
```

