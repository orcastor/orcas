# 快速挂载指南

## 快速开始

### 1. 编译程序（如果还没有编译）

```bash
cd /DATA/orcas
go build -o main ./cmd/main.go
```

### 2. 挂载到 /DATA/orcas_test（启用加密和智能压缩）

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user <你的用户名> \
  -pass <你的密码> \
  -endecway AES256 \
  -endeckey "your-encryption-key-must-be-longer-than-16-chars" \
  -cmprway ZSTD \
  -cmprqlty 5
```

### 3. 或者使用脚本

```bash
./mount_vfs.sh
```

## 示例命令

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

### 如果未指定加密和压缩，程序会自动启用默认值

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas
```

默认配置：
- 加密: AES256（使用默认密钥，**不推荐用于生产环境**）
- 智能压缩: ZSTD，级别 5

## 卸载

按 `Ctrl+C` 停止挂载，或使用系统命令：

```bash
sudo fusermount -u /DATA/orcas_test
```

## 注意事项

1. 确保挂载点目录存在：`sudo mkdir -p /DATA/orcas_test`
2. AES256 加密密钥长度必须 >16 字符
3. SM4 加密密钥长度必须 =16 字符
4. 挂载需要 FUSE 支持（Linux/macOS）

