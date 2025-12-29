# SQLCipher 快速开始指南

## 快速构建（macOS）

### 方法 1: 使用构建脚本（推荐）

```bash
cd core
./build_sqlcipher_macos.sh
```

脚本会自动：
1. 检查并安装 OpenSSL（如果需要）
2. 克隆/更新 SQLCipher 源码
3. 编译 SQLCipher
4. 复制库文件到 `core/` 目录

### 方法 2: 使用 Homebrew（最简单）

```bash
# 安装 SQLCipher
brew install sqlcipher

# 修改 core/sqlcipher_build.go 使用 pkg-config:
# /*
# #cgo pkg-config: sqlcipher
# #cgo CFLAGS: -DSQLITE_HAS_CODEC
# */
```

### 方法 3: 手动构建

参考 `building.md` 中的详细步骤。

## 构建项目

```bash
cd /Users/orca/Documents/GitHub/orcas
CGO_ENABLED=1 go build -tags sqlcipher ./cmd
```

## 使用加密数据库

代码中已经支持加密，只需在连接字符串中添加 `key` 参数：

```go
// 创建加密数据库
db, err := sql.Open("sqlite3", "path/to/db.db?key=your-encryption-key")

// 使用现有的加密数据库
db, err := sql.Open("sqlite3", "path/to/encrypted.db?key=your-encryption-key")
```

## 验证

检查二进制文件是否链接了 SQLCipher：

```bash
# macOS
otool -L ./server | grep sqlcipher

# Linux
ldd ./server | grep sqlcipher
```

## 常见问题

### 找不到 libsqlcipher

- 确保库文件在 `core/` 目录
- 检查 `sqlcipher_build.go` 中的路径配置
- macOS 使用 `.a` 或 `.dylib`，Linux 使用 `.so`

### 链接错误

- 确保 OpenSSL 已正确安装
- 检查 CGO 是否启用：`export CGO_ENABLED=1`

### 运行时错误

- 确保运行时能找到库文件
- 或使用系统安装的 SQLCipher（Homebrew）

## 在依赖项目中使用 SQLCipher

如果你的项目依赖 `github.com/orcastor/orcas`，请参考 `SQLCIPHER_DEPENDENCY.md` 了解如何在你的项目中启用 SQLCipher。

**关键点**：
- 在你的项目构建时使用 `-tags sqlcipher`
- 确保 `CGO_ENABLED=1`
- 确保 SQLCipher 库文件在 orcas 项目的 `core/` 目录中

```bash
# 在你的项目中构建（启用 SQLCipher）
CGO_ENABLED=1 go build -tags sqlcipher -o your-app ./cmd
```

## 更多信息

- 详细构建文档：`building.md`
- 依赖项目使用指南：`SQLCIPHER_DEPENDENCY.md`

