# Building with SQLCipher

SQLCipher 是 SQLite 的加密版本，提供 256 位 AES 数据库加密。本项目支持通过构建标签 `sqlcipher` 来编译使用 SQLCipher 的版本。

## 概述

项目中的 `core/sqlcipher_build.go` 文件包含了 SQLCipher 的 CGO 配置。当使用 `-tags sqlcipher` 构建时，会链接 SQLCipher 库而不是标准的 SQLite。

代码中已经支持通过连接字符串的 `key` 参数来设置加密密钥：
```go
db, err := sql.Open("sqlite3", "path/to/db.db?key=your-encryption-key")
```

## macOS 构建步骤

### 1. 安装依赖

使用 Homebrew 安装 OpenSSL（如果尚未安装）：

```bash
brew install openssl@3
```

### 2. 构建 SQLCipher

```bash
# 克隆 SQLCipher 源码
git clone https://github.com/sqlcipher/sqlcipher.git
cd sqlcipher

# 配置构建（macOS 使用 darwin64-x86_64-cc 或 darwin64-arm64-cc）
# 对于 Apple Silicon Mac，使用 arm64
# 对于 Intel Mac，使用 x86_64

# 检测架构
ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
    OPENSSL_TARGET="darwin64-arm64-cc"
else
    OPENSSL_TARGET="darwin64-x86_64-cc"
fi

# 获取 OpenSSL 路径
OPENSSL_PREFIX=$(brew --prefix openssl@3)

# 配置 SQLCipher
CFLAGS="-DSQLITE_HAS_CODEC -I${OPENSSL_PREFIX}/include" \
LDFLAGS="-L${OPENSSL_PREFIX}/lib" \
./configure \
  --enable-tempstore=yes \
  --disable-tcl \
  --with-crypto-lib=openssl \
  --prefix=$HOME/sqlcipher-local

# 编译
make -j$(sysctl -n hw.ncpu)

# 安装
make install
```

### 3. 复制库文件到项目

将编译好的 SQLCipher 库文件复制到 `core/` 目录：

```bash
# 创建目标目录
mkdir -p /Users/orca/Documents/GitHub/orcas/core

# 复制库文件（macOS 使用 .dylib）
cp $HOME/sqlcipher-local/lib/libsqlcipher.a /Users/orca/Documents/GitHub/orcas/core/
# 或者如果是动态库
cp $HOME/sqlcipher-local/lib/libsqlcipher.dylib /Users/orca/Documents/GitHub/orcas/core/ 2>/dev/null || true

# 如果需要，也复制 OpenSSL 库（通常系统已安装，可跳过）
# cp ${OPENSSL_PREFIX}/lib/libcrypto.dylib /Users/orca/Documents/GitHub/orcas/core/
```

**注意**：macOS 通常使用静态库（`.a`）或动态库（`.dylib`），而不是 Linux 的 `.so` 文件。

### 4. 更新 sqlcipher_build.go（如果需要）

检查 `core/sqlcipher_build.go` 的配置。对于 macOS，可能需要调整链接选项：

```go
//go:build sqlcipher
// +build sqlcipher

package core

/*
#cgo CFLAGS: -DSQLITE_HAS_CODEC -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto
#cgo darwin LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto -Wl,-rpath,@loader_path
#cgo linux LDFLAGS: -L${SRCDIR} -lsqlcipher -lcrypto -Wl,-rpath,'$ORIGIN'
*/
import "C"
```

### 5. 构建 Go 项目

```bash
cd /Users/orca/Documents/GitHub/orcas

# 确保 CGO 已启用
export CGO_ENABLED=1

# 使用 sqlcipher 标签构建
go build -tags sqlcipher ./cmd

# 或者构建特定命令
go build -tags sqlcipher -o server ./cmd
```

### 6. 使用系统库（替代方案）

如果你使用 Homebrew 安装了 SQLCipher：

```bash
brew install sqlcipher
```

然后可以修改 `sqlcipher_build.go` 使用 pkg-config：

```go
/*
#cgo pkg-config: sqlcipher
#cgo CFLAGS: -DSQLITE_HAS_CODEC
*/
import "C"
```

## Linux 构建步骤

### 方法 1: 使用构建脚本（推荐）

```bash
cd core
chmod +x build_sqlcipher_linux.sh
./build_sqlcipher_linux.sh
```

脚本会自动：
1. 检查并安装依赖
2. 克隆/更新 SQLCipher 源码
3. 编译 SQLCipher
4. 复制库文件到 `core/` 目录

### 方法 2: 手动构建 SQLCipher from source

```bash
git clone https://github.com/sqlcipher/sqlcipher.git
cd sqlcipher

# Adjust --prefix as you like
CFLAGS="-DSQLITE_HAS_CODEC" \
LDFLAGS="-L/usr/lib/x86_64-linux-gnu" \
./configure \
  --enable-tempstore \
  --disable-tcl \
  --with-crypto-lib=openssl \
  --prefix=$HOME/sqlcipher-local

make -j$(nproc)
make install
```

如果 OpenSSL 头文件/库不在系统默认路径，在 `CFLAGS` 中添加 `-I/path/to/include`，在 `LDFLAGS` 中添加 `-L/path/to/lib`。

### 2. (可选) 本地构建 OpenSSL

```bash
git clone https://github.com/openssl/openssl.git
cd openssl
./Configure linux-x86_64 --prefix=$HOME/openssl-local
make -j$(nproc)
make install
```

然后将 SQLCipher 的 `CFLAGS/LDFLAGS` 指向 `$HOME/openssl-local/{include,lib}`。

### 3. 复制库文件到项目

将生成的 `libsqlcipher.so`（如果需要，还有 `libcrypto.so`）放到 `core/` 目录下：

```
/path/to/orcas/core/libsqlcipher.so
/path/to/orcas/core/libcrypto.so   # 如果依赖系统 libcrypto 则可选
```

### 3. 复制库文件到项目

将生成的 `libsqlcipher.so`（如果需要，还有 `libcrypto.so`）放到 `core/` 目录下：

```
/path/to/orcas/core/libsqlcipher.so
/path/to/orcas/core/libcrypto.so   # 如果依赖系统 libcrypto 则可选
```

### 4. 构建 Go 二进制文件

```bash
cd /path/to/orcas
CGO_ENABLED=1 go build -tags sqlcipher ./cmd
```

运行时，加载器会从 `core/` 文件夹中加载 `.so` 文件（感谢嵌入的 rpath）。如果你更喜欢使用系统安装的库，可以省略复制步骤，通过安装发行版的 `libsqlcipher-dev` 包来使用 `pkg-config`。

## GitHub Actions CI/CD 集成

项目已包含 GitHub Actions 工作流配置，可以在 CI/CD 中自动构建 SQLCipher 版本。

### 工作流文件

- `.github/workflows/ci.yml`: 主要的 CI/CD 工作流
- `.github/workflows/build-sqlcipher.yml`: 专门的 SQLCipher 构建工作流

### 自动构建

当推送到 `main`、`master` 或 `develop` 分支时，GitHub Actions 会自动：
1. 安装构建依赖（build-essential, libssl-dev, tcl-dev）
2. 构建 SQLCipher（使用缓存加速）
3. 运行测试（标准 SQLite 和 SQLCipher 版本）
4. 构建二进制文件
5. 上传构建产物

### 本地测试 CI 脚本

```bash
# 设置 CI 环境变量
export CI=true
export GITHUB_WORKSPACE=$(pwd)

# 运行构建脚本
cd core
./build_sqlcipher_linux.sh
```

### CI 环境优化

- **缓存机制**: SQLCipher 构建结果会被缓存，基于构建脚本哈希
- **并行构建**: 使用所有可用 CPU 核心加速编译
- **浅克隆**: 使用 `--depth 1` 减少克隆时间

## 验证构建

构建完成后，可以通过以下方式验证是否使用了 SQLCipher：

1. 检查二进制文件链接的库：
   ```bash
   # macOS
   otool -L ./server | grep sqlcipher
   
   # Linux
   ldd ./server | grep sqlcipher
   ```

2. 在代码中测试加密数据库：
   ```go
   db, err := sql.Open("sqlite3", "test.db?key=test-key-123")
   // 如果使用 SQLCipher，数据库文件会被加密
   ```

## 注意事项

1. **glibc 版本匹配**：在 Linux 上，SQLCipher/OpenSSL 工具链需要与目标设备的 glibc 版本匹配。在新版本发行版上编译的 `.so` 文件在旧版本上可能无法运行（如 `GLIBC_2.34 not found`）。安全做法是在目标机器上本地构建。

2. **跨平台编译**：macOS 和 Linux 的库文件格式不同（`.dylib` vs `.so`），需要分别为每个平台构建。

3. **加密密钥**：确保妥善保管数据库加密密钥。丢失密钥将无法恢复数据。

4. **性能**：SQLCipher 的加密/解密会带来一定的性能开销，但通常可以接受。

## 故障排除

### macOS: 找不到 libsqlcipher

如果遇到链接错误，检查：
- 库文件是否在 `core/` 目录
- `sqlcipher_build.go` 中的路径是否正确
- OpenSSL 是否正确安装

### Linux: GLIBC 版本不匹配

在目标机器上重新编译 SQLCipher，或使用 Docker 容器进行构建。

### 运行时错误

确保运行时库文件在正确的位置，或使用系统包管理器安装 SQLCipher。

