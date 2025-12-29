# 在依赖项目中使用 SQLCipher

当你的项目依赖 `github.com/orcastor/orcas` 时，可以通过构建标签（build tags）来启用 SQLCipher 支持。

## 快速开始

### 1. 确保 SQLCipher 库文件可用

首先，你需要在 orcas 项目的 `core/` 目录中准备好 SQLCipher 库文件：

#### macOS
```bash
# 在 orcas 项目目录中运行
./core/build_sqlcipher_macos.sh
```

这会在 `core/` 目录下生成：
- `libsqlcipher.a` 或 `libsqlcipher.dylib`
- 确保 OpenSSL 的 crypto 库可用

#### Linux
```bash
# 在 orcas 项目目录中运行
./core/build_sqlcipher_linux.sh
```

这会在 `core/` 目录下生成：
- `libsqlcipher.so` 或 `libsqlcipher.a`
- 确保 OpenSSL 的 crypto 库可用

### 2. 在你的项目中启用 SQLCipher

在你的项目中使用 `-tags sqlcipher` 构建标签来编译：

```bash
# 构建你的项目（启用 SQLCipher）
CGO_ENABLED=1 go build -tags sqlcipher -o your-app ./cmd

# 或者使用 go install
CGO_ENABLED=1 go install -tags sqlcipher ./cmd

# 运行测试
CGO_ENABLED=1 go test -tags sqlcipher ./...
```

### 3. 使用 replace 指令（推荐）

如果你的项目使用 `go.mod`，可以使用 `replace` 指令来指向本地的 orcas 项目：

```go
module your-project

go 1.18

require (
    github.com/orcastor/orcas v0.0.0
    // ... 其他依赖
)

replace github.com/orcastor/orcas => /path/to/orcas
```

这样你就可以在 orcas 项目中构建 SQLCipher 库，然后在你的项目中使用。

## 详细说明

### 构建标签的工作原理

orcas 项目中的 `core/sqlcipher_build.go` 文件使用了构建标签：

```go
//go:build sqlcipher
// +build sqlcipher
```

这意味着：
- **不使用 `-tags sqlcipher`**：使用标准的 SQLite（通过 `mattn/go-sqlite3`）
- **使用 `-tags sqlcipher`**：使用 SQLCipher（通过 `core/sqlcipher_build.go` 的 CGO 配置）

### 构建标签的传递性

**重要**：构建标签不会自动传递给依赖项。你必须在构建你的项目时显式指定 `-tags sqlcipher`，这样 Go 编译器才会：
1. 编译 orcas 项目中使用 `sqlcipher` 标签的文件
2. 链接 SQLCipher 库而不是标准 SQLite

### 示例：完整的构建流程

```bash
# 1. 在 orcas 项目中构建 SQLCipher 库
cd /path/to/orcas
./core/build_sqlcipher_macos.sh  # macOS
# 或
./core/build_sqlcipher_linux.sh  # Linux

# 2. 在你的项目中使用 replace 指向本地 orcas
cd /path/to/your-project
# 编辑 go.mod，添加 replace 指令（见上文）

# 3. 构建你的项目（启用 SQLCipher）
CGO_ENABLED=1 go build -tags sqlcipher -o your-app ./cmd
```

### 验证是否使用了 SQLCipher

构建完成后，可以验证是否链接了 SQLCipher：

#### macOS
```bash
otool -L ./your-app | grep sqlcipher
```

#### Linux
```bash
ldd ./your-app | grep sqlcipher
```

如果看到 `libsqlcipher` 相关的输出，说明成功链接了 SQLCipher。

### 运行时要求

使用 SQLCipher 构建的应用程序在运行时需要：
1. SQLCipher 库文件（`libsqlcipher.so`/`libsqlcipher.dylib`/`libsqlcipher.a`）
2. OpenSSL crypto 库（`libcrypto.so`/`libcrypto.dylib`）

这些库文件应该：
- 在 `core/` 目录中（如果使用 `-L${SRCDIR}` 和 `-Wl,-rpath` 配置）
- 或者在系统库路径中
- 或者在运行时库搜索路径中

### 常见问题

#### 1. 找不到 libsqlcipher

**问题**：构建时提示找不到 `libsqlcipher`

**解决方案**：
- 确保在 orcas 项目的 `core/` 目录中有 SQLCipher 库文件
- 检查 `core/sqlcipher_build.go` 中的路径配置是否正确
- 确保使用 `CGO_ENABLED=1` 环境变量

#### 2. 运行时找不到库

**问题**：运行时提示找不到 `libsqlcipher.so` 或 `libcrypto.so`

**解决方案**：
- 确保库文件在正确的位置（`core/` 目录或系统库路径）
- 检查 `rpath` 配置是否正确
- 使用 `LD_LIBRARY_PATH` 环境变量（Linux）或 `DYLD_LIBRARY_PATH`（macOS）

#### 3. 版本不匹配

**问题**：在不同机器上编译的库文件无法运行

**解决方案**：
- 在目标机器上重新编译 SQLCipher
- 使用 Docker 容器进行构建以确保环境一致
- 使用静态链接（`.a` 文件）而不是动态链接（`.so`/`.dylib`）

## 使用系统安装的 SQLCipher

如果你使用系统包管理器安装了 SQLCipher（如 `brew install sqlcipher` 或 `apt-get install libsqlcipher-dev`），可以修改 `core/sqlcipher_build.go` 使用 `pkg-config`：

```go
//go:build sqlcipher
// +build sqlcipher

package core

/*
#cgo pkg-config: sqlcipher
*/
import "C"
```

这样就不需要手动管理 SQLCipher 库文件了。

## CI/CD 集成

在 CI/CD 流程中，可以这样构建：

```yaml
# GitHub Actions 示例
- name: Build SQLCipher
  run: |
    cd $GITHUB_WORKSPACE/orcas
    ./core/build_sqlcipher_linux.sh

- name: Build application
  env:
    CGO_ENABLED: 1
  run: |
    go build -tags sqlcipher -o app ./cmd
```

## 总结

要在依赖 orcas 的项目中使用 SQLCipher：

1. ✅ 在 orcas 项目中构建 SQLCipher 库
2. ✅ 在你的项目构建时使用 `-tags sqlcipher`
3. ✅ 确保 `CGO_ENABLED=1`
4. ✅ 确保运行时库文件可用

这样就可以在你的项目中使用 SQLCipher 的加密数据库功能了！

