# GitHub Actions SQLCipher 构建指南

## 概述

项目已配置 GitHub Actions 工作流，可以在 CI/CD 中自动构建带 SQLCipher 加密的版本。

## 工作流说明

### ci.yml（推荐使用）

主要的 CI/CD 工作流，包含完整的测试和构建流程：

1. **test-standard**: 使用标准 SQLite 运行所有测试
2. **test-sqlcipher**: 使用 SQLCipher 运行所有测试
3. **build-sqlcipher**: 构建带 SQLCipher 的二进制文件（需要 test-sqlcipher 通过）
4. **build-standard**: 构建标准 SQLite 版本的二进制文件（需要 test-standard 通过）

### build-sqlcipher.yml

独立的 SQLCipher 构建工作流，可以单独运行。

## 触发条件

工作流会在以下情况自动运行：

- ✅ Push 到 `main`、`master` 或 `develop` 分支
- ✅ 创建 Pull Request 到上述分支
- ✅ 手动触发（在 Actions 页面点击 "Run workflow"）

## 构建过程

### 1. 安装依赖

```yaml
sudo apt-get install -y \
  build-essential \
  libssl-dev \
  tcl-dev \
  git
```

### 2. 构建 SQLCipher

使用 `core/build_sqlcipher_linux.sh` 脚本：
- 自动检测架构（x86_64/arm64）
- 查找 OpenSSL 库
- 编译 SQLCipher
- 复制库文件到 `core/` 目录

### 3. 构建 Go 项目

```bash
CGO_ENABLED=1 go build -tags sqlcipher -o orcas-server ./cmd
```

## 缓存机制

SQLCipher 构建结果会被缓存以加速后续构建：

- **缓存键**: `sqlcipher-ubuntu-latest-<脚本哈希>`
- **缓存路径**: 
  - `sqlcipher-src/` (源码)
  - `sqlcipher-local/` (编译结果)

缓存会在以下情况失效：
- 构建脚本 (`build_sqlcipher_linux.sh`) 被修改
- 缓存过期（GitHub Actions 默认 7 天）

## 构建产物

构建成功后会生成以下产物，可在 Actions 页面下载：

- **orcas-server-sqlcipher**: 带 SQLCipher 的二进制文件 + 库文件
- **orcas-server-standard**: 标准 SQLite 版本的二进制文件

产物保留 **30 天**。

## 查看构建日志

1. 进入 GitHub 仓库
2. 点击 "Actions" 标签
3. 选择对应的工作流运行
4. 查看各步骤的详细日志

## 故障排除

### 构建失败：找不到 OpenSSL

确保工作流中安装了 `libssl-dev`：
```yaml
- name: Install build dependencies
  run: |
    sudo apt-get update
    sudo apt-get install -y libssl-dev
```

### 构建失败：找不到库文件

检查 `core/libsqlcipher.so` 是否存在：
```bash
ls -lh core/libsqlcipher.*
```

### 测试失败

确保使用正确的构建标签：
```bash
CGO_ENABLED=1 go test -tags sqlcipher ./...
```

### 缓存问题

如果缓存导致问题，可以在工作流中添加清理步骤：
```yaml
- name: Clear cache
  uses: actions/cache@v4
  with:
    path: |
      ${{ github.workspace }}/sqlcipher-local
      ${{ github.workspace }}/sqlcipher-src
    key: clear-cache
```

## 本地测试 CI 脚本

在本地测试 CI 构建脚本：

```bash
# 设置 CI 环境变量
export CI=true
export GITHUB_WORKSPACE=$(pwd)

# 运行构建脚本
cd core
./build_sqlcipher_linux.sh

# 构建项目
cd ..
CGO_ENABLED=1 go build -tags sqlcipher ./cmd
```

## 自定义工作流

如果需要自定义构建流程，可以：

1. 复制 `.github/workflows/ci.yml`
2. 修改构建步骤
3. 调整缓存策略
4. 添加额外的测试或部署步骤

## 性能优化建议

1. **使用缓存**: 已配置 SQLCipher 构建缓存
2. **并行构建**: 使用 `-j$(nproc)` 利用所有 CPU 核心
3. **浅克隆**: SQLCipher 使用 `--depth 1` 减少克隆时间
4. **条件构建**: 只在需要时构建 SQLCipher 版本

## 相关文件

- `.github/workflows/ci.yml`: 主 CI/CD 工作流
- `.github/workflows/build-sqlcipher.yml`: SQLCipher 构建工作流
- `core/build_sqlcipher_linux.sh`: Linux 构建脚本
- `core/sqlcipher_build.go`: CGO 构建配置
- `core/building.md`: 详细构建文档

