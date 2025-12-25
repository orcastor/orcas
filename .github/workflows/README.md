# GitHub Actions Workflows

本目录包含项目的 CI/CD 工作流配置。

## 工作流说明

### ci.yml
主要的 CI/CD 工作流，包含：
- **test-standard**: 使用标准 SQLite 运行测试
- **test-sqlcipher**: 使用 SQLCipher 运行测试
- **build-sqlcipher**: 构建带 SQLCipher 的二进制文件
- **build-standard**: 构建标准 SQLite 版本的二进制文件

### build-sqlcipher.yml
专门用于构建 SQLCipher 版本的工作流（独立运行）。

## 使用说明

### 触发条件
- Push 到 `main`、`master` 或 `develop` 分支
- 创建 Pull Request 到上述分支
- 手动触发（workflow_dispatch）

### 缓存机制
SQLCipher 构建结果会被缓存，以加速后续构建：
- 缓存键基于构建脚本的哈希值
- 缓存路径：`sqlcipher-src` 和 `sqlcipher-local`

### 构建产物
构建成功后会生成以下产物：
- `orcas-server-sqlcipher`: 带 SQLCipher 的二进制文件
- `orcas-server-standard`: 标准 SQLite 版本的二进制文件

产物保留 30 天，可在 Actions 页面下载。

## 本地测试

在本地测试 CI 脚本：

```bash
# 设置 CI 环境变量
export CI=true
export GITHUB_WORKSPACE=$(pwd)

# 运行构建脚本
cd core
./build_sqlcipher_linux.sh
```

## 故障排除

### 构建失败：找不到 OpenSSL
确保安装了 `libssl-dev`：
```bash
sudo apt-get install -y libssl-dev
```

### 构建失败：找不到库文件
检查 `core/` 目录是否有 `libsqlcipher.so` 文件。

### 测试失败
确保使用 `-tags sqlcipher` 标签运行测试：
```bash
CGO_ENABLED=1 go test -tags sqlcipher ./...
```

