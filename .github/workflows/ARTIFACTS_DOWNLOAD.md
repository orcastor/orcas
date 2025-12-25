# GitHub Actions 构建产物下载指南

## 方法 1: 通过 GitHub Web 界面下载（最简单）

### 步骤：

1. **进入 GitHub 仓库页面**
   - 打开你的 GitHub 仓库：`https://github.com/你的用户名/orcas`

2. **进入 Actions 页面**
   - 点击仓库顶部的 **"Actions"** 标签

3. **选择工作流运行**
   - 在左侧选择工作流（如 `CI` 或 `Build with SQLCipher`）
   - 点击你想要下载的构建运行记录

4. **下载产物**
   - 在运行详情页面底部，找到 **"Artifacts"** 部分
   - 点击产物名称（如 `orcas-server-sqlcipher` 或 `orcas-server-standard`）
   - 产物会自动下载为 ZIP 文件

### 示例：

```
仓库页面 → Actions → CI → 选择运行记录 → Artifacts → orcas-server-sqlcipher
```

## 方法 2: 通过 GitHub CLI 下载

### 安装 GitHub CLI

```bash
# macOS
brew install gh

# Linux (Ubuntu/Debian)
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
sudo apt update
sudo apt install gh

# 登录
gh auth login
```

### 下载最新产物

```bash
# 列出可用的产物
gh run list --workflow=ci.yml

# 下载最新的 SQLCipher 构建产物
gh run download --name orcas-server-sqlcipher

# 下载最新的标准构建产物
gh run download --name orcas-server-standard

# 下载特定运行的所有产物
gh run download <run-id>
```

## 方法 3: 通过 GitHub API 下载

### 获取运行 ID

```bash
# 使用 GitHub CLI
gh run list --workflow=ci.yml --limit 1

# 或通过 API
curl -H "Authorization: token YOUR_TOKEN" \
  https://api.github.com/repos/你的用户名/orcas/actions/runs
```

### 下载产物

```bash
# 1. 获取产物 ID
RUN_ID="你的运行ID"
curl -H "Authorization: token YOUR_TOKEN" \
  https://api.github.com/repos/你的用户名/orcas/actions/runs/$RUN_ID/artifacts

# 2. 下载产物（需要产物 ID）
ARTIFACT_ID="产物ID"
curl -L -H "Authorization: token YOUR_TOKEN" \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/你的用户名/orcas/actions/artifacts/$ARTIFACT_ID/zip \
  -o artifact.zip
```

## 方法 4: 自动下载脚本

创建一个脚本自动下载最新的构建产物：

```bash
#!/bin/bash
# download_latest_artifact.sh

REPO="你的用户名/orcas"
WORKFLOW="ci.yml"
ARTIFACT_NAME="orcas-server-sqlcipher"

# 使用 GitHub CLI
if command -v gh &> /dev/null; then
    echo "使用 GitHub CLI 下载..."
    gh run download --name "$ARTIFACT_NAME" --repo "$REPO"
else
    echo "请安装 GitHub CLI: brew install gh"
    exit 1
fi
```

## 产物内容说明

### orcas-server-sqlcipher
包含：
- `orcas-server` - 带 SQLCipher 的二进制文件
- `core/libsqlcipher.so` - SQLCipher 动态库（Linux）
- `core/libsqlcipher.a` - SQLCipher 静态库（如果存在）

### orcas-server-standard
包含：
- `orcas-server` - 标准 SQLite 版本的二进制文件

## 产物保留时间

根据工作流配置，产物会保留 **30 天**。过期后需要重新构建。

## 使用下载的二进制文件

### Linux

```bash
# 解压产物
unzip orcas-server-sqlcipher.zip

# 如果使用动态库，确保库文件在正确位置
export LD_LIBRARY_PATH=./core:$LD_LIBRARY_PATH

# 运行
./orcas-server
```

### macOS

```bash
# 解压产物
unzip orcas-server-sqlcipher.zip

# 如果使用动态库，可能需要设置路径
export DYLD_LIBRARY_PATH=./core:$DYLD_LIBRARY_PATH

# 运行
./orcas-server
```

## 故障排除

### 问题：找不到 Artifacts 部分

**原因**：构建可能失败或未生成产物

**解决**：
1. 检查构建是否成功完成
2. 确认工作流中包含了 `upload-artifact` 步骤
3. 检查产物名称是否正确

### 问题：下载的 ZIP 文件损坏

**解决**：
1. 重新下载
2. 检查网络连接
3. 使用 GitHub CLI 下载（更可靠）

### 问题：二进制文件无法运行

**原因**：可能是架构不匹配或缺少依赖库

**解决**：
1. 确认下载的产物架构与目标系统匹配
2. 检查动态库依赖：`ldd orcas-server`（Linux）或 `otool -L orcas-server`（macOS）
3. 确保所有依赖库都已安装

## 自动化部署

如果你需要自动下载并部署，可以在工作流中添加部署步骤：

```yaml
- name: Download artifact
  uses: actions/download-artifact@v4
  with:
    name: orcas-server-sqlcipher

- name: Deploy
  run: |
    chmod +x orcas-server
    # 你的部署命令
```

## 相关链接

- [GitHub Actions Artifacts 文档](https://docs.github.com/en/actions/using-workflows/storing-workflow-data-as-artifacts)
- [GitHub CLI 文档](https://cli.github.com/manual/)
- [GitHub API 文档](https://docs.github.com/en/rest/actions/artifacts)

