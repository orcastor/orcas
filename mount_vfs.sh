#!/bin/bash

# ORCAS VFS 挂载脚本
# 挂载到 /media/ZimaOS-HD/orcas_test，开启加密和智能压缩

set -e

# 配置参数
MOUNT_POINT="/media/ZimaOS-HD/orcas_test"
ENCRYPTION_METHOD="AES256"  # 可选: AES256 或 SM4
ENCRYPTION_KEY="orcas-encryption-key-12345678901234567890"  # AES256需要>16字符，SM4需要16字符
#COMPRESSION_METHOD="ZSTD"   # 可选: SNAPPY, ZSTD, GZIP, BR
#COMPRESSION_LEVEL=5         # 压缩级别

# 检查挂载点是否存在，不存在则创建
if [ ! -d "$MOUNT_POINT" ]; then
    echo "创建挂载点目录: $MOUNT_POINT"
    sudo mkdir -p "$MOUNT_POINT"
    sudo chmod 755 "$MOUNT_POINT"
fi

# 检查是否已经挂载
if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    echo "警告: $MOUNT_POINT 已经挂载"
    echo "如果要重新挂载，请先卸载: sudo fusermount -u $MOUNT_POINT"
    exit 1
fi

# 获取可执行文件路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORCAS_CMD="$SCRIPT_DIR/main"

# 如果main不存在，尝试使用go run
if [ ! -f "$ORCAS_CMD" ]; then
    echo "可执行文件不存在，使用 go run..."
    ORCAS_CMD="go run $SCRIPT_DIR/cmd/main.go"
fi

# 提示用户输入用户名和密码
read -p "请输入用户名: " USERNAME
read -sp "请输入密码: " PASSWORD
echo ""

# 构建挂载命令
MOUNT_CMD="$ORCAS_CMD -action mount -mountpoint $MOUNT_POINT -user $USERNAME -pass $PASSWORD -endecway $ENCRYPTION_METHOD -endeckey $ENCRYPTION_KEY -debug"
# -cmprway $COMPRESSION_METHOD -cmprqlty $COMPRESSION_LEVEL 

echo "=========================================="
echo "ORCAS VFS 挂载配置"
echo "=========================================="
echo "挂载点: $MOUNT_POINT"
echo "加密方式: $ENCRYPTION_METHOD"
# echo "智能压缩: $COMPRESSION_METHOD (级别: $COMPRESSION_LEVEL)"
echo "=========================================="
echo ""

# 执行挂载
echo "正在挂载..."
eval $MOUNT_CMD

