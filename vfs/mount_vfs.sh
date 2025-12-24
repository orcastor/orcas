#!/bin/bash

# ORCAS VFS 挂载脚本
# 此脚本用于快速挂载 OrcaS 文件系统

set -e

# 默认配置
MOUNTPOINT="${MOUNTPOINT:-/DATA/orcas_test}"
ENCRYPTION_KEY="${ENCRYPTION_KEY:-orcas-encryption-key-12345678901234567890}"
COMPRESSION="ZSTD"
COMPRESSION_LEVEL="5"

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 切换到项目根目录
cd "$PROJECT_ROOT"

# 检查是否已编译
if [ ! -f "./main" ]; then
    echo "正在编译程序..."
    go build -o main ./cmd/main.go
    if [ $? -ne 0 ]; then
        echo "编译失败！"
        exit 1
    fi
    echo "编译完成！"
fi

# 检查挂载点
if [ ! -d "$MOUNTPOINT" ]; then
    echo "创建挂载点: $MOUNTPOINT"
    sudo mkdir -p "$MOUNTPOINT"
    sudo chmod 755 "$MOUNTPOINT"
fi

# 检查是否已挂载
if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
    echo "警告: $MOUNTPOINT 已经挂载"
    read -p "是否卸载后重新挂载? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        sudo fusermount -u "$MOUNTPOINT" 2>/dev/null || true
    else
        echo "取消挂载"
        exit 0
    fi
fi

# 检查是否使用无主数据库模式
# 注意：ORCAS_BASE 和 ORCAS_DATA 环境变量已不再使用
# 路径现在通过 context 配置，默认使用当前目录
# 如果未指定 basePath，将使用 -noauth 模式
if [ -z "$BASE_PATH" ] || [ "$BASE_PATH" = "" ]; then
    echo "检测到无主数据库模式（BASE_PATH 未设置）"
    echo "将使用 -noauth 模式"
    
    if [ -z "$BUCKET_ID" ]; then
        read -p "请输入 Bucket ID: " BUCKET_ID
    fi
    
    if [ -z "$BUCKET_ID" ] || [ "$BUCKET_ID" = "" ]; then
        echo "错误: 无主数据库模式下必须指定 Bucket ID"
        exit 1
    fi
    
    echo "使用 Bucket ID: $BUCKET_ID"
    echo "挂载点: $MOUNTPOINT"
    echo "加密密钥: ${ENCRYPTION_KEY:0:20}..."
    echo ""
    echo "开始挂载..."
    
    ./main -action mount \
        -noauth \
        -bucket "$BUCKET_ID" \
        -mountpoint "$MOUNTPOINT" \
        -endecway AES256 \
        -endeckey "$ENCRYPTION_KEY" \
        -cmprway "$COMPRESSION" \
        -cmprqlty "$COMPRESSION_LEVEL"
else
    echo "使用主数据库模式"
    
    # 获取用户名和密码
    if [ -z "$ORCAS_USER" ]; then
        read -p "请输入用户名: " ORCAS_USER
    fi
    
    if [ -z "$ORCAS_PASSWORD" ]; then
        read -s -p "请输入密码: " ORCAS_PASSWORD
        echo
    fi
    
    if [ -z "$ORCAS_USER" ] || [ -z "$ORCAS_PASSWORD" ]; then
        echo "错误: 用户名和密码不能为空"
        exit 1
    fi
    
    echo "用户名: $ORCAS_USER"
    echo "挂载点: $MOUNTPOINT"
    echo "加密密钥: ${ENCRYPTION_KEY:0:20}..."
    echo ""
    echo "开始挂载..."
    
    ./main -action mount \
        -mountpoint "$MOUNTPOINT" \
        -user "$ORCAS_USER" \
        -pass "$ORCAS_PASSWORD" \
        -endecway AES256 \
        -endeckey "$ENCRYPTION_KEY" \
        -cmprway "$COMPRESSION" \
        -cmprqlty "$COMPRESSION_LEVEL"
fi

