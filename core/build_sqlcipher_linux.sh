#!/bin/bash

# SQLCipher 构建脚本 for Linux (适用于 CI/CD)
# 此脚本帮助在 Linux 上编译 SQLCipher 并复制到项目目录

set -e

# 颜色输出（如果支持）
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    NC=''
fi

echo -e "${GREEN}SQLCipher Linux 构建脚本${NC}"
echo "================================"

# 检测架构
ARCH=$(uname -m)
echo -e "${GREEN}检测到架构: ${ARCH}${NC}"

# 项目 core 目录（脚本所在目录）
PROJECT_CORE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 如果设置了环境变量，使用环境变量（CI/CD 环境）
if [ -n "$CI" ] || [ -n "$GITHUB_WORKSPACE" ]; then
    WORKSPACE="${GITHUB_WORKSPACE:-$(pwd)}"
    SQLCIPHER_DIR="${SQLCIPHER_DIR:-$WORKSPACE/sqlcipher-src}"
    INSTALL_PREFIX="${INSTALL_PREFIX:-$WORKSPACE/sqlcipher-local}"
    echo -e "${GREEN}检测到 CI 环境，使用工作空间: ${WORKSPACE}${NC}"
else
    SQLCIPHER_DIR="${SQLCIPHER_DIR:-$HOME/sqlcipher-src}"
    INSTALL_PREFIX="${INSTALL_PREFIX:-$HOME/sqlcipher-local}"
fi

echo -e "\n${YELLOW}SQLCipher 源码目录: ${SQLCIPHER_DIR}${NC}"
echo -e "${YELLOW}安装前缀: ${INSTALL_PREFIX}${NC}"
echo -e "${YELLOW}项目 core 目录: ${PROJECT_CORE_DIR}${NC}"

# 检查依赖
echo -e "\n${YELLOW}检查构建依赖...${NC}"

MISSING_DEPS=()

if ! command -v gcc &> /dev/null; then
    MISSING_DEPS+=("gcc")
fi

if ! command -v make &> /dev/null; then
    MISSING_DEPS+=("make")
fi

if ! pkg-config --exists openssl 2>/dev/null; then
    if [ ! -f "/usr/include/openssl/ssl.h" ] && [ ! -f "/usr/local/include/openssl/ssl.h" ]; then
        MISSING_DEPS+=("libssl-dev")
    fi
fi

if [ ${#MISSING_DEPS[@]} -gt 0 ]; then
    echo -e "${YELLOW}缺少依赖: ${MISSING_DEPS[*]}${NC}"
    if [ -n "$CI" ]; then
        echo -e "${RED}错误: CI 环境中缺少必要的构建依赖${NC}"
        echo "请在 GitHub Actions 中安装: sudo apt-get update && sudo apt-get install -y build-essential libssl-dev tcl-dev"
        exit 1
    else
        echo "请安装缺少的依赖包"
        exit 1
    fi
fi

# 查找 OpenSSL
OPENSSL_PREFIX=""
if pkg-config --exists openssl 2>/dev/null; then
    OPENSSL_PREFIX=$(pkg-config --variable=prefix openssl 2>/dev/null || echo "")
fi

if [ -z "$OPENSSL_PREFIX" ]; then
    if [ -f "/usr/lib/x86_64-linux-gnu/libssl.so" ] || [ -f "/usr/lib/aarch64-linux-gnu/libssl.so" ]; then
        # 系统安装的 OpenSSL
        if [ -f "/usr/lib/x86_64-linux-gnu/libssl.so" ]; then
            OPENSSL_LIB_DIR="/usr/lib/x86_64-linux-gnu"
        else
            OPENSSL_LIB_DIR="/usr/lib/aarch64-linux-gnu"
        fi
        OPENSSL_INCLUDE_DIR="/usr/include"
    else
        OPENSSL_LIB_DIR="/usr/local/lib"
        OPENSSL_INCLUDE_DIR="/usr/local/include"
    fi
else
    OPENSSL_LIB_DIR="$OPENSSL_PREFIX/lib"
    OPENSSL_INCLUDE_DIR="$OPENSSL_PREFIX/include"
fi

echo -e "${GREEN}OpenSSL 库目录: ${OPENSSL_LIB_DIR}${NC}"
echo -e "${GREEN}OpenSSL 头文件目录: ${OPENSSL_INCLUDE_DIR}${NC}"

# 克隆或更新 SQLCipher
if [ ! -d "$SQLCIPHER_DIR" ]; then
    echo -e "\n${YELLOW}克隆 SQLCipher 源码...${NC}"
    git clone --depth 1 https://github.com/sqlcipher/sqlcipher.git "$SQLCIPHER_DIR"
else
    echo -e "\n${YELLOW}更新 SQLCipher 源码...${NC}"
    cd "$SQLCIPHER_DIR"
    git fetch --depth 1
    git reset --hard origin/master || git reset --hard origin/main
fi

cd "$SQLCIPHER_DIR"

# 清理之前的构建
if [ -f "Makefile" ]; then
    echo -e "\n${YELLOW}清理之前的构建...${NC}"
    make distclean || true
fi

# 配置
echo -e "\n${YELLOW}配置 SQLCipher...${NC}"
# SQLCipher 新版本不再支持 --with-crypto-lib，使用 CFLAGS 和 LDFLAGS 指定 OpenSSL
CFLAGS="-DSQLITE_HAS_CODEC -DSQLITE_TEMP_STORE=2 -DSQLCIPHER_CRYPTO_OPENSSL -I${OPENSSL_INCLUDE_DIR}" \
LDFLAGS="-L${OPENSSL_LIB_DIR} -lcrypto" \
./configure \
  --disable-tcl \
  --prefix="$INSTALL_PREFIX"

# 编译
CPU_COUNT=$(nproc 2>/dev/null || echo "4")
echo -e "\n${YELLOW}编译 SQLCipher (使用 ${CPU_COUNT} 个核心)...${NC}"
make -j${CPU_COUNT}

# 安装
echo -e "\n${YELLOW}安装 SQLCipher...${NC}"
make install

# 复制库文件到项目
echo -e "\n${YELLOW}复制库文件到项目目录...${NC}"

# 查找库文件
LIB_FILE=""
if [ -f "$INSTALL_PREFIX/lib/libsqlcipher.so" ]; then
    LIB_FILE="$INSTALL_PREFIX/lib/libsqlcipher.so"
    echo -e "${GREEN}找到动态库: ${LIB_FILE}${NC}"
    cp "$LIB_FILE" "$PROJECT_CORE_DIR/libsqlcipher.so"
    echo -e "${GREEN}已复制到: ${PROJECT_CORE_DIR}/libsqlcipher.so${NC}"
    
    # 如果是动态库，可能需要复制依赖
    if ldd "$LIB_FILE" 2>/dev/null | grep -q "libcrypto"; then
        echo -e "${YELLOW}注意: libsqlcipher.so 依赖 libcrypto，确保运行时可用${NC}"
    fi
elif [ -f "$INSTALL_PREFIX/lib/libsqlcipher.a" ]; then
    LIB_FILE="$INSTALL_PREFIX/lib/libsqlcipher.a"
    echo -e "${GREEN}找到静态库: ${LIB_FILE}${NC}"
    cp "$LIB_FILE" "$PROJECT_CORE_DIR/libsqlcipher.a"
    echo -e "${GREEN}已复制到: ${PROJECT_CORE_DIR}/libsqlcipher.a${NC}"
else
    echo -e "${RED}错误: 未找到编译好的库文件${NC}"
    echo "请检查 $INSTALL_PREFIX/lib/ 目录"
    ls -la "$INSTALL_PREFIX/lib/" || true
    exit 1
fi

echo -e "\n${GREEN}构建完成！${NC}"
echo ""
echo "库文件位置: ${PROJECT_CORE_DIR}/$(basename "$LIB_FILE")"
echo ""
echo "下一步："
echo "  CGO_ENABLED=1 go build -tags sqlcipher ./cmd"

