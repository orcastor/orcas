#!/bin/bash

# SQLCipher 构建脚本 for macOS
# 此脚本帮助在 macOS 上编译 SQLCipher 并复制到项目目录

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}SQLCipher macOS 构建脚本${NC}"
echo "================================"

# 检测架构
ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
    echo -e "${GREEN}检测到 Apple Silicon (arm64)${NC}"
    OPENSSL_TARGET="darwin64-arm64-cc"
else
    echo -e "${GREEN}检测到 Intel (x86_64)${NC}"
    OPENSSL_TARGET="darwin64-x86_64-cc"
fi

# 检查 OpenSSL
echo -e "\n${YELLOW}检查 OpenSSL...${NC}"
if ! command -v brew &> /dev/null; then
    echo -e "${RED}错误: 未找到 Homebrew。请先安装 Homebrew: https://brew.sh${NC}"
    exit 1
fi

OPENSSL_PREFIX=$(brew --prefix openssl@3 2>/dev/null || brew --prefix openssl 2>/dev/null || echo "")
if [ -z "$OPENSSL_PREFIX" ]; then
    echo -e "${YELLOW}未找到 OpenSSL，正在安装...${NC}"
    brew install openssl@3
    OPENSSL_PREFIX=$(brew --prefix openssl@3)
fi

echo -e "${GREEN}OpenSSL 路径: ${OPENSSL_PREFIX}${NC}"

# SQLCipher 源码目录
SQLCIPHER_DIR="$HOME/sqlcipher-src"
INSTALL_PREFIX="$HOME/sqlcipher-local"
PROJECT_CORE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "\n${YELLOW}SQLCipher 源码目录: ${SQLCIPHER_DIR}${NC}"
echo -e "${YELLOW}安装前缀: ${INSTALL_PREFIX}${NC}"
echo -e "${YELLOW}项目 core 目录: ${PROJECT_CORE_DIR}${NC}"

# 克隆或更新 SQLCipher
if [ ! -d "$SQLCIPHER_DIR" ]; then
    echo -e "\n${YELLOW}克隆 SQLCipher 源码...${NC}"
    git clone https://github.com/sqlcipher/sqlcipher.git "$SQLCIPHER_DIR"
else
    echo -e "\n${YELLOW}更新 SQLCipher 源码...${NC}"
    cd "$SQLCIPHER_DIR"
    git pull
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
# SQLCipher 要求必须定义 SQLITE_EXTRA_INIT 和 SQLITE_EXTRA_SHUTDOWN
CFLAGS="-DSQLITE_HAS_CODEC -DSQLITE_TEMP_STORE=2 -DSQLCIPHER_CRYPTO_OPENSSL -DSQLITE_EXTRA_INIT=sqlcipher_extra_init -DSQLITE_EXTRA_SHUTDOWN=sqlcipher_extra_shutdown -I${OPENSSL_PREFIX}/include" \
LDFLAGS="-L${OPENSSL_PREFIX}/lib -lcrypto" \
./configure \
  --disable-tcl \
  --prefix="$INSTALL_PREFIX"

# 编译
echo -e "\n${YELLOW}编译 SQLCipher (使用 $(sysctl -n hw.ncpu) 个核心)...${NC}"
make -j$(sysctl -n hw.ncpu)

# 安装
echo -e "\n${YELLOW}安装 SQLCipher...${NC}"
make install

# 复制库文件到项目
echo -e "\n${YELLOW}复制库文件到项目目录...${NC}"

# 查找库文件
LIB_FILE=""
if [ -f "$INSTALL_PREFIX/lib/libsqlcipher.a" ]; then
    LIB_FILE="$INSTALL_PREFIX/lib/libsqlcipher.a"
    echo -e "${GREEN}找到静态库: ${LIB_FILE}${NC}"
    cp "$LIB_FILE" "$PROJECT_CORE_DIR/libsqlcipher.a"
    echo -e "${GREEN}已复制到: ${PROJECT_CORE_DIR}/libsqlcipher.a${NC}"
elif [ -f "$INSTALL_PREFIX/lib/libsqlcipher.dylib" ]; then
    LIB_FILE="$INSTALL_PREFIX/lib/libsqlcipher.dylib"
    echo -e "${GREEN}找到动态库: ${LIB_FILE}${NC}"
    cp "$LIB_FILE" "$PROJECT_CORE_DIR/libsqlcipher.dylib"
    echo -e "${GREEN}已复制到: ${PROJECT_CORE_DIR}/libsqlcipher.dylib${NC}"
else
    echo -e "${RED}错误: 未找到编译好的库文件${NC}"
    echo "请检查 $INSTALL_PREFIX/lib/ 目录"
    exit 1
fi

# 检查是否需要复制 OpenSSL 库（通常不需要，系统已安装）
echo -e "\n${GREEN}构建完成！${NC}"
echo ""
echo "下一步："
echo "1. 确保库文件在: ${PROJECT_CORE_DIR}/"
echo "2. 使用以下命令构建项目:"
echo "   cd $(dirname "$PROJECT_CORE_DIR")"
echo "   CGO_ENABLED=1 go build -tags sqlcipher ./cmd"
echo ""
echo "或者使用系统安装的 SQLCipher:"
echo "   brew install sqlcipher"
echo "   然后修改 sqlcipher_build.go 使用 pkg-config"

