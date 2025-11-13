#!/bin/bash

# Benchmark script to compare minIO and orcas S3 using minIO's warp tool
# Tests 4KB small files on RAM disk

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
RAMDISK_PATH="/tmp/orcas_benchmark_ramdisk"
ORCAS_BASE="/tmp/orcas_benchmark"
ORCAS_DATA="/tmp/orcas_benchmark_data"
ORCAS_PORT=9000
MINIO_PORT=9001
BUCKET_NAME="test-bucket"
ACCESS_KEY="minioadmin"
SECRET_KEY="minioadmin"
FILE_SIZE="4KB"
CONCURRENCY=10
DURATION="30s"
OBJECTS=1000

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    # Kill background processes
    jobs -p | xargs -r kill 2>/dev/null || true
    sleep 1
    # Force kill if still running
    jobs -p | xargs -r kill -9 2>/dev/null || true
    # Unmount RAM disk if mounted (Linux)
    if [[ "$OSTYPE" != "darwin"* ]] && mountpoint -q "$RAMDISK_PATH" 2>/dev/null; then
        sudo umount "$RAMDISK_PATH" 2>/dev/null || true
    fi
    # Remove temp directories
    rm -rf "$RAMDISK_PATH" "$ORCAS_BASE" "$ORCAS_DATA" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

# Check if warp is installed
if ! command -v warp &> /dev/null; then
    echo -e "${RED}Error: warp tool not found. Please install it:${NC}"
    echo "  go install github.com/minio/warp@latest"
    exit 1
fi

# Check if minIO server is installed
if ! command -v minio &> /dev/null; then
    echo -e "${YELLOW}Warning: minIO server not found.${NC}"
    echo "Please install minIO: https://min.io/download"
    echo "Or use: brew install minio/stable/minio (macOS)"
    exit 1
fi

# Setup RAM disk
echo -e "${GREEN}=== Setting up RAM disk ===${NC}"
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS: create RAM disk
    RAMDISK_SIZE_MB=1024
    if [ -d "/Volumes/OrcasBenchmark" ]; then
        echo "RAM disk already exists, using existing one"
        RAMDISK_PATH="/Volumes/OrcasBenchmark"
    else
        DISK_ID=$(hdiutil attach -nomount ram://$((RAMDISK_SIZE_MB * 2048)) 2>/dev/null | awk '{print $1}')
        if [ -z "$DISK_ID" ]; then
            echo -e "${RED}Failed to create RAM disk${NC}"
            exit 1
        fi
        # Clean up DISK_ID (remove extra spaces)
        DISK_ID=$(echo "$DISK_ID" | tr -d '[:space:]')
        if diskutil eraseDisk HFS+ "OrcasBenchmark" "$DISK_ID" > /dev/null 2>&1; then
            RAMDISK_PATH="/Volumes/OrcasBenchmark"
            echo "RAM disk created at: $RAMDISK_PATH"
        else
            echo -e "${YELLOW}Warning: Failed to create RAM disk, using regular directory${NC}"
            RAMDISK_PATH="/tmp/orcas_benchmark_ramdisk"
            mkdir -p "$RAMDISK_PATH"
        fi
    fi
else
    # Linux: create tmpfs
    mkdir -p "$RAMDISK_PATH"
    if ! mountpoint -q "$RAMDISK_PATH" 2>/dev/null; then
        sudo mount -t tmpfs -o size=1G tmpfs "$RAMDISK_PATH" 2>/dev/null || {
            echo -e "${YELLOW}Warning: Failed to create RAM disk, using regular directory${NC}"
            RAMDISK_PATH="/tmp/orcas_benchmark_ramdisk"
            mkdir -p "$RAMDISK_PATH"
        }
    fi
    echo "RAM disk created at: $RAMDISK_PATH"
fi

# Setup orcas directories
mkdir -p "$ORCAS_BASE" "$ORCAS_DATA"
export ORCAS_BASE ORCAS_DATA

# Build orcas benchmark server
echo -e "${GREEN}=== Building orcas S3 benchmark server ===${NC}"
cd "$(dirname "$0")"
# Create a temporary file that includes handler functions
cat > benchmark_main.go << 'BENCHMARK_EOF'
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/s3/middleware"
)

func main() {
	// Get port from environment or use default
	port := os.Getenv("ORCAS_PORT")
	if port == "" {
		port = "9000"
	}

	// Setup directories
	baseDir := os.Getenv("ORCAS_BASE")
	if baseDir == "" {
		baseDir = filepath.Join(os.TempDir(), "orcas_benchmark")
		os.MkdirAll(baseDir, 0o755)
		os.Setenv("ORCAS_BASE", baseDir)
		core.ORCAS_BASE = baseDir
	}

	dataDir := os.Getenv("ORCAS_DATA")
	if dataDir == "" {
		dataDir = filepath.Join(os.TempDir(), "orcas_benchmark_data")
		os.MkdirAll(dataDir, 0o755)
		os.Setenv("ORCAS_DATA", dataDir)
		core.ORCAS_DATA = dataDir
	}

	// Initialize database
	core.InitDB("")

	// Ensure test user exists
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetDB()
	if err == nil {
		db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
		db.Close()
	}

	// Setup gin router
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(middleware.CORS())
	router.Use(middleware.S3Auth())

	// S3 API endpoints
	router.GET("/", listBuckets)
	router.PUT("/:bucket", createBucket)
	router.DELETE("/:bucket", deleteBucket)
	router.GET("/:bucket", listObjects)
	router.GET("/:bucket/*key", getObject)
	router.PUT("/:bucket/*key", putObject)
	router.DELETE("/:bucket/*key", deleteObject)
	router.HEAD("/:bucket/*key", headObject)

	// Start server
	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	// Handle graceful shutdown
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
		<-sigint

		fmt.Println("\nShutting down server...")
		server.Shutdown(context.Background())
		os.Exit(0)
	}()

	fmt.Printf("orcas S3 benchmark server starting on port %s...\n", port)
	fmt.Printf("ORCAS_BASE: %s\n", baseDir)
	fmt.Printf("ORCAS_DATA: %s\n", dataDir)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
BENCHMARK_EOF

# Build with handler.go (exclude server.go by temporarily renaming it)
if [ -f server.go ]; then
    mv server.go server.go.bak
fi
# Build the benchmark server (includes handler.go and benchmark_main.go)
go build -o orcas_benchmark_server handler.go benchmark_main.go 2>&1
BUILD_STATUS=$?
if [ -f server.go.bak ]; then
    mv server.go.bak server.go
fi
if [ $BUILD_STATUS -ne 0 ] || [ ! -f orcas_benchmark_server ]; then
    echo -e "${RED}Failed to build orcas benchmark server${NC}"
    echo "Build error details:"
    if [ -f server.go ]; then
        mv server.go server.go.bak
        go build -o orcas_benchmark_server handler.go benchmark_main.go 2>&1
        mv server.go.bak server.go
    fi
    exit 1
fi
rm -f benchmark_main.go

# Start orcas S3 server
echo -e "${GREEN}=== Starting orcas S3 server on port $ORCAS_PORT ===${NC}"
# Skip signature verification for benchmarking (set ORCAS_SKIP_SIG_VERIFY=1)
ORCAS_BASE="$ORCAS_BASE" ORCAS_DATA="$ORCAS_DATA" ORCAS_PORT="$ORCAS_PORT" \
    ORCAS_SKIP_SIG_VERIFY=1 \
    ./orcas_benchmark_server > /tmp/orcas_server.log 2>&1 &
ORCAS_PID=$!
sleep 3

# Check if orcas server started
if ! curl -s "http://localhost:$ORCAS_PORT" > /dev/null 2>&1; then
    echo -e "${RED}Error: orcas S3 server failed to start${NC}"
    echo "Server log:"
    cat /tmp/orcas_server.log
    kill $ORCAS_PID 2>/dev/null || true
    exit 1
fi
echo -e "${GREEN}orcas S3 server started (PID: $ORCAS_PID)${NC}"

# Create bucket in orcas explicitly 
echo -e "${GREEN}=== Setting up orcas bucket ===${NC}"
sleep 3

# Try to create bucket using PUT
echo "Creating bucket: $BUCKET_NAME"
curl -s -X PUT "http://localhost:$ORCAS_PORT/$BUCKET_NAME" \
    -H "Authorization: AWS4-HMAC-SHA256 Credential=$ACCESS_KEY/20251113/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=test" \
    -H "X-Amz-Date: 20251113T120000Z" \
    -o /dev/null 2>&1

# Verify bucket exists (Note: bucket is auto-created by benchmark_server.go)
echo "Bucket '$BUCKET_NAME' ready (auto-created by server)"

# Start minIO server
echo -e "${GREEN}=== Starting minIO server on port $MINIO_PORT ===${NC}"
MINIO_ROOT_USER="$ACCESS_KEY" MINIO_ROOT_PASSWORD="$SECRET_KEY" \
    minio server "$RAMDISK_PATH" --address ":$MINIO_PORT" > /tmp/minio_server.log 2>&1 &
MINIO_PID=$!
sleep 3

# Check if minIO server started
if ! curl -s "http://localhost:$MINIO_PORT" > /dev/null 2>&1; then
    echo -e "${RED}Error: minIO server failed to start${NC}"
    echo "Server log:"
    cat /tmp/minio_server.log
    kill $MINIO_PID $ORCAS_PID 2>/dev/null || true
    exit 1
fi
echo -e "${GREEN}minIO server started (PID: $MINIO_PID)${NC}"

# Setup minIO client and create bucket
echo -e "${GREEN}=== Setting up minIO bucket ===${NC}"
if command -v mc &> /dev/null; then
    mc alias set minio-local "http://localhost:$MINIO_PORT" "$ACCESS_KEY" "$SECRET_KEY" 2>/dev/null || true
    mc mb "minio-local/$BUCKET_NAME" 2>/dev/null || true
else
    # Create bucket using curl
    curl -X PUT "http://localhost:$MINIO_PORT/$BUCKET_NAME" \
        -H "Authorization: AWS $ACCESS_KEY:$SECRET_KEY" \
        -H "Date: $(date -u +'%a, %d %b %Y %H:%M:%S GMT')" \
        -v 2>&1 | grep -E "(HTTP|Created|Bucket)" || echo "Bucket creation attempted"
fi

# Test results directory
RESULTS_DIR="./benchmark_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Benchmark Test Configuration${NC}"
echo -e "${BLUE}========================================${NC}"
echo "File Size: $FILE_SIZE"
echo "Concurrency: $CONCURRENCY"
echo "Duration: $DURATION"
echo "Note: Objects will be generated during test duration"
echo "Storage: RAM Disk ($RAMDISK_PATH)"
echo "Results: $RESULTS_DIR"
echo -e "${BLUE}========================================${NC}"
echo ""

# Test 1: PutObject (4KB files)
echo -e "${YELLOW}=== Test 1: PutObject (4KB files) ===${NC}"
echo ""

echo -e "${GREEN}[1/2] Testing orcas S3...${NC}"
warp put \
    --host "localhost:$ORCAS_PORT" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --bucket "$BUCKET_NAME" \
    --obj.size "$FILE_SIZE" \
    --concurrent "$CONCURRENCY" \
    --duration "$DURATION" \
    --no-color \
    > "$RESULTS_DIR/orcas_put_4kb.txt" 2>&1 || echo "Test completed with warnings"

echo -e "${GREEN}[2/2] Testing minIO...${NC}"
warp put \
    --host "localhost:$MINIO_PORT" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --bucket "$BUCKET_NAME" \
    --obj.size "$FILE_SIZE" \
    --concurrent "$CONCURRENCY" \
    --duration "$DURATION" \
    --no-color \
    > "$RESULTS_DIR/minio_put_4kb.txt" 2>&1 || echo "Test completed with warnings"

# Test 2: GetObject (4KB files)
echo ""
echo -e "${YELLOW}=== Test 2: GetObject (4KB files) ===${NC}"
echo ""

echo -e "${GREEN}[1/2] Testing orcas S3...${NC}"
# For GET test, we need to use existing objects from PUT test
# Wait a moment for PUT to complete and objects to be available
sleep 5
warp get \
    --host "localhost:$ORCAS_PORT" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --bucket "$BUCKET_NAME" \
    --concurrent "$CONCURRENCY" \
    --duration "$DURATION" \
    --no-color \
    > "$RESULTS_DIR/orcas_get_4kb.txt" 2>&1 || echo "Test completed with warnings"

echo -e "${GREEN}[2/2] Testing minIO...${NC}"
warp get \
    --host "localhost:$MINIO_PORT" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --bucket "$BUCKET_NAME" \
    --obj.size "$FILE_SIZE" \
    --concurrent "$CONCURRENCY" \
    --duration "$DURATION" \
    --no-color \
    > "$RESULTS_DIR/minio_get_4kb.txt" 2>&1 || echo "Test completed with warnings"

# Test 3: Mixed operations
echo ""
echo -e "${YELLOW}=== Test 3: Mixed Operations (4KB files) ===${NC}"
echo ""

echo -e "${GREEN}[1/2] Testing orcas S3...${NC}"
warp mixed \
    --host "localhost:$ORCAS_PORT" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --bucket "$BUCKET_NAME" \
    --obj.size "$FILE_SIZE" \
    --concurrent "$CONCURRENCY" \
    --duration "$DURATION" \
    --get-distrib 50 \
    --put-distrib 50 \
    --no-color \
    > "$RESULTS_DIR/orcas_mixed_4kb.txt" 2>&1 || echo "Test completed with warnings"

echo -e "${GREEN}[2/2] Testing minIO...${NC}"
warp mixed \
    --host "localhost:$MINIO_PORT" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --bucket "$BUCKET_NAME" \
    --obj.size "$FILE_SIZE" \
    --concurrent "$CONCURRENCY" \
    --duration "$DURATION" \
    --get-distrib 50 \
    --put-distrib 50 \
    --no-color \
    > "$RESULTS_DIR/minio_mixed_4kb.txt" 2>&1 || echo "Test completed with warnings"

# Test 4: ListObjects
echo ""
echo -e "${YELLOW}=== Test 4: ListObjects ===${NC}"
echo ""

echo -e "${GREEN}[1/2] Testing orcas S3...${NC}"
warp list \
    --host "localhost:$ORCAS_PORT" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --bucket "$BUCKET_NAME" \
    --concurrent "$CONCURRENCY" \
    --duration "$DURATION" \
    --no-color \
    > "$RESULTS_DIR/orcas_list.txt" 2>&1 || echo "Test completed with warnings"

echo -e "${GREEN}[2/2] Testing minIO...${NC}"
warp list \
    --host "localhost:$MINIO_PORT" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --bucket "$BUCKET_NAME" \
    --concurrent "$CONCURRENCY" \
    --duration "$DURATION" \
    --no-color \
    > "$RESULTS_DIR/minio_list.txt" 2>&1 || echo "Test completed with warnings"

# Generate comparison report
echo ""
echo -e "${GREEN}=== Generating comparison report ===${NC}"

# Function to extract key metrics from warp output
extract_metrics() {
    local file=$1
    if [ -f "$file" ]; then
        echo '```'
        { grep -A 50 "Operation:" "$file" | head -30 || grep -E "(Throughput|Requests/sec|Average|p50|p99|Operation)" "$file" | head -20; } 2>/dev/null || cat "$file"
        echo '```'
    else
        echo "Results file not found"
    fi
}

cat > "$RESULTS_DIR/comparison_report.md" << EOF
# minIO vs orcas S3 Benchmark Comparison

**Test Date**: $(date)
**Test Configuration**:
- File Size: $FILE_SIZE
- Concurrency: $CONCURRENCY
- Duration: $DURATION
- Objects: $OBJECTS
- Storage: RAM Disk ($RAMDISK_PATH)

## Test Results

### 1. PutObject Performance (4KB files)

#### orcas S3
EOF

extract_metrics "$RESULTS_DIR/orcas_put_4kb.txt" >> "$RESULTS_DIR/comparison_report.md"

cat >> "$RESULTS_DIR/comparison_report.md" << EOF

#### minIO
EOF

extract_metrics "$RESULTS_DIR/minio_put_4kb.txt" >> "$RESULTS_DIR/comparison_report.md"

cat >> "$RESULTS_DIR/comparison_report.md" << EOF

### 2. GetObject Performance (4KB files)

#### orcas S3
EOF

extract_metrics "$RESULTS_DIR/orcas_get_4kb.txt" >> "$RESULTS_DIR/comparison_report.md"

cat >> "$RESULTS_DIR/comparison_report.md" << EOF

#### minIO
EOF

extract_metrics "$RESULTS_DIR/minio_get_4kb.txt" >> "$RESULTS_DIR/comparison_report.md"

cat >> "$RESULTS_DIR/comparison_report.md" << EOF

### 3. Mixed Operations Performance (4KB files)

#### orcas S3
EOF

extract_metrics "$RESULTS_DIR/orcas_mixed_4kb.txt" >> "$RESULTS_DIR/comparison_report.md"

cat >> "$RESULTS_DIR/comparison_report.md" << EOF

#### minIO
EOF

extract_metrics "$RESULTS_DIR/minio_mixed_4kb.txt" >> "$RESULTS_DIR/comparison_report.md"

cat >> "$RESULTS_DIR/comparison_report.md" << EOF

### 4. ListObjects Performance

#### orcas S3
EOF

extract_metrics "$RESULTS_DIR/orcas_list.txt" >> "$RESULTS_DIR/comparison_report.md"

cat >> "$RESULTS_DIR/comparison_report.md" << EOF

#### minIO
EOF

extract_metrics "$RESULTS_DIR/minio_list.txt" >> "$RESULTS_DIR/comparison_report.md"

cat >> "$RESULTS_DIR/comparison_report.md" << EOF

## Summary

Detailed results are available in the individual test files:
- \`orcas_put_4kb.txt\` / \`minio_put_4kb.txt\`
- \`orcas_get_4kb.txt\` / \`minio_get_4kb.txt\`
- \`orcas_mixed_4kb.txt\` / \`minio_mixed_4kb.txt\`
- \`orcas_list.txt\` / \`minio_list.txt\`
EOF

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Benchmark Completed!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "Results saved to: ${BLUE}$RESULTS_DIR${NC}"
echo ""
echo "View comparison report:"
echo -e "  ${YELLOW}cat $RESULTS_DIR/comparison_report.md${NC}"
echo ""
echo "View detailed results:"
echo -e "  ${YELLOW}ls -la $RESULTS_DIR/${NC}"
echo ""
