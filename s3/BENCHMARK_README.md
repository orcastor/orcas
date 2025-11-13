# minIO vs orcas S3 Benchmark

使用 minIO 的 warp benchmark 工具对比测试 minIO 和 orcas S3 的性能，主要测试 4KB 小文件，在内存盘上运行。

## 前置要求

1. **安装 warp 工具**：
   ```bash
   go install github.com/minio/warp@latest
   ```

2. **安装 minIO 服务器**：
   ```bash
   # macOS
   brew install minio/stable/minio
   
   # Linux
   wget https://dl.min.io/server/minio/release/linux-amd64/minio
   chmod +x minio
   sudo mv minio /usr/local/bin/
   ```

3. **（可选）安装 minIO 客户端**：
   ```bash
   # macOS
   brew install minio/stable/mc
   
   # Linux
   wget https://dl.min.io/client/mc/release/linux-amd64/mc
   chmod +x mc
   sudo mv mc /usr/local/bin/
   ```

## 使用方法

### 运行完整测试套件

```bash
cd s3
./benchmark_minio_vs_orcas.sh
```

脚本会自动：
1. 创建内存盘（macOS 使用 hdiutil，Linux 使用 tmpfs）
2. 启动 orcas S3 服务器（端口 9000）
3. 启动 minIO 服务器（端口 9001）
4. 运行以下测试：
   - **PutObject**：4KB 文件上传性能
   - **GetObject**：4KB 文件下载性能
   - **Mixed Operations**：混合读写操作（50% GET, 50% PUT）
   - **ListObjects**：列出对象性能
5. 生成对比报告

### 测试配置

可以通过修改脚本中的变量来调整测试参数：

```bash
FILE_SIZE="4KB"        # 文件大小
CONCURRENCY=10         # 并发数
DURATION="30s"         # 测试持续时间
OBJECTS=1000           # 对象数量
ORCAS_PORT=9000        # orcas S3 端口
MINIO_PORT=9001        # minIO 端口
```

### 单独运行 orcas S3 服务器

如果需要单独测试 orcas S3：

```bash
cd s3
go build -o orcas_benchmark_server benchmark_server.go
ORCAS_BASE=/tmp/orcas ORCAS_DATA=/tmp/orcas_data ORCAS_PORT=9000 ./orcas_benchmark_server
```

然后使用 warp 工具测试：

```bash
warp put \
    --host "http://localhost:9000" \
    --access-key "minioadmin" \
    --secret-key "minioadmin" \
    --bucket "test-bucket" \
    --objects 1000 \
    --obj.size "4KB" \
    --concurrent 10 \
    --duration "30s"
```

## 测试结果

测试结果会保存在 `benchmark_results_YYYYMMDD_HHMMSS/` 目录下：

- `orcas_put_4kb.txt` / `minIO_put_4kb.txt` - PutObject 测试结果
- `orcas_get_4kb.txt` / `minIO_get_4kb.txt` - GetObject 测试结果
- `orcas_mixed_4kb.txt` / `minIO_mixed_4kb.txt` - 混合操作测试结果
- `orcas_list.txt` / `minIO_list.txt` - ListObjects 测试结果
- `comparison_report.md` - 对比报告

## 查看结果

```bash
# 查看对比报告
cat benchmark_results_*/comparison_report.md

# 查看详细测试结果
cat benchmark_results_*/orcas_put_4kb.txt
cat benchmark_results_*/minio_put_4kb.txt
```

## 注意事项

1. **内存盘大小**：默认创建 1GB 内存盘，可根据需要调整
2. **端口冲突**：确保 9000 和 9001 端口未被占用
3. **权限要求**：Linux 上创建 tmpfs 需要 sudo 权限
4. **清理**：脚本退出时会自动清理临时文件和进程

## 故障排除

### warp 工具未找到
```bash
export PATH=$PATH:$(go env GOPATH)/bin
```

### minIO 服务器启动失败
检查端口是否被占用：
```bash
lsof -i :9001
```

### orcas S3 服务器启动失败
查看服务器日志：
```bash
cat /tmp/orcas_server.log
```

### 内存盘创建失败
- macOS：检查磁盘空间
- Linux：确保有 sudo 权限

## 性能指标说明

warp 工具会输出以下关键指标：

- **Throughput**：吞吐量（MB/s 或 ops/sec）
- **Average**：平均延迟
- **p50/p99**：50% 和 99% 分位延迟
- **Requests/sec**：每秒请求数
- **Errors**：错误数量

## 示例输出

```
Operation: PUT
Throughput: 1583.77 obj/s
Average: 631.40µs
p50: 580.20µs
p99: 1.23ms
```

