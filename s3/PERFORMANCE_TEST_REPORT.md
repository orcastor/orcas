# S3 Performance Test Report

Generated at: 2025-11-12T17:39:22+08:00

## Single Operation Performance

| Operation | Data Size | Ops/sec | Avg Latency | Throughput (MB/s) |
|-----------|-----------|---------|-------------|-------------------|
| PutObject | 1KB | 431.44 | 2.317792ms | 0.42 |
| PutObject | 1MB | 236.94 | 4.22048ms | 236.94 |
| PutObject | 10MB | 45.69 | 21.887763ms | 456.88 |

## Concurrent Performance

| Operation | Concurrency | Data Size | Ops/sec | Avg Latency | Throughput (MB/s) |
|-----------|-------------|-----------|---------|-------------|-------------------|
| PutObject | 1 | 1MB | 233.66 | 4.279775ms | 233.66 |
| PutObject | 10 | 1MB | 706.61 | 1.415207ms | 706.61 |
| PutObject | 50 | 1MB | 606.32 | 1.649299ms | 606.32 |
| PutObject | 100 | 1MB | 514.99 | 1.941802ms | 514.99 |
