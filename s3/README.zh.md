# S3 API 服务

基于 ego 框架实现的 S3 兼容接口服务。

- [English](README.md) | [中文](README.zh.md)

## 功能特性

实现了以下 S3 兼容的 API 接口：

### 基础对象操作

- `GET /` - ListBuckets (列出所有 bucket)
- `PUT /{bucket}` - CreateBucket (创建 bucket)
- `DELETE /{bucket}` - DeleteBucket (删除 bucket)
- `GET /{bucket}` - ListObjects (列出 bucket 中的对象)
- `GET /{bucket}/{key}` - GetObject (获取对象)
- `PUT /{bucket}/{key}` - PutObject (上传对象)
- `PUT /{bucket}/{key}` with `x-amz-copy-source` header - CopyObject (复制对象)
- `PUT /{bucket}/{key}` with `x-amz-move-source` header - MoveObject (移动对象，非标准扩展)
- `DELETE /{bucket}/{key}` - DeleteObject (删除对象)
- `HEAD /{bucket}/{key}` - HeadObject (获取对象元数据)

### 范围读取 (Range Read)

支持通过 `Range` 请求头读取对象的指定字节范围：

- 单块范围读取：读取单个数据块内的数据
- 多块范围读取：跨多个数据块读取数据
- 前缀读取：从指定位置读取到文件末尾
- 后缀读取：读取文件的最后 N 个字节

**使用示例：**

```bash
# 读取字节范围 0-1023 (前 1KB)
curl -X GET http://localhost:7983/my-bucket/file.txt \
  -H "Authorization: Bearer <token>" \
  -H "Range: bytes=0-1023"

# 读取从 1024 到文件末尾
curl -X GET http://localhost:7983/my-bucket/file.txt \
  -H "Authorization: Bearer <token>" \
  -H "Range: bytes=1024-"

# 读取最后 512 字节
curl -X GET http://localhost:7983/my-bucket/file.txt \
  -H "Authorization: Bearer <token>" \
  -H "Range: bytes=-512"
```

### 分片上传 (Multipart Upload)

支持大文件的分片上传，适用于上传大文件或网络不稳定的场景：

- `POST /{bucket}/{key}?uploads` - InitiateMultipartUpload (初始化分片上传)
- `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}` - UploadPart (上传分片)
- `POST /{bucket}/{key}?uploadId={id}` - CompleteMultipartUpload (完成分片上传)
- `DELETE /{bucket}/{key}?uploadId={id}` - AbortMultipartUpload (取消分片上传)
- `GET /{bucket}?uploads` - ListMultipartUploads (列出进行中的分片上传)
- `GET /{bucket}/{key}?uploadId={id}` - ListParts (列出已上传的分片)

**使用示例：**

```bash
# 1. 初始化分片上传
curl -X POST "http://localhost:7983/my-bucket/large-file.zip?uploads" \
  -H "Authorization: Bearer <token>"

# 响应包含 UploadId，例如：
# <InitiateMultipartUploadResult>
#   <UploadId>abc123</UploadId>
# </InitiateMultipartUploadResult>

# 2. 上传分片 (partNumber 从 1 开始)
curl -X PUT "http://localhost:7983/my-bucket/large-file.zip?partNumber=1&uploadId=abc123" \
  -H "Authorization: Bearer <token>" \
  --data-binary @part1.bin

# 3. 上传更多分片
curl -X PUT "http://localhost:7983/my-bucket/large-file.zip?partNumber=2&uploadId=abc123" \
  -H "Authorization: Bearer <token>" \
  --data-binary @part2.bin

# 4. 完成分片上传
curl -X POST "http://localhost:7983/my-bucket/large-file.zip?uploadId=abc123" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/xml" \
  -d '<CompleteMultipartUpload>
    <Part>
      <PartNumber>1</PartNumber>
      <ETag>"etag1"</ETag>
    </Part>
    <Part>
      <PartNumber>2</PartNumber>
      <ETag>"etag2"</ETag>
    </Part>
  </CompleteMultipartUpload>'
```

**分片上传特性：**

- 支持任意大小的分片（建议使用 5MB 或更大）
- 支持最多 10,000 个分片
- 分片可以按任意顺序上传
- 支持替换已上传的分片
- 每个分片会根据 bucket 的 chunkSize 配置自动分块存储
- 如果分片大小超过 chunkSize，会自动拆分为多个数据块

## 认证方式

支持两种认证方式：

1. **JWT Token 认证**
   - 通过 `Authorization` header: `Bearer <token>`
   - 或通过 query 参数: `?token=<token>`
   - 或通过 form 参数: `token=<token>`

2. **AWS Signature V4 认证** (已实现)
   - 通过 `Authorization` header: `AWS4-HMAC-SHA256 Credential=...`
   - 需要 `X-Amz-Date` header
   - 需要 `X-Amz-Content-Sha256` header (对于有请求体的请求)
   - 使用标准的 AWS Signature V4 签名算法

### 配置 AWS 访问密钥

在使用 AWS Signature V4 认证之前，需要先配置访问密钥：

```go
import "github.com/orcastor/orcas/s3/middleware"

// 创建并设置访问密钥
credential := &middleware.AWSCredential{
    AccessKeyID:     "your-access-key-id",
    SecretAccessKey: "your-secret-access-key",
    UserID:          123, // 关联的用户ID
}

// 使用内存存储（生产环境建议使用数据库存储）
store := middleware.NewInMemoryCredentialStore()
store.PutCredential(credential)
middleware.SetCredentialStore(store)
```

### AWS Signature V4 使用示例

使用 AWS SDK 或兼容的 S3 客户端时，配置访问密钥和区域即可：

```bash
# 使用 AWS CLI
export AWS_ACCESS_KEY_ID=your-access-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-access-key
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url=http://localhost:7983 s3 ls
aws --endpoint-url=http://localhost:7983 s3 cp file.txt s3://my-bucket/file.txt
```

## 配置

配置文件 `config.toml`:

```toml
[server.http]
    port = 7983
    host = "0.0.0.0"
```

## 运行

```bash
# 设置环境变量
export EGO_DEBUG=true
export EGO_LOG_EXTRA_KEYS=uid
# 注意：ORCAS_BASE 和 ORCAS_DATA 不再作为环境变量使用。
# 路径可以通过 context 使用 Path2Ctx 或 Config2Ctx 进行配置。
export ORCAS_SECRET=xxxxxxxx

# 使用egoctl运行
cd s3
egoctl run --runargs --config=config.toml

# 或直接使用go run
go run . --config=config.toml
```

## API 使用示例

### 1. ListBuckets

```bash
curl -X GET http://localhost:7983/ \
  -H "Authorization: Bearer <token>"
```

### 2. CreateBucket

```bash
curl -X PUT http://localhost:7983/my-bucket \
  -H "Authorization: Bearer <token>"
```

### 3. ListObjects

```bash
curl -X GET "http://localhost:7983/my-bucket?prefix=path/to/&max-keys=100" \
  -H "Authorization: Bearer <token>"
```

### 4. PutObject

```bash
curl -X PUT http://localhost:7983/my-bucket/path/to/file.txt \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @file.txt
```

### 5. GetObject

```bash
curl -X GET http://localhost:7983/my-bucket/path/to/file.txt \
  -H "Authorization: Bearer <token>" \
  -o file.txt
```

### 6. DeleteObject

```bash
curl -X DELETE http://localhost:7983/my-bucket/path/to/file.txt \
  -H "Authorization: Bearer <token>"
```

### 7. HeadObject

```bash
curl -X HEAD http://localhost:7983/my-bucket/path/to/file.txt \
  -H "Authorization: Bearer <token>"
```

### 8. CopyObject

```bash
# 在同一 bucket 内复制对象
curl -X PUT http://localhost:7983/my-bucket/path/to/copy.txt \
  -H "Authorization: Bearer <token>" \
  -H "x-amz-copy-source: /my-bucket/path/to/file.txt"

# 在不同 bucket 之间复制对象
curl -X PUT http://localhost:7983/my-bucket2/path/to/copy.txt \
  -H "Authorization: Bearer <token>" \
  -H "x-amz-copy-source: /my-bucket/path/to/file.txt"
```

### 9. MoveObject (非标准扩展)

```bash
# 在同一 bucket 内移动对象（重命名/移动）
curl -X PUT http://localhost:7983/my-bucket/path/to/new-name.txt \
  -H "Authorization: Bearer <token>" \
  -H "x-amz-move-source: /my-bucket/path/to/old-name.txt"

# 在不同 bucket 之间移动对象
curl -X PUT http://localhost:7983/my-bucket2/path/to/moved.txt \
  -H "Authorization: Bearer <token>" \
  -H "x-amz-move-source: /my-bucket/path/to/file.txt"
```

**注意**：MoveObject 是非标准扩展。它会将对象从源位置移动到目标位置并删除源对象。对于同一 bucket 内的移动，它使用高效的元数据更新。对于跨 bucket 移动，它会复制数据然后删除源对象。

## 响应格式

### 成功响应

- ListBuckets: XML 格式的 bucket 列表
- ListObjects: XML 格式的对象列表
- GetObject: 二进制数据流
- PutObject/DeleteObject: 204 No Content
- HeadObject: 仅返回 HTTP headers

### 错误响应

所有错误都返回 XML 格式的 S3 错误响应：

```xml
<Error>
  <Code>ErrorCode</Code>
  <Message>Error message</Message>
</Error>
```

常见错误码：
- `AccessDenied` - 访问被拒绝
- `NoSuchBucket` - Bucket 不存在
- `NoSuchKey` - 对象不存在
- `BucketAlreadyExists` - Bucket 已存在
- `InvalidBucketName` - 无效的 bucket 名称
- `InternalError` - 内部服务器错误

## 数据存储机制

### 分块存储

- 所有数据按照 bucket 的 `ChunkSize` 配置进行分块存储（默认 4MB）
- 小文件（小于 ChunkSize）直接存储为单个数据块
- 大文件（大于 ChunkSize）自动拆分为多个数据块，sn 从 0 开始递增
- 分片上传时，每个分片也会根据 ChunkSize 进行分块处理

### 批量写入优化

S3 服务支持小文件的批量写入优化：

- **自动启用**：对于小于 1MB 的新文件，自动使用批量写入
- **批量打包**：多个小文件的数据块打包到一个数据块中
- **批量元数据**：批量写入 DataInfo 和 ObjectInfo，减少数据库操作
- **配置控制**：通过环境变量 `ORCAS_BATCH_WRITE_ENABLED` 控制是否启用（默认启用）

#### 配置选项

```bash
# 启用/禁用批量写入（默认启用）
export ORCAS_BATCH_WRITE_ENABLED=true  # 或 false

# 批量写入缓冲区窗口时间（默认10秒）
export ORCAS_WRITE_BUFFER_WINDOW_SEC=10

# 最大缓冲区大小（默认8MB）
export ORCAS_MAX_WRITE_BUFFER_SIZE=8388608

# 最大缓冲写入次数（默认2048）
export ORCAS_MAX_WRITE_BUFFER_COUNT=2048
```

#### 批量写入优势

- **减少 I/O 操作**：多个小文件打包写入，减少磁盘 I/O
- **批量元数据写入**：批量写入 DataInfo 和 ObjectInfo，减少数据库事务
- **提高吞吐量**：小文件写入性能提升 50-90%

## 性能测试

详细的性能测试用例和报告请参考：

- `test/performance_test.go` - 性能测试实现
- `test/multipart_and_range_test.go` - 分片上传和范围读取测试用例
- [性能测试报告（中文）](docs/PERFORMANCE_TEST_REPORT.zh.md) - 完整的性能分析

### 快速运行测试

```bash
# 运行所有测试
go test -v

# 运行范围读取测试
go test -v -run TestRangeRead

# 运行分片上传测试
go test -v -run TestMultipartUpload

# 运行性能基准测试
go test -bench=. -benchmem
```

## 注意事项

1. 所有 bucket 和对象都基于 orcas 的核心存储系统实现
2. S3 的 key 路径会自动转换为 orcas 的目录结构
3. 上传对象时会自动创建必要的父目录
4. 当前实现使用 JWT token 认证，需要先通过 `/api/login` 接口获取 token
5. 性能测试使用临时目录，不会影响生产环境
6. 批量写入仅适用于新文件（小于 1MB），更新现有文件使用普通写入路径
7. 分片上传时，每个分片会根据 bucket 的 ChunkSize 自动分块，确保数据存储的一致性
8. 范围读取支持跨多个数据块读取，自动处理数据块的拼接

