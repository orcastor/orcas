# S3 API Service

S3-compatible API service implemented based on the ego framework.

- [English](README.md) | [中文](README.zh.md)

## Features

The following S3-compatible API interfaces are implemented:

### Basic Object Operations

- `GET /` - ListBuckets (List all buckets)
- `PUT /{bucket}` - CreateBucket (Create bucket)
- `DELETE /{bucket}` - DeleteBucket (Delete bucket)
- `GET /{bucket}` - ListObjects (List objects in bucket)
- `GET /{bucket}/{key}` - GetObject (Get object)
- `PUT /{bucket}/{key}` - PutObject (Upload object)
- `PUT /{bucket}/{key}` with `x-amz-copy-source` header - CopyObject (Copy object)
- `PUT /{bucket}/{key}` with `x-amz-move-source` header - MoveObject (Move object, non-standard extension)
- `DELETE /{bucket}/{key}` - DeleteObject (Delete object)
- `HEAD /{bucket}/{key}` - HeadObject (Get object metadata)

### Range Read

Supports reading specified byte ranges of objects through the `Range` request header:

- Single chunk range read: Read data within a single data chunk
- Multi-chunk range read: Read data across multiple data chunks
- Prefix read: Read from a specified position to the end of the file
- Suffix read: Read the last N bytes of the file

**Usage Examples:**

```bash
# Read byte range 0-1023 (first 1KB)
curl -X GET http://localhost:7983/my-bucket/file.txt \
  -H "Authorization: Bearer <token>" \
  -H "Range: bytes=0-1023"

# Read from 1024 to end of file
curl -X GET http://localhost:7983/my-bucket/file.txt \
  -H "Authorization: Bearer <token>" \
  -H "Range: bytes=1024-"

# Read last 512 bytes
curl -X GET http://localhost:7983/my-bucket/file.txt \
  -H "Authorization: Bearer <token>" \
  -H "Range: bytes=-512"
```

### Multipart Upload

Supports multipart upload for large files, suitable for uploading large files or in unstable network scenarios:

- `POST /{bucket}/{key}?uploads` - InitiateMultipartUpload (Initialize multipart upload)
- `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}` - UploadPart (Upload part)
- `POST /{bucket}/{key}?uploadId={id}` - CompleteMultipartUpload (Complete multipart upload)
- `DELETE /{bucket}/{key}?uploadId={id}` - AbortMultipartUpload (Abort multipart upload)
- `GET /{bucket}?uploads` - ListMultipartUploads (List ongoing multipart uploads)
- `GET /{bucket}/{key}?uploadId={id}` - ListParts (List uploaded parts)

**Usage Examples:**

```bash
# 1. Initialize multipart upload
curl -X POST "http://localhost:7983/my-bucket/large-file.zip?uploads" \
  -H "Authorization: Bearer <token>"

# Response contains UploadId, for example:
# <InitiateMultipartUploadResult>
#   <UploadId>abc123</UploadId>
# </InitiateMultipartUploadResult>

# 2. Upload part (partNumber starts from 1)
curl -X PUT "http://localhost:7983/my-bucket/large-file.zip?partNumber=1&uploadId=abc123" \
  -H "Authorization: Bearer <token>" \
  --data-binary @part1.bin

# 3. Upload more parts
curl -X PUT "http://localhost:7983/my-bucket/large-file.zip?partNumber=2&uploadId=abc123" \
  -H "Authorization: Bearer <token>" \
  --data-binary @part2.bin

# 4. Complete multipart upload
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

**Multipart Upload Features:**

- Supports parts of any size (recommended 5MB or larger)
- Supports up to 10,000 parts
- Parts can be uploaded in any order
- Supports replacing already uploaded parts
- Each part is automatically chunked according to the bucket's chunkSize configuration
- If a part size exceeds chunkSize, it will be automatically split into multiple data chunks

## Authentication

Two authentication methods are supported:

1. **JWT Token Authentication**
   - Via `Authorization` header: `Bearer <token>`
   - Or via query parameter: `?token=<token>`
   - Or via form parameter: `token=<token>`

2. **AWS Signature V4 Authentication** (Implemented)
   - Via `Authorization` header: `AWS4-HMAC-SHA256 Credential=...`
   - Requires `X-Amz-Date` header
   - Requires `X-Amz-Content-Sha256` header (for requests with body)
   - Uses standard AWS Signature V4 signing algorithm

### Configure AWS Access Keys

Before using AWS Signature V4 authentication, you need to configure access keys:

```go
import "github.com/orcastor/orcas/s3/middleware"

// Create and set access keys
credential := &middleware.AWSCredential{
    AccessKeyID:     "your-access-key-id",
    SecretAccessKey: "your-secret-access-key",
    UserID:          123, // Associated user ID
}

// Use in-memory storage (production environments should use database storage)
store := middleware.NewInMemoryCredentialStore()
store.PutCredential(credential)
middleware.SetCredentialStore(store)
```

### AWS Signature V4 Usage Example

When using AWS SDK or compatible S3 clients, simply configure access keys and region:

```bash
# Using AWS CLI
export AWS_ACCESS_KEY_ID=your-access-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-access-key
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url=http://localhost:7983 s3 ls
aws --endpoint-url=http://localhost:7983 s3 cp file.txt s3://my-bucket/file.txt
```

## Configuration

Configuration file `config.toml`:

```toml
[server.http]
    port = 7983
    host = "0.0.0.0"
```

## Running

```bash
# Set environment variables
export EGO_DEBUG=true
export EGO_LOG_EXTRA_KEYS=uid
export ORCAS_BASE=/opt/orcas
export ORCAS_DATA=/opt/orcas_disk
export ORCAS_SECRET=xxxxxxxx

# Run using egoctl
cd s3
egoctl run --runargs --config=config.toml

# Or run directly with go run
go run . --config=config.toml
```

## API Usage Examples

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
# Copy object within same bucket
curl -X PUT http://localhost:7983/my-bucket/path/to/copy.txt \
  -H "Authorization: Bearer <token>" \
  -H "x-amz-copy-source: /my-bucket/path/to/file.txt"

# Copy object between different buckets
curl -X PUT http://localhost:7983/my-bucket2/path/to/copy.txt \
  -H "Authorization: Bearer <token>" \
  -H "x-amz-copy-source: /my-bucket/path/to/file.txt"
```

### 9. MoveObject (Non-standard Extension)

```bash
# Move object within same bucket (rename/move)
curl -X PUT http://localhost:7983/my-bucket/path/to/new-name.txt \
  -H "Authorization: Bearer <token>" \
  -H "x-amz-move-source: /my-bucket/path/to/old-name.txt"

# Move object between different buckets
curl -X PUT http://localhost:7983/my-bucket2/path/to/moved.txt \
  -H "Authorization: Bearer <token>" \
  -H "x-amz-move-source: /my-bucket/path/to/file.txt"
```

**Note**: MoveObject is a non-standard extension. It moves the object from source to destination and deletes the source. For same-bucket moves, it uses efficient metadata updates. For cross-bucket moves, it copies the data and then deletes the source.

## Response Format

### Success Response

- ListBuckets: XML format bucket list
- ListObjects: XML format object list
- GetObject: Binary data stream
- PutObject/DeleteObject: 204 No Content
- HeadObject: Returns HTTP headers only

### Error Response

All errors return XML format S3 error responses:

```xml
<Error>
  <Code>ErrorCode</Code>
  <Message>Error message</Message>
</Error>
```

Common error codes:
- `AccessDenied` - Access denied
- `NoSuchBucket` - Bucket does not exist
- `NoSuchKey` - Object does not exist
- `BucketAlreadyExists` - Bucket already exists
- `InvalidBucketName` - Invalid bucket name
- `InternalError` - Internal server error

## Data Storage Mechanism

### Chunked Storage

- All data is stored in chunks according to the bucket's `ChunkSize` configuration (default 4MB)
- Small files (smaller than ChunkSize) are stored directly as a single data chunk
- Large files (larger than ChunkSize) are automatically split into multiple data chunks, with sn starting from 0 and incrementing
- During multipart upload, each part is also chunked according to ChunkSize

### Batch Write Optimization

S3 service supports batch write optimization for small files:

- **Auto-enabled**: For new files smaller than 1MB, batch write is automatically used
- **Batch packing**: Data chunks from multiple small files are packed into a single data chunk
- **Batch metadata**: Batch write DataInfo and ObjectInfo, reducing database operations
- **Configuration control**: Controlled via environment variable `ORCAS_BATCH_WRITE_ENABLED` (enabled by default)

#### Configuration Options

```bash
# Enable/disable batch write (enabled by default)
export ORCAS_BATCH_WRITE_ENABLED=true  # or false

# Batch write buffer window time (default 10 seconds)
export ORCAS_WRITE_BUFFER_WINDOW_SEC=10

# Maximum buffer size (default 8MB)
export ORCAS_MAX_WRITE_BUFFER_SIZE=8388608

# Maximum buffer write count (default 2048)
export ORCAS_MAX_WRITE_BUFFER_COUNT=2048
```

#### Batch Write Advantages

- **Reduce I/O operations**: Multiple small files packed and written together, reducing disk I/O
- **Batch metadata write**: Batch write DataInfo and ObjectInfo, reducing database transactions
- **Improve throughput**: Small file write performance improved by 50-90%

## Performance Testing

For detailed performance test cases and reports, please refer to:

- `test/performance_test.go` - Performance test implementation
- `test/multipart_and_range_test.go` - Multipart upload and range read test cases
- [Performance Test Report (English)](docs/PERFORMANCE_TEST_REPORT.md) - Comprehensive performance analysis

### Quick Run Tests

```bash
# Run all tests
go test -v

# Run range read tests
go test -v -run TestRangeRead

# Run multipart upload tests
go test -v -run TestMultipartUpload

# Run performance benchmark tests
go test -bench=. -benchmem
```

## Notes

1. All buckets and objects are implemented based on orcas core storage system
2. S3 key paths are automatically converted to orcas directory structure
3. Parent directories are automatically created when uploading objects
4. Current implementation uses JWT token authentication, requires obtaining token through `/api/login` interface first
5. Performance tests use temporary directories and will not affect production environment
6. Batch write only applies to new files (smaller than 1MB), updating existing files uses normal write path
7. During multipart upload, each part is automatically chunked according to the bucket's ChunkSize to ensure data storage consistency
8. Range read supports reading across multiple data chunks, automatically handling data chunk concatenation
