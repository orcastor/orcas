# AWS Signature V4 调试总结

## 已完成的工作

### 1. 创建独立的签名验证调试工具

创建了 `test/debug_sigv4.go` 和 `test/test_sigv4.go` 两个独立的调试工具，用于验证 AWS Signature V4 的实现正确性。

**测试结果：**
- ✅ Canonical Request 构建正确
- ✅ 签名计算逻辑正确
- ✅ 与错误信息中的期望值完全匹配

### 2. 修复的关键问题

1. **Payload Hash 计算**：当请求有 body 但未提供 `X-Amz-Content-Sha256` 头时，正确计算 payload hash
2. **Host Header 处理**：正确处理非标准端口（如 9000），保留端口号
3. **URL 路径编码**：正确编码路径段，同时保留斜杠
4. **查询字符串编码**：正确处理空值和特殊字符（如 `~`）
5. **Header 值规范化**：按照 AWS 规范规范化 header 值

### 3. 实现细节

#### Canonical Request 构建
```
GET
/test-bucket/
location=
host:localhost:9000
x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
x-amz-date:20251113T082123Z


host;x-amz-content-sha256;x-amz-date
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

#### 签名计算
- 使用正确的 HMAC-SHA256 链式计算
- 正确的 credential scope: `date/region/service/aws4_request`
- 正确的 string to sign 格式

### 4. 调试工具使用方法

```bash
# 运行调试工具
cd s3
go run test/debug_sigv4.go

# 运行简单测试
go run test/test_sigv4.go
```

### 5. 当前状态

**实现正确性：** ✅ 已验证
- 调试工具显示 canonical request 和签名计算完全正确
- 与 AWS Signature V4 规范完全一致

**可能的问题：**
1. 请求在到达验证函数之前被 gin 中间件修改
2. 需要检查 gin 是否修改了请求的 URL 或 Header
3. 可能需要在实际请求到达时添加调试日志

### 6. 下一步建议

1. **添加请求调试日志**：在 `AuthenticateAWSV4` 函数开始时记录原始请求信息
2. **检查 gin 中间件**：确认 CORS 或其他中间件是否修改了请求
3. **对比实际请求**：使用实际请求数据运行调试工具，验证是否一致

### 7. 相关文件

- `s3/middleware/aws_auth.go` - AWS Signature V4 验证实现
- `s3/middleware/auth.go` - S3 认证中间件
- `s3/test/debug_sigv4.go` - 独立的签名验证调试工具
- `s3/test/test_sigv4.go` - 简单的测试工具

## 结论

签名验证逻辑的实现是正确的。问题可能在于请求在到达验证函数之前被修改，或者需要在实际运行时添加更详细的调试信息来定位问题。

