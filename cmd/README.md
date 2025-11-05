# ORCAS 命令行工具

这是一个基于 SDK 实现的 ORCAS 文件上传下载命令行工具。

## 功能特性

- 支持上传文件和目录到 ORCAS 存储
- 支持从 ORCAS 存储下载文件和目录
- 支持通过命令行参数或配置文件（JSON）设置配置
- 支持加密、压缩、秒传等高级功能

## 编译

```bash
go build -o orcas-cli cmd/main.go
```

## 使用方法

### 基本用法

#### 上传文件或目录

```bash
# 使用命令行参数
orcas-cli -action upload -local /path/to/local/file -remote /remote/path -user orcas -pass orcas

# 使用配置文件
orcas-cli -action upload -local /path/to/local/file -remote /remote/path -config config.json
```

#### 下载文件或目录

```bash
# 使用命令行参数
orcas-cli -action download -local /path/to/save -remote /remote/path -user orcas -pass orcas

# 使用配置文件
orcas-cli -action download -local /path/to/save -remote /remote/path -config config.json
```

### 命令行参数

#### 必需参数

- `-action`: 操作类型，`upload` 或 `download`
- `-local`: 本地文件或目录路径
- `-user`: 用户名（或通过配置文件设置）
- `-pass`: 密码（或通过配置文件设置）

#### 可选参数

- `-config`: 配置文件路径（JSON 格式）
- `-remote`: 远程路径（相对于 bucket 根目录，上传时可选，下载时必需）
- `-bucket`: Bucket ID（如果不指定，使用第一个 bucket）

#### 高级配置参数

- `-datasync`: 数据同步（断电保护），`true` 或 `false`
- `-reflevel`: 秒传级别，`OFF`、`FULL`、`FAST`
- `-wisecmpr`: 智能压缩，`SNAPPY`、`ZSTD`、`GZIP`、`BR`
- `-cmprqlty`: 压缩级别（br: 0-11, gzip: -3-9, zstd: 0-10）
- `-endecway`: 加密方式，`AES256` 或 `SM4`
- `-endeckey`: 加密密钥（AES256需要>16字符，SM4需要16字符）
- `-dontsync`: 不同步的文件名通配符（用分号分隔）
- `-conflict`: 同名冲突解决方式，`COVER`、`RENAME`、`THROW`、`SKIP`
- `-nametmpl`: 重命名模板（包含 `%s`）
- `-workers`: 并发池大小（不小于16）

### 配置文件

配置文件支持 JSON 格式。命令行参数会覆盖配置文件中的对应值。

#### JSON 配置文件示例

参考 `config.example.json`

### 配置项说明

#### 秒传级别 (ref_level)

- `OFF`: 关闭秒传，每次都上传完整文件
- `FULL`: 完整文件秒传，读取整个文件计算校验值
- `FAST`: 快速秒传，先检查文件头部，匹配成功后再读取整个文件

#### 智能压缩 (wise_cmpr)

根据文件类型自动决定是否压缩：

- `SNAPPY`: Snappy 压缩
- `ZSTD`: Zstd 压缩
- `GZIP`: Gzip 压缩
- `BR`: Brotli 压缩

#### 加密方式 (endec_way)

- `AES256`: AES-256 加密（密钥长度需 > 16 字符）
- `SM4`: SM4 国密加密（密钥长度需 = 16 字符）
- 留空：不加密

#### 同名冲突解决方式 (conflict)

- `COVER`: 合并或覆盖
- `RENAME`: 重命名（使用 name_tmpl 模板）
- `THROW`: 报错
- `SKIP`: 跳过

## 使用示例

### 示例 1: 上传目录（使用配置文件）

1. 创建配置文件 `config.json`：

```json
{
  "user_name": "orcas",
  "password": "orcas",
  "data_sync": true,
  "ref_level": "FULL",
  "wise_cmpr": "GZIP",
  "cmpr_qlty": 5
}
```

2. 执行上传：

```bash
orcas-cli -action upload -local /path/to/directory -remote /my/directory -config config.json
```

### 示例 2: 下载文件（使用命令行参数）

```bash
orcas-cli -action download -local ./downloads -remote /my/file.txt -user orcas -pass orcas -reflevel FULL
```

### 示例 3: 上传并启用加密

```bash
orcas-cli -action upload \
  -local /path/to/file \
  -remote /encrypted/file \
  -user orcas \
  -pass orcas \
  -endecway AES256 \
  -endeckey "my-very-long-encryption-key-here"
```

## 环境变量

工具会使用 ORCAS 系统的环境变量配置，主要包括：

- `ORCAS_BASE`: 基础路径，用于存储元数据等基础文件
- `ORCAS_DATA`: 数据路径，用于存储数据文件

详细说明请参考 `core/const.go`。

## 注意事项

1. 上传目录时，如果远程路径不存在，会自动创建
2. 下载时，如果本地目录不存在，会自动创建
3. 如果指定的 bucket ID 不存在，工具会报错
4. 如果不指定 bucket ID，会使用第一个可用的 bucket
5. 加密密钥必须符合长度要求，否则会报错

