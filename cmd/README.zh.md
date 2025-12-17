# ORCAS 命令行工具

- [English](README.md) | [中文](README.zh.md)

这是一个基于 SDK 实现的 ORCAS 文件上传下载命令行工具。

## 功能特性

- 支持上传文件和目录到 ORCAS 存储
- 支持从 ORCAS 存储下载文件和目录
- 支持通过命令行参数或配置文件（JSON）设置配置
- 支持加密、压缩、秒传等高级功能
- 支持用户管理（需要管理员权限）：创建、更新、删除、列出用户
- 支持桶管理（需要管理员权限）：创建、删除、列出桶，支持指定桶的所有者和配额

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

- `-action`: 操作类型，`upload`、`download`、`add-user`、`update-user`、`delete-user`、`list-users`、`create-bucket`、`delete-bucket` 或 `list-buckets`
- `-local`: 本地文件或目录路径（上传/下载时必需）
- `-user`: 用户名（或通过配置文件设置）
- `-pass`: 密码（或通过配置文件设置，或通过 stdin 输入，或使用管道传递）

**密码输入方式：**
- 通过 `-pass` 参数直接指定（不推荐，密码会出现在命令行历史中）
- 通过配置文件设置（推荐用于脚本）
- 通过 stdin 交互式输入（隐藏输入，不显示字符）
- 通过管道传递（支持 `echo "password" | orcas-cli ...` 或 `password-command | orcas-cli ...`）

#### 可选参数

- `-config`: 配置文件路径（JSON 格式）
- `-remote`: 远程路径（相对于 bucket 根目录，上传时可选，下载时必需）

#### 用户管理参数

- `-userid`: 用户 ID（用于 `update-user` 和 `delete-user`）
- `-newuser`: 新用户名（用于 `add-user` 和 `update-user`）
- `-newpass`: 新密码（用于 `add-user` 和 `update-user`）
- `-username`: 用户显示名称（用于 `add-user` 和 `update-user`）
- `-role`: 用户角色，`USER` 或 `ADMIN`（用于 `add-user` 和 `update-user`）

#### 桶管理参数

- `-bucketname`: 桶名称（用于 `create-bucket`）
- `-quota`: 桶配额（字节数，用于 `create-bucket`，-1 表示无限制，默认为 -1）
- `-owner`: 桶所有者用户名或用户 ID（用于 `create-bucket`，默认为当前用户）

#### 其他可选参数

- `-bucket`: Bucket ID（如果不指定，使用第一个 bucket）

#### 高级配置参数

- `-reflevel`: 秒传级别，`OFF`、`FULL`、`FAST`
- `-cmprway`: 压缩方式（默认智能压缩），`SNAPPY`、`ZSTD`、`GZIP`、`BR`
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

#### 压缩方式 (cmpr_way)

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
  "cmpr_way": "GZIP",
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

### 示例 4: 使用交互式密码输入（隐藏输入）

```bash
# 不提供 -pass 参数，程序会提示输入密码（输入时不会显示字符）
orcas-cli -action upload -local /path/to/file -remote /file -user orcas
# 程序会提示: Password: （输入时不会显示字符）
```

### 示例 5: 使用管道传递密码

```bash
# 通过 echo 传递密码
echo "mypassword" | orcas-cli -action upload -local /path/to/file -remote /file -user orcas

# 通过密码管理工具传递密码（如 pass、1password-cli 等）
pass orcas | orcas-cli -action upload -local /path/to/file -remote /file -user orcas

# 从文件读取密码
cat password.txt | orcas-cli -action upload -local /path/to/file -remote /file -user orcas
```

## 环境变量

工具会使用 ORCAS 系统的环境变量配置，主要包括：

- `ORCAS_BASE`: 基础路径，用于存储元数据等基础文件
- `ORCAS_DATA`: 数据路径，用于存储数据文件

详细说明请参考 `core/const.go`。

## 默认管理员账户

系统初始化时会自动创建默认管理员账户：
- **用户名**: `orcas`
- **密码**: `orcas`

**重要提示**: 首次使用后，请尽快修改默认管理员密码以确保安全。

## 用户管理

用户管理功能需要管理员权限。使用管理员账号登录后，可以执行以下操作：

### 创建用户 (add-user)

```bash
# 创建普通用户
orcas-cli -action add-user -user admin -pass admin -newuser testuser -newpass testpass

# 创建管理员用户
orcas-cli -action add-user -user admin -pass admin -newuser admin2 -newpass admin2 -role ADMIN

# 创建用户并指定显示名称
orcas-cli -action add-user -user admin -pass admin -newuser testuser -newpass testpass -username "Test User"

# 使用交互式输入新用户密码（隐藏输入）
orcas-cli -action add-user -user admin -pass admin -newuser testuser
# 程序会提示: New user password: （输入时不会显示字符）

# 使用管道传递新用户密码
echo "newuserpass" | orcas-cli -action add-user -user admin -pass admin -newuser testuser
```

**参数说明：**
- `-user`: 管理员用户名（必需）
- `-pass`: 管理员密码（必需）
- `-newuser`: 新用户的用户名（必需）
- `-newpass`: 新用户的密码（必需）
- `-username`: 用户显示名称（可选，默认为用户名）
- `-role`: 用户角色，`USER` 或 `ADMIN`（可选，默认为 `USER`）

### 更新用户 (update-user)

```bash
# 更新用户密码
orcas-cli -action update-user -user admin -pass admin -userid 123 -newpass newpassword

# 更新用户名
orcas-cli -action update-user -user admin -pass admin -userid 123 -newuser newusername

# 更新用户角色
orcas-cli -action update-user -user admin -pass admin -userid 123 -role ADMIN

# 更新用户显示名称
orcas-cli -action update-user -user admin -pass admin -userid 123 -username "New Display Name"
```

**参数说明：**
- `-user`: 管理员用户名（必需）
- `-pass`: 管理员密码（必需）
- `-userid`: 要更新的用户 ID（必需）
- `-newuser`: 新用户名（可选）
- `-newpass`: 新密码（可选）
- `-username`: 新显示名称（可选）
- `-role`: 新角色，`USER` 或 `ADMIN`（可选）

### 删除用户 (delete-user)

```bash
orcas-cli -action delete-user -user admin -pass admin -userid 123
```

**参数说明：**
- `-user`: 管理员用户名（必需）
- `-pass`: 管理员密码（必需）
- `-userid`: 要删除的用户 ID（必需）

### 列出所有用户 (list-users)

```bash
orcas-cli -action list-users -user admin -pass admin
```

**参数说明：**
- `-user`: 管理员用户名（必需）
- `-pass`: 管理员密码（必需）

**输出示例：**
```
Total users: 2

ID: 1
  Username: admin
  Name: Administrator
  Role: ADMIN

ID: 2
  Username: testuser
  Name: Test User
  Role: USER
```

### 使用配置文件进行用户管理

也可以使用配置文件来设置管理员凭据：

```json
{
  "user_name": "admin",
  "password": "admin"
}
```

```bash
orcas-cli -action add-user -config config.json -newuser testuser -newpass testpass
```

## 桶管理

桶管理功能需要管理员权限。使用管理员账号登录后，可以执行以下操作：

### 创建桶 (create-bucket)

```bash
# 创建无限制配额的桶
orcas-cli -action create-bucket -user orcas -pass orcas -bucketname mybucket

# 创建指定配额的桶（10GB）
orcas-cli -action create-bucket -user orcas -pass orcas -bucketname mybucket -quota 10737418240

# 创建桶并指定所有者（通过用户名）
orcas-cli -action create-bucket -user orcas -pass orcas -bucketname mybucket -owner testuser

# 创建桶并指定所有者（通过用户ID）
orcas-cli -action create-bucket -user orcas -pass orcas -bucketname mybucket -owner 123456

# 使用配置文件
orcas-cli -action create-bucket -config config.json -bucketname mybucket
```

**参数说明：**
- `-user`: 管理员用户名（必需）
- `-pass`: 管理员密码（必需）
- `-bucketname`: 桶名称（必需）
- `-quota`: 桶配额（字节数，可选，默认为 -1 表示无限制）
- `-owner`: 桶所有者用户名或用户 ID（可选，默认为当前登录用户）

### 删除桶 (delete-bucket)

```bash
orcas-cli -action delete-bucket -user orcas -pass orcas -bucket 123456
```

**参数说明：**
- `-user`: 管理员用户名（必需）
- `-pass`: 管理员密码（必需）
- `-bucket`: 要删除的桶 ID（必需）

### 列出所有桶 (list-buckets)

```bash
orcas-cli -action list-buckets -user orcas -pass orcas
```

**参数说明：**
- `-user`: 用户名（必需）
- `-pass`: 密码（必需）

**输出示例：**
```
Total buckets: 2

ID: 123456
  Name: mybucket
  Quota: Unlimited
  Used: 1048576 bytes (0.00 GB)
  Real Used: 1048576 bytes (0.00 GB)

ID: 789012
  Name: another-bucket
  Quota: 10737418240 bytes (10.00 GB)
  Used: 5242880 bytes (0.00 GB)
  Real Used: 5242880 bytes (0.00 GB)
```

### 使用配置文件进行桶管理

也可以使用配置文件来设置管理员凭据：

```json
{
  "user_name": "orcas",
  "password": "orcas"
}
```

```bash
orcas-cli -action create-bucket -config config.json -bucketname mybucket
```

## 注意事项

1. 上传目录时，如果远程路径不存在，会自动创建
2. 下载时，如果本地目录不存在，会自动创建
3. 如果指定的 bucket ID 不存在，工具会报错
4. 如果不指定 bucket ID，会使用第一个可用的 bucket
5. 加密密钥必须符合长度要求，否则会报错
6. **用户管理功能需要管理员权限**，只有角色为 `ADMIN` 的用户才能执行用户管理操作
7. 创建用户时，密码会使用 PBKDF2 算法进行加密存储
8. 用户名必须唯一，如果尝试创建已存在的用户名，会返回错误
9. **系统首次初始化时会自动创建默认管理员账户**（用户名：`orcas`，密码：`orcas`），请尽快修改密码

