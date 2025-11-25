# ORCAS Command Line Tool

- [English](README.md) | [中文](README.zh.md)

This is an ORCAS file upload/download command line tool implemented based on the SDK.

## Features

- Support uploading files and directories to ORCAS storage
- Support downloading files and directories from ORCAS storage
- Support configuration via command line parameters or configuration file (JSON)
- Support advanced features like encryption, compression, instant upload, etc.
- Support user management (requires admin privileges): create, update, delete, and list users
- Support bucket management (requires admin privileges): create, delete, and list buckets, with support for specifying bucket owner and quota

## Build

```bash
go build -o orcas-cli cmd/main.go
```

## Usage

### Basic Usage

#### Upload File or Directory

```bash
# Using command line parameters
orcas-cli -action upload -local /path/to/local/file -remote /remote/path -user orcas -pass orcas

# Using configuration file
orcas-cli -action upload -local /path/to/local/file -remote /remote/path -config config.json
```

#### Download File or Directory

```bash
# Using command line parameters
orcas-cli -action download -local /path/to/save -remote /remote/path -user orcas -pass orcas

# Using configuration file
orcas-cli -action download -local /path/to/save -remote /remote/path -config config.json
```

### Command Line Parameters

#### Required Parameters

- `-action`: Operation type, `upload`, `download`, `add-user`, `update-user`, `delete-user`, `list-users`, `create-bucket`, `delete-bucket`, or `list-buckets`
- `-local`: Local file or directory path (required for upload/download)
- `-user`: Username (or set via configuration file)
- `-pass`: Password (or set via configuration file, or via stdin input, or via pipe)

**Password Input Methods:**
- Via `-pass` parameter directly (not recommended, password appears in command history)
- Via configuration file (recommended for scripts)
- Via stdin interactive input (hidden input, characters not displayed)
- Via pipe (supports `echo "password" | orcas-cli ...` or `password-command | orcas-cli ...`)

#### Optional Parameters

- `-config`: Configuration file path (JSON format)
- `-remote`: Remote path (relative to bucket root directory, optional for upload, required for download)

#### User Management Parameters

- `-userid`: User ID (for `update-user` and `delete-user`)
- `-newuser`: New username (for `add-user` and `update-user`)
- `-newpass`: New password (for `add-user` and `update-user`)
- `-username`: User display name (for `add-user` and `update-user`)
- `-role`: User role, `USER` or `ADMIN` (for `add-user` and `update-user`)

#### Bucket Management Parameters

- `-bucketname`: Bucket name (for `create-bucket`)
- `-quota`: Bucket quota in bytes (for `create-bucket`, -1 for unlimited, default is -1)
- `-owner`: Bucket owner username or user ID (for `create-bucket`, default is current user)

#### Other Optional Parameters

- `-bucket`: Bucket ID (if not specified, use the first bucket)

#### Advanced Configuration Parameters

- `-reflevel`: Instant upload level, `OFF`, `FULL`, `FAST`
- `-wisecmpr`: Smart compression, `SNAPPY`, `ZSTD`, `GZIP`, `BR`
- `-cmprqlty`: Compression level (br: 0-11, gzip: -3-9, zstd: 0-10)
- `-endecway`: Encryption method, `AES256` or `SM4`
- `-endeckey`: Encryption key (AES256 requires >16 characters, SM4 requires 16 characters)
- `-dontsync`: Filename wildcards to exclude from sync (separated by semicolons)
- `-conflict`: Conflict resolution for same name, `COVER`, `RENAME`, `THROW`, `SKIP`
- `-nametmpl`: Rename template (contains `%s`)
- `-workers`: Concurrent pool size (not less than 16)

### Configuration File

Configuration file supports JSON format. Command line parameters will override corresponding values in the configuration file.

#### JSON Configuration File Example

Refer to `config.example.json`

### Configuration Item Description

#### Instant Upload Level (ref_level)

- `OFF`: Disable instant upload, upload complete file every time
- `FULL`: Full file instant upload, read entire file to calculate checksum
- `FAST`: Fast instant upload, check file header first, then read entire file if match succeeds

#### Smart Compression (wise_cmpr)

Automatically decide whether to compress based on file type:

- `SNAPPY`: Snappy compression
- `ZSTD`: Zstd compression
- `GZIP`: Gzip compression
- `BR`: Brotli compression

#### Encryption Method (endec_way)

- `AES256`: AES-256 encryption (key length must be > 16 characters)
- `SM4`: SM4 national encryption (key length must be = 16 characters)
- Empty: No encryption

#### Conflict Resolution for Same Name (conflict)

- `COVER`: Merge or overwrite
- `RENAME`: Rename (using name_tmpl template)
- `THROW`: Throw error
- `SKIP`: Skip

## Usage Examples

### Example 1: Upload Directory (Using Configuration File)

1. Create configuration file `config.json`:

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

2. Execute upload:

```bash
orcas-cli -action upload -local /path/to/directory -remote /my/directory -config config.json
```

### Example 2: Download File (Using Command Line Parameters)

```bash
orcas-cli -action download -local ./downloads -remote /my/file.txt -user orcas -pass orcas -reflevel FULL
```

### Example 3: Upload with Encryption Enabled

```bash
orcas-cli -action upload \
  -local /path/to/file \
  -remote /encrypted/file \
  -user orcas \
  -pass orcas \
  -endecway AES256 \
  -endeckey "my-very-long-encryption-key-here"
```

### Example 4: Using Interactive Password Input (Hidden Input)

```bash
# Without providing -pass parameter, program will prompt for password (characters not displayed)
orcas-cli -action upload -local /path/to/file -remote /file -user orcas
# Program will prompt: Password: (characters not displayed while typing)
```

### Example 5: Using Pipe to Pass Password

```bash
# Pass password via echo
echo "mypassword" | orcas-cli -action upload -local /path/to/file -remote /file -user orcas

# Pass password via password manager (e.g., pass, 1password-cli, etc.)
pass orcas | orcas-cli -action upload -local /path/to/file -remote /file -user orcas

# Read password from file
cat password.txt | orcas-cli -action upload -local /path/to/file -remote /file -user orcas
```

## Environment Variables

The tool uses ORCAS system environment variable configuration, mainly including:

- `ORCAS_BASE`: Base path, used to store metadata and other base files
- `ORCAS_DATA`: Data path, used to store data files

For detailed description, please refer to `core/const.go`.

## Default Administrator Account

A default administrator account is automatically created when the system is initialized:
- **Username**: `orcas`
- **Password**: `orcas`

**Important**: Please change the default administrator password as soon as possible after first use to ensure security.

## User Management

User management functions require administrator privileges. After logging in with an administrator account, you can perform the following operations:

### Create User (add-user)

```bash
# Create a regular user
orcas-cli -action add-user -user admin -pass admin -newuser testuser -newpass testpass

# Create an administrator user
orcas-cli -action add-user -user admin -pass admin -newuser admin2 -newpass admin2 -role ADMIN

# Create a user with display name
orcas-cli -action add-user -user admin -pass admin -newuser testuser -newpass testpass -username "Test User"

# Use interactive input for new user password (hidden input)
orcas-cli -action add-user -user admin -pass admin -newuser testuser
# Program will prompt: New user password: (characters not displayed while typing)

# Use pipe to pass new user password
echo "newuserpass" | orcas-cli -action add-user -user admin -pass admin -newuser testuser
```

**Parameters:**
- `-user`: Administrator username (required)
- `-pass`: Administrator password (required)
- `-newuser`: New user's username (required)
- `-newpass`: New user's password (required)
- `-username`: User display name (optional, defaults to username)
- `-role`: User role, `USER` or `ADMIN` (optional, defaults to `USER`)

### Update User (update-user)

```bash
# Update user password
orcas-cli -action update-user -user admin -pass admin -userid 123 -newpass newpassword

# Update username
orcas-cli -action update-user -user admin -pass admin -userid 123 -newuser newusername

# Update user role
orcas-cli -action update-user -user admin -pass admin -userid 123 -role ADMIN

# Update user display name
orcas-cli -action update-user -user admin -pass admin -userid 123 -username "New Display Name"
```

**Parameters:**
- `-user`: Administrator username (required)
- `-pass`: Administrator password (required)
- `-userid`: User ID to update (required)
- `-newuser`: New username (optional)
- `-newpass`: New password (optional)
- `-username`: New display name (optional)
- `-role`: New role, `USER` or `ADMIN` (optional)

### Delete User (delete-user)

```bash
orcas-cli -action delete-user -user admin -pass admin -userid 123
```

**Parameters:**
- `-user`: Administrator username (required)
- `-pass`: Administrator password (required)
- `-userid`: User ID to delete (required)

### List All Users (list-users)

```bash
orcas-cli -action list-users -user admin -pass admin
```

**Parameters:**
- `-user`: Administrator username (required)
- `-pass`: Administrator password (required)

**Output Example:**
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

### Using Configuration File for User Management

You can also use a configuration file to set administrator credentials:

```json
{
  "user_name": "admin",
  "password": "admin"
}
```

```bash
orcas-cli -action add-user -config config.json -newuser testuser -newpass testpass
```

## Bucket Management

Bucket management functions require administrator privileges. After logging in with an administrator account, you can perform the following operations:

### Create Bucket (create-bucket)

```bash
# Create bucket with unlimited quota
orcas-cli -action create-bucket -user orcas -pass orcas -bucketname mybucket

# Create bucket with specified quota (10GB)
orcas-cli -action create-bucket -user orcas -pass orcas -bucketname mybucket -quota 10737418240

# Create bucket and specify owner (by username)
orcas-cli -action create-bucket -user orcas -pass orcas -bucketname mybucket -owner testuser

# Create bucket and specify owner (by user ID)
orcas-cli -action create-bucket -user orcas -pass orcas -bucketname mybucket -owner 123456

# Using configuration file
orcas-cli -action create-bucket -config config.json -bucketname mybucket
```

**Parameters:**
- `-user`: Administrator username (required)
- `-pass`: Administrator password (required)
- `-bucketname`: Bucket name (required)
- `-quota`: Bucket quota in bytes (optional, default is -1 for unlimited)
- `-owner`: Bucket owner username or user ID (optional, default is current logged-in user)

### Delete Bucket (delete-bucket)

```bash
orcas-cli -action delete-bucket -user orcas -pass orcas -bucket 123456
```

**Parameters:**
- `-user`: Administrator username (required)
- `-pass`: Administrator password (required)
- `-bucket`: Bucket ID to delete (required)

### List All Buckets (list-buckets)

```bash
orcas-cli -action list-buckets -user orcas -pass orcas
```

**Parameters:**
- `-user`: Username (required)
- `-pass`: Password (required)

**Output Example:**
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

### Using Configuration File for Bucket Management

You can also use a configuration file to set administrator credentials:

```json
{
  "user_name": "orcas",
  "password": "orcas"
}
```

```bash
orcas-cli -action create-bucket -config config.json -bucketname mybucket
```

## Notes

1. When uploading a directory, if the remote path does not exist, it will be created automatically
2. When downloading, if the local directory does not exist, it will be created automatically
3. If the specified bucket ID does not exist, the tool will report an error
4. If bucket ID is not specified, the first available bucket will be used
5. Encryption key must meet length requirements, otherwise an error will be reported
6. **User management functions require administrator privileges** - only users with `ADMIN` role can perform user management operations
7. When creating users, passwords are encrypted using PBKDF2 algorithm before storage
8. Usernames must be unique - attempting to create a user with an existing username will return an error
9. **A default administrator account is automatically created on first system initialization** (username: `orcas`, password: `orcas`) - please change the password as soon as possible

