# ORCAS VFS Mount Guide

This guide explains how to use OrcaS VFS (Virtual File System) to mount OrcaS object storage as a local filesystem.

- [English](MOUNT_GUIDE.md) | [中文](MOUNT_GUIDE.zh.md)

## Table of Contents

- [Quick Start](#quick-start)
- [Creating Buckets](#creating-buckets)
- [Mounting Filesystem](#mounting-filesystem)
- [No Base Database Mode](#no-base-database-mode)
- [Parameters](#parameters)
- [Unmounting](#unmounting)
- [FAQ](#faq)

## Quick Start

### Method 1: Using Mount Script (Recommended)

```bash
cd /DATA/orcas
./vfs/mount_vfs.sh
```

The script will prompt for username and password, then automatically mount to `/DATA/orcas_test`.

### Method 2: Direct Command Line

```bash
cd /DATA/orcas

# Build (if not already built)
go build -o main ./cmd/main.go

# Mount (requires username and password)
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user <username> \
  -pass <password> \
  -endecway AES256 \
  -endeckey "your-encryption-key-here-must-be-longer-than-16-chars" \
  -cmprway ZSTD \
  -cmprqlty 5
```

### Method 3: Using Configuration File

Create a configuration file `config.json`:

```json
{
  "user_name": "your_username",
  "password": "your_password",
  "endec_way": "AES256",
  "endec_key": "your-encryption-key-here-must-be-longer-than-16-chars",
  "cmpr_way": "ZSTD",
  "cmpr_qlty": 5
}
```

Then run:

```bash
./main -action mount -mountpoint /DATA/orcas_test -config config.json
```

### Complete Example (Encryption + Smart Compression)

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -endecway AES256 \
  -endeckey "orcas-encryption-key-12345678901234567890" \
  -cmprway ZSTD \
  -cmprqlty 5
```

### Default Configuration

If encryption and compression are not specified, the program will automatically enable default values:

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas
```

Default configuration:
- **Encryption**: AES256 (using default key, **not recommended for production**)
- **Smart Compression**: ZSTD, level 5

## Creating Buckets

Before mounting the filesystem, you need to create a bucket (Bucket). A bucket is the basic container for storing data, and each bucket can be configured with independent encryption, compression, and quota settings.

### Creating a Bucket (Requires Admin Privileges)

```bash
cd /DATA/orcas

# Build (if not already built)
go build -o main ./cmd/main.go

# Create bucket (basic usage)
./main -action create-bucket \
  -user <admin_username> \
  -pass <admin_password> \
  -bucketname <bucket_name>
```

### Bucket Creation Parameters

#### Required Parameters

- `-action create-bucket`: Specify the operation as creating a bucket
- `-bucketname <name>`: Bucket name
- `-user <username>`: Admin username (requires ADMIN permission)
- `-pass <password>`: Admin password

#### Optional Parameters

- `-quota <bytes>`: Bucket quota (in bytes, `-1` means unlimited)
  - Example: `-quota 1073741824` means 1GB
  - Example: `-quota -1` means unlimited (default)
- `-owner <username or userID>`: Bucket owner (default is current user)
  - Can be username or userID
  - Example: `-owner user1` or `-owner 12345`
- `-cmprway <algorithm>`: Compression method (default: smart compression)
  - `SNAPPY`: Snappy compression (fast)
  - `ZSTD`: Zstd compression (recommended, balances speed and compression ratio)
  - `GZIP`: Gzip compression (good compatibility)
  - `BR`: Brotli compression (high compression ratio)
- `-cmprqlty <level>`: Compression level (0-11, default: 5)
- `-endecway <method>`: Encryption method
  - `AES256`: AES-256 encryption (recommended, key length must be >16 characters)
  - `SM4`: SM4 encryption (key length must be =16 characters)
- `-endeckey <key>`: Encryption key
  - AES256: Length must be >16 characters
  - SM4: Length must be =16 characters
- `-reflevel <level>`: Instant upload (deduplication) level
  - `FULL`: Full deduplication (recommended)
  - `FAST`: Fast deduplication
  - `OFF`: Disable deduplication

### Bucket Creation Examples

#### Basic Bucket (No Encryption and Compression)

```bash
./main -action create-bucket \
  -user orcas \
  -pass orcas \
  -bucketname my-bucket
```

#### Bucket with Quota

```bash
./main -action create-bucket \
  -user orcas \
  -pass orcas \
  -bucketname my-bucket \
  -quota 10737418240
```

(Creates a bucket with 10GB quota)

#### Fully Configured Bucket (Encryption+Compression+Deduplication)

```bash
./main -action create-bucket \
  -user orcas \
  -pass orcas \
  -bucketname secure-bucket \
  -quota -1 \
  -endecway AES256 \
  -endeckey "my-secure-encryption-key-12345678901234567890" \
  -cmprway ZSTD \
  -cmprqlty 5 \
  -reflevel FULL
```

#### Create Bucket for Specific User

```bash
./main -action create-bucket \
  -user orcas \
  -pass orcas \
  -bucketname user-bucket \
  -owner user1
```

### List Buckets

```bash
./main -action list-buckets \
  -user <username> \
  -pass <password>
```

### Update Bucket Configuration

```bash
./main -action update-bucket \
  -user <admin_username> \
  -pass <admin_password> \
  -bucket <bucket_id> \
  -cmprway ZSTD \
  -cmprqlty 6
```

### Delete Bucket

```bash
./main -action delete-bucket \
  -user <admin_username> \
  -pass <admin_password> \
  -bucket <bucket_id>
```

## Mounting Filesystem

### Parameters

#### Required Parameters

- `-action mount`: Specify the operation as mounting
- `-mountpoint <path>`: Mount point path (e.g., `/DATA/orcas_test`)
- `-user <username>`: ORCAS username (can be omitted when using `-noauth`)
- `-pass <password>`: ORCAS password (can be omitted when using `-noauth`)

#### Encryption Parameters

- `-endecway`: Encryption method
  - `AES256`: AES-256 encryption (recommended, key length must be >16 characters)
  - `SM4`: SM4 encryption (key length must be =16 characters)
- `-endeckey`: Encryption key
  - AES256: Length must be >16 characters
  - SM4: Length must be =16 characters

#### Compression Parameters

- `-cmprway`: Compression method (default: smart compression)
  - `SNAPPY`: Snappy compression (fast)
  - `ZSTD`: Zstd compression (recommended, balances speed and compression ratio)
  - `GZIP`: Gzip compression (good compatibility)
  - `BR`: Brotli compression (high compression ratio)
- `-cmprqlty`: Compression level (0-11, default: 5)

#### Other Parameters

- `-config <file_path>`: Configuration file path (JSON format)
- `-bucket <ID>`: Specify Bucket ID (default: use first bucket)
- `-noauth`: No authentication mode (no main database required, see below)
- `-debug`: Enable debug mode (verbose output)
- `-requirekey`: Require KEY in context, otherwise return EPERM error

### Mount Examples

#### Enable Smart Compression Only (No Encryption)

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -cmprway ZSTD \
  -cmprqlty 5
```

#### Enable Encryption Only (No Compression)

```bash
./main -action mount \
  -mountpoint /DATA/orcas_test \
  -user orcas \
  -pass orcas \
  -endecway AES256 \
  -endeckey "my-secure-encryption-key-12345678901234567890"
```

## No Base Database Mode

OrcaS supports a completely no-base-database operation mode, suitable for scenarios that do not require user management and ACL permission control.

### Configuration

**No Base Database Mode:**
```bash
# Do not set ORCAS_BASE (or set to empty)
# export ORCAS_BASE=""  # Optional, just don't set it

# Must set ORCAS_DATA (bucket database storage location)
export ORCAS_DATA=/var/orcas/data
```

### Usage Example

```bash
# Mount filesystem using -noauth parameter, no main database required
./main -action mount \
  -noauth \
  -bucket 123456789 \
  -mountpoint /mnt/orcas \
  -endeckey "your-encryption-key" \
  -cmprway ZSTD \
  -cmprqlty 3

# Now you can use it like a regular filesystem
echo "Hello, World!" > /mnt/orcas/test.txt
cat /mnt/orcas/test.txt
```

**Notes:**
- When using `-noauth`, you must specify `-bucket <bucket_id>`
- No need to set `ORCAS_BASE` environment variable
- No user authentication required, directly use bucket ID

For more details, see: [No Base Database Mode Guide](../docs/NO_BASE_DB_ANALYSIS.md)

## Unmounting

Press `Ctrl+C` to stop mounting, or use system command:

```bash
sudo fusermount -u /DATA/orcas_test
```

## Notes

1. **Permissions**: Mounting may require root privileges, or ensure the user has permission to access the mount point
2. **Encryption Key**: Use a strong key in production, do not use the default key
3. **Mount Point**: Ensure the mount point directory exists and is empty (or can accept mounting)
4. **FUSE**: Requires system support for FUSE (Linux/macOS)
5. **AES256 encryption key length must be >16 characters**
6. **SM4 encryption key length must be =16 characters**

## FAQ

### Q: Mount failed, permission error?

A: Ensure the mount point directory exists and has write permission:
```bash
sudo mkdir -p /DATA/orcas_test
sudo chmod 755 /DATA/orcas_test
```

### Q: How to check mount status?

A: Use system commands:
```bash
mount | grep orcas
# or
df -h | grep orcas
```

### Q: How to set up automatic mount on boot?

A: You can add the mount command to `/etc/fstab` or use systemd service. Note: OrcaS VFS does not support direct addition to fstab, it is recommended to use systemd service.

### Q: How to manage buckets in no-base-database mode?

A: In no-base-database mode, you need to manually manage bucket IDs. Recommendations:
- Use configuration files to store bucket IDs
- Use environment variables to store bucket IDs
- Hardcode bucket IDs in applications

### Q: How to backup and restore?

A:
- **Backup**: Backup the `ORCAS_DATA` directory (contains all bucket databases and data files)
- **Restore**: Restore the `ORCAS_DATA` directory to a new location, set the `ORCAS_DATA` environment variable to point to the new location

## Related Documentation

- [No Base Database Mode Guide](../docs/NO_BASE_DB_ANALYSIS.md) - Learn how to use without main database
- [VFS Performance Optimization Report](PERFORMANCE_OPTIMIZATION_FINAL.md) - VFS performance optimization details
- [Architecture Refactoring Proposal](../docs/ARCHITECTURE_REFACTOR_PROPOSAL.md) - Learn about database architecture design

