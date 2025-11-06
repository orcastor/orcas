# ORCAS Command Line Tool

- [English](README.md) | [中文](README.zh.md)

This is an ORCAS file upload/download command line tool implemented based on the SDK.

## Features

- Support uploading files and directories to ORCAS storage
- Support downloading files and directories from ORCAS storage
- Support configuration via command line parameters or configuration file (JSON)
- Support advanced features like encryption, compression, instant upload, etc.

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

- `-action`: Operation type, `upload` or `download`
- `-local`: Local file or directory path
- `-user`: Username (or set via configuration file)
- `-pass`: Password (or set via configuration file)

#### Optional Parameters

- `-config`: Configuration file path (JSON format)
- `-remote`: Remote path (relative to bucket root directory, optional for upload, required for download)
- `-bucket`: Bucket ID (if not specified, use the first bucket)

#### Advanced Configuration Parameters

- `-datasync`: Data sync (power failure protection), `true` or `false`
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

## Environment Variables

The tool uses ORCAS system environment variable configuration, mainly including:

- `ORCAS_BASE`: Base path, used to store metadata and other base files
- `ORCAS_DATA`: Data path, used to store data files

For detailed description, please refer to `core/const.go`.

## Notes

1. When uploading a directory, if the remote path does not exist, it will be created automatically
2. When downloading, if the local directory does not exist, it will be created automatically
3. If the specified bucket ID does not exist, the tool will report an error
4. If bucket ID is not specified, the first available bucket will be used
5. Encryption key must meet length requirements, otherwise an error will be reported

