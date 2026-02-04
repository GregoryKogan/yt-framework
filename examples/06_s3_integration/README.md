# S3 Integration Example

Demonstrates S3 integration for listing files and saving paths to YT tables. Shows how to set up S3 client and use S3 operations.

## What It Demonstrates

- **S3 Client**: Creating and configuring S3 client
- **File Listing**: Listing files from S3 buckets
- **Path Storage**: Saving S3 paths to YT tables
- **Secrets Management**: Using AWS credentials from secrets.env

## Features

- S3 client creation in stage
- File listing with optional filters (prefix, extension, max files)
- Saving S3 paths to YT table
- AWS credentials management

## Running

**Prerequisites:**

1. Create `configs/secrets.env`:
```bash
S3_ENDPOINT=https://your-s3-endpoint.com
S3_DOWNLOAD_ACCESS_KEY=your-download-access-key
S3_DOWNLOAD_SECRET_KEY=your-download-secret-key
S3_UPLOAD_ACCESS_KEY=your-upload-access-key
S3_UPLOAD_SECRET_KEY=your-upload-secret-key
```

2. Update bucket and prefix in `stages/list_s3/config.yaml`

3. Run pipeline:
```bash
python pipeline.py
```

## Files

- `pipeline.py`: Pipeline entry point
- `stages/list_s3/stage.py`: Stage that lists S3 files and saves to table
- `stages/list_s3/config.yaml`: S3 configuration (bucket, prefix, filters)
- `configs/config.yaml`: Pipeline configuration
- `configs/secrets.example.env`: Example secrets file

## Key Concepts

- S3 client is created in stage `__init__` method
- Files are listed with optional filters
- Paths are saved to YT table for processing
- Credentials are loaded from `secrets.env`

## Configuration

```yaml
client:
  input_bucket: my-bucket
  input_prefix: data/2024/
  file_extension: .json      # Optional: filter by extension
  max_files: 1000           # Optional: limit results
  output_table: //tmp/my_pipeline/s3_paths
```

## Next Steps

- See [04_map_operation](../04_map_operation/) for processing S3 files
- See [Configuration Guide](../../docs/configuration.md) for secrets management
