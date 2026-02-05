# Operation Issues

Common issues with Map, Vanilla, YQL, S3, Docker, and Checkpoint operations.

## Map Operation Issues

### Mapper script not found

**Error:**

```plaintext
FileNotFoundError: stages/my_stage/src/mapper.py not found
```

**Solution:**

- Verify `src/mapper.py` exists
- Check file name is exactly `mapper.py`
- Ensure file is in correct location

### Mapper script errors

**Error:**

```plaintext
SyntaxError in mapper.py
```

**Solution:**

- Check Python syntax
- Verify imports are correct
- Test mapper script locally
- Review error messages

### Input table not found

**Error:**

```plaintext
FileNotFoundError: Input table not found
```

**Solution:**

- Verify input table path is correct
- Check table exists (use `yt_client.exists()`)
- Ensure previous stage created the table
- Review table path in config

### Output table creation fails

**Error:**

```plaintext
Error: Cannot create output table
```

**Solution:**

- Check YT permissions
- Verify output path is correct
- Ensure parent directory exists
- Check disk space on cluster

## Vanilla Operation Issues

### Vanilla script not found

**Error:**

```plaintext
FileNotFoundError: stages/my_stage/src/vanilla.py not found
```

**Solution:**

- Verify `src/vanilla.py` exists
- Check file name is exactly `vanilla.py`
- Ensure file is in correct location

### Vanilla script errors

**Error:**

```plaintext
RuntimeError: Vanilla operation failed
```

**Solution:**

- Check script syntax
- Verify script has `if __name__ == "__main__": main()` block
- Review operation logs
- Test script locally

## YQL Operation Issues

### Join fails

**Error:**

```plaintext
Error: Join operation failed
```

**Solution:**

- Check column names match
- Verify table schemas are compatible
- Ensure tables exist
- Review join configuration

### Filter condition error

**Error:**

```plaintext
SyntaxError: Invalid filter condition
```

**Solution:**

- Use proper SQL-like syntax
- Escape special characters
- Check column names exist
- Verify condition syntax

### Aggregation fails

**Error:**

```plaintext
Error: Aggregation operation failed
```

**Solution:**

- Verify column types are numeric (for sum/avg)
- Check column names exist
- Ensure group_by columns exist
- Review aggregation configuration

## S3 Integration Issues

### S3 client creation fails

**Error:**

```plaintext
Error: Failed to create S3 client
```

**Solution:**

- Check AWS credentials in `secrets.env`
- Verify credentials have S3 access
- Check AWS region is correct
- Review credential format

### S3 files not found

**Error:**

```plaintext
Warning: No files found in S3
```

**Solution:**

- Verify bucket name is correct
- Check prefix path is correct
- Ensure files exist in S3
- Review S3 permissions

### S3 permission denied

**Error:**

```plaintext
PermissionError: Access denied to S3 bucket
```

**Solution:**

- Check IAM permissions for S3 access
- Verify credentials have read/list permissions
- Check bucket policy
- Review AWS credentials

## Docker Issues

### Docker image not found

**Error:**

```plaintext
Error: Docker image not found
```

**Solution:**

- Check image name and tag
- Verify image exists in registry
- Check Docker authentication
- Review image path

### Platform mismatch

**Error:**

```plaintext
Error: Platform mismatch
```

**Solution:**

- Build for `linux/amd64` platform
- Use `docker buildx` for cross-platform builds
- Verify image platform compatibility

### GPU not available

**Error:**

```plaintext
Error: GPU not available
```

**Solution:**

- Verify GPU-enabled image
- Check `gpu_limit` is set
- Ensure cluster has GPU nodes
- Review GPU resource allocation

## Checkpoint Issues

### Checkpoint not found

**Error:**

```plaintext
FileNotFoundError: Required checkpoint not found in YT
```

**Solution:**

- Verify `checkpoint_base` path exists
- Check `model_name` matches filename
- Ensure checkpoint was uploaded
- Check YT permissions

### Checkpoint upload fails

**Error:**

```plaintext
Error: Failed to upload checkpoint
```

**Solution:**

- Check `local_checkpoint_path` exists
- Verify file permissions
- Check YT credentials
- Review upload logs

### Checkpoint format error

**Error:**

```plaintext
Error: Invalid checkpoint format
```

**Solution:**

- Verify checkpoint format (PyTorch, etc.)
- Check model loading code
- Review checkpoint creation process
- Test checkpoint loading locally

## See Also

- [Map Operations](../operations/map.md) - Map operation guide
- [Vanilla Operations](../operations/vanilla.md) - Vanilla operation guide
- [YQL Operations](../operations/yql.md) - YQL operation guide
- [S3 Operations](../operations/s3.md) - S3 operation guide
- [Checkpoints](../advanced/checkpoints.md) - Checkpoint management guide
- [Docker Guide](../advanced/docker.md) - Docker configuration guide
