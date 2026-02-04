# Troubleshooting

Common issues and solutions for YT Framework pipelines.

## Common Issues

### Pipeline Issues

#### No enabled_stages found

**Error:**

```plaintext
ValueError: No enabled_stages found in stages config section.
```

**Solution:**
Add `enabled_stages` to pipeline config:

```yaml
stages:
  enabled_stages:
    - stage1
    - stage2
```

#### Stage not found

**Error:**

```plaintext
ValueError: Unknown stage: my_stage. Available stages: [...]
```

**Solution:**

- Check stage name matches directory name
- Verify `stages/my_stage/stage.py` exists
- Ensure stage class inherits from `BaseStage`
- Check stage is registered (for `BasePipeline`)

#### Config file not found

**Error:**

```plaintext
FileNotFoundError: Config file not found: configs/config.yaml
```

**Solution:**

- Verify config file exists at specified path
- Check file path is correct
- Use `--config` flag to specify different config

### Dev Mode Issues

#### Tables not found in dev mode

**Error:**

```plaintext
FileNotFoundError: .dev/table.jsonl not found
```

**Solution:**

- Check table was created in previous stage
- Verify table path is correct
- Check `.dev/` directory exists
- Review previous stage logs

#### Operation fails in dev mode

**Error:**

```plaintext
RuntimeError: Map operation failed
```

**Solution:**

- Check mapper script syntax
- Verify input table exists
- Review `.dev/` directory for logs
- Check mapper script has `if __name__ == "__main__": main()` block

#### Sandbox directory issues

**Error:**

```plaintext
PermissionError: Cannot create sandbox directory
```

**Solution:**

- Check file permissions on pipeline directory
- Verify disk space available
- Check `.dev/` directory permissions

### Prod Mode Issues

#### YT credentials not found

**Error:**

```plaintext
ValueError: secrets are required for prod mode
```

**Solution:**

- Create `configs/secrets.env` file
- Add `YT_PROXY` and `YT_TOKEN`
- Verify credentials are correct
- Check file permissions

#### Build folder not found

**Error:**

```plaintext
ValueError: build_folder not found in [pipeline] config section.
```

**Solution:**

- Add `build_folder` to pipeline config
- Verify YT path is correct
- Check YT permissions for build folder
- Ensure build folder path exists or can be created

#### Code upload fails

**Error:**

```plaintext
Error: Failed to upload code to YT
```

**Solution:**

- Check YT credentials and permissions
- Verify build folder path is correct
- Check network connectivity
- Review upload logs

#### Operation fails on cluster

**Error:**

```plaintext
RuntimeError: Operation failed on YT cluster
```

**Solution:**

- Check YT web UI for operation details
- Review operation logs in YT
- Verify resource limits are sufficient
- Check operation configuration

### Map Operation Issues

#### Mapper script not found

**Error:**

```plaintext
FileNotFoundError: stages/my_stage/src/mapper.py not found
```

**Solution:**

- Verify `src/mapper.py` exists
- Check file name is exactly `mapper.py`
- Ensure file is in correct location

#### Mapper script errors

**Error:**

```plaintext
SyntaxError in mapper.py
```

**Solution:**

- Check Python syntax
- Verify imports are correct
- Test mapper script locally
- Review error messages

#### Input table not found

**Error:**

```plaintext
FileNotFoundError: Input table not found
```

**Solution:**

- Verify input table path is correct
- Check table exists (use `yt_client.exists()`)
- Ensure previous stage created the table
- Review table path in config

#### Output table creation fails

**Error:**

```plaintext
Error: Cannot create output table
```

**Solution:**

- Check YT permissions
- Verify output path is correct
- Ensure parent directory exists
- Check disk space on cluster

### Vanilla Operation Issues

#### Vanilla script not found

**Error:**

```plaintext
FileNotFoundError: stages/my_stage/src/vanilla.py not found
```

**Solution:**

- Verify `src/vanilla.py` exists
- Check file name is exactly `vanilla.py`
- Ensure file is in correct location

#### Vanilla script errors

**Error:**

```plaintext
RuntimeError: Vanilla operation failed
```

**Solution:**

- Check script syntax
- Verify script has `if __name__ == "__main__": main()` block
- Review operation logs
- Test script locally

### YQL Operation Issues

#### Join fails

**Error:**

```plaintext
Error: Join operation failed
```

**Solution:**

- Check column names match
- Verify table schemas are compatible
- Ensure tables exist
- Review join configuration

#### Filter condition error

**Error:**

```plaintext
SyntaxError: Invalid filter condition
```

**Solution:**

- Use proper SQL-like syntax
- Escape special characters
- Check column names exist
- Verify condition syntax

#### Aggregation fails

**Error:**

```plaintext
Error: Aggregation operation failed
```

**Solution:**

- Verify column types are numeric (for sum/avg)
- Check column names exist
- Ensure group_by columns exist
- Review aggregation configuration

### S3 Integration Issues

#### S3 client creation fails

**Error:**

```plaintext
Error: Failed to create S3 client
```

**Solution:**

- Check AWS credentials in `secrets.env`
- Verify credentials have S3 access
- Check AWS region is correct
- Review credential format

#### S3 files not found

**Error:**

```plaintext
Warning: No files found in S3
```

**Solution:**

- Verify bucket name is correct
- Check prefix path is correct
- Ensure files exist in S3
- Review S3 permissions

#### S3 permission denied

**Error:**

```plaintext
PermissionError: Access denied to S3 bucket
```

**Solution:**

- Check IAM permissions for S3 access
- Verify credentials have read/list permissions
- Check bucket policy
- Review AWS credentials

### Docker Issues

#### Docker image not found

**Error:**

```plaintext
Error: Docker image not found
```

**Solution:**

- Check image name and tag
- Verify image exists in registry
- Check Docker authentication
- Review image path

#### Platform mismatch

**Error:**

```plaintext
Error: Platform mismatch
```

**Solution:**

- Build for `linux/amd64` platform
- Use `docker buildx` for cross-platform builds
- Verify image platform compatibility

#### GPU not available

**Error:**

```plaintext
Error: GPU not available
```

**Solution:**

- Verify GPU-enabled image
- Check `gpu_limit` is set
- Ensure cluster has GPU nodes
- Review GPU resource allocation

### Checkpoint Issues

#### Checkpoint not found

**Error:**

```plaintext
FileNotFoundError: Required checkpoint not found in YT
```

**Solution:**

- Verify `checkpoint_base` path exists
- Check `model_name` matches filename
- Ensure checkpoint was uploaded
- Check YT permissions

#### Checkpoint upload fails

**Error:**

```plaintext
Error: Failed to upload checkpoint
```

**Solution:**

- Check `local_checkpoint_path` exists
- Verify file permissions
- Check YT credentials
- Review upload logs

#### Checkpoint format error

**Error:**

```plaintext
Error: Invalid checkpoint format
```

**Solution:**

- Verify checkpoint format (PyTorch, etc.)
- Check model loading code
- Review checkpoint creation process
- Test checkpoint loading locally

## Debugging Tips

### Enable Debug Logging

Set log level to DEBUG:

```python
from yt_framework.core.pipeline import DefaultPipeline
import logging

# In pipeline.py
if __name__ == "__main__":
    DefaultPipeline.main()
```

Or set in code:

```python
self.logger.setLevel(logging.DEBUG)
```

### Check Dev Mode Files

In dev mode, inspect generated files:

```bash
# Check tables
ls -la .dev/*.jsonl

# Check logs
cat .dev/operation.log

# Check sandbox
ls -la .dev/sandbox_*/
```

### Review YT Web UI

In prod mode, check YT web UI:

1. Navigate to operation
2. Review operation logs
3. Check job details
4. Inspect input/output tables

### Test Locally First

Always test in dev mode first:

```yaml
pipeline:
  mode: "dev"
```

Then switch to prod:

```yaml
pipeline:
  mode: "prod"
```

### Check Configuration

Validate configuration:

```python
# Print config
print(OmegaConf.to_yaml(self.config))

# Check specific values
print(self.config.client.operations.map.input_table)
```

### Review Logs

Check logs for errors:

```python
# In stage
self.logger.error("Error message", exc_info=True)
```

Review operation logs in YT web UI or `.dev/` directory.

## Getting Help

### Check Documentation

- Review relevant documentation sections
- Check examples for similar patterns
- Review API reference

### Common Patterns

- Check examples directory for working code
- Review similar pipelines
- Look for common patterns

### Error Messages

- Read error messages carefully
- Check stack traces
- Review operation logs

### Community Support

- Check project issues
- Review documentation
- Ask for help with specific error messages

## Prevention

### Best Practices

1. **Test locally**: Always test in dev mode first
2. **Validate configs**: Check configuration before running
3. **Handle errors**: Add error handling in stages
4. **Log progress**: Use logging for debugging
5. **Version control**: Track code and config changes

### Common Mistakes

1. **Wrong mode**: Using prod mode for development
2. **Missing configs**: Forgetting required configuration
3. **Path errors**: Incorrect table or file paths
4. **Resource limits**: Insufficient resources allocated
5. **Missing dependencies**: Forgetting to install packages

## Next Steps

- Review [Configuration Guide](configuration.md) for config issues
- Check [Dev vs Prod](dev-vs-prod.md) for mode-specific issues
- Explore [Examples](../examples/) for working patterns
