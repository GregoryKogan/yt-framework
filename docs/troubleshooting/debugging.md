# Debugging Guide

Tips and strategies for debugging YT Framework pipelines.

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

## Debug Mode

### Enable Debug Mode

Set debug logging in pipeline:

```python
from yt_framework.core.pipeline import DefaultPipeline
import logging

if __name__ == "__main__":
    pipeline = DefaultPipeline(
        config=config,
        pipeline_dir=Path("."),
        log_level=logging.DEBUG
    )
    pipeline.run()
```

### Log Analysis

Common log patterns to look for:

- `ERROR` - Critical failures
- `WARNING` - Potential issues
- `DEBUG` - Detailed execution information

### Common Error Codes

- `ValueError` - Configuration or parameter errors
- `FileNotFoundError` - Missing files or tables
- `PermissionError` - Access denied
- `RuntimeError` - Operation execution failures

## Next Steps

- Review [Configuration Guide](../configuration.md) for config issues
- Check [Dev vs Prod](../dev-vs-prod.md) for mode-specific issues
- Explore [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples/) for working patterns
- Review [API Reference](../reference/api.md) for detailed API information
