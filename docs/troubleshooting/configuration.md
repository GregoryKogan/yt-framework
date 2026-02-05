# Configuration Issues

Issues specific to Dev mode and Prod mode configuration.

## Dev Mode Issues

### Tables not found in dev mode

**Error:**

```plaintext
FileNotFoundError: .dev/table.jsonl not found
```

**Solution:**

- Check table was created in previous stage
- Verify table path is correct
- Check `.dev/` directory exists
- Review previous stage logs

### Operation fails in dev mode

**Error:**

```plaintext
RuntimeError: Map operation failed
```

**Solution:**

- Check mapper script syntax
- Verify input table exists
- Review `.dev/` directory for logs
- Check mapper script has `if __name__ == "__main__": main()` block

### Sandbox directory issues

**Error:**

```plaintext
PermissionError: Cannot create sandbox directory
```

**Solution:**

- Check file permissions on pipeline directory
- Verify disk space available
- Check `.dev/` directory permissions

### DuckDB errors

**Error:**

```plaintext
Error: DuckDB query failed
```

**Solution:**

- Check YQL to SQL conversion
- Verify table schemas
- Review query syntax
- Check DuckDB version compatibility

## Prod Mode Issues

### YT credentials not found

**Error:**

```plaintext
ValueError: secrets are required for prod mode
```

**Solution:**

- Create `configs/secrets.env` file
- Add `YT_PROXY` and `YT_TOKEN`
- Verify credentials are correct
- Check file permissions

### Build folder not found

**Error:**

```plaintext
ValueError: build_folder not found in [pipeline] config section.
```

**Solution:**

- Add `build_folder` to pipeline config
- Verify YT path is correct
- Check YT permissions for build folder
- Ensure build folder path exists or can be created

### Code upload fails

**Error:**

```plaintext
Error: Failed to upload code to YT
```

**Solution:**

- Check YT credentials and permissions
- Verify build folder path is correct
- Check network connectivity
- Review upload logs

### Operation fails on cluster

**Error:**

```plaintext
RuntimeError: Operation failed on YT cluster
```

**Solution:**

- Check YT web UI for operation details
- Review operation logs in YT
- Verify resource limits are sufficient
- Check operation configuration

### YT connection errors

**Error:**

```plaintext
ConnectionError: Cannot connect to YT cluster
```

**Solution:**

- Verify `YT_PROXY` URL is correct
- Check network connectivity
- Review firewall settings
- Test YT connection manually

## Mode Switching Issues

### Dev to Prod migration

**Common Issues:**

- Table paths may need adjustment
- Resource limits may need tuning
- Checkpoint paths may differ
- Environment variables may be required

**Solution:**

- Test thoroughly in dev mode first
- Gradually migrate stages
- Review [Dev vs Prod](../dev-vs-prod.md) guide
- Check configuration differences

## See Also

- [Configuration Guide](../configuration.md) - Complete configuration reference
- [Dev vs Prod](../dev-vs-prod.md) - Understanding execution modes
- [Secrets Management](../configuration.md#secrets-management) - Managing credentials
