# Dev vs Prod Modes

YT Framework supports two execution modes: **dev** (development) and **prod** (production). Understanding the differences and when to use each mode is crucial for effective pipeline development.

## Overview

```{tip}
**Start with Dev Mode**

Always develop and test your pipelines in dev mode first. It's faster, doesn't require YT credentials, and makes debugging easier.
```

- **Dev Mode**: Simulates YT operations locally using the file system. Perfect for development, testing, and debugging.
- **Prod Mode**: Executes operations on the actual YT cluster. Used for production workloads.

Both modes use the same code and configuration, making it easy to develop locally and deploy to production.

```{warning}
**Credentials Required for Prod Mode**

Production mode requires YT credentials in `configs/secrets.env`. Make sure to set up credentials before running in prod mode.
```

## Dev Mode

### How It Works (dev)

Dev mode simulates YT operations using the local file system:

- **Tables**: Stored as `.jsonl` files in `.dev/` directory
- **Operations**: Executed locally using subprocess
- **Code Upload**: No-op (code runs directly from local filesystem)
- **YQL Operations**: Executed using DuckDB for local simulation

### Configuration (dev)

Set mode in pipeline config:

```yaml
# configs/config.yaml
pipeline:
  mode: "dev"
```

### Directory Structure (dev)

When running in dev mode, the framework creates a `.dev/` directory:

```plaintext
my_pipeline/
├── .dev/
│   ├── table1.jsonl      # Simulated YT tables
│   ├── table2.jsonl
│   └── operation.log     # Operation logs
├── configs/
├── stages/
└── pipeline.py
```

### Table Operations (dev)

**Writing tables:**

```python
# In dev mode, writes to .dev/table_name.jsonl
self.deps.yt_client.write_table(
    table_path="//tmp/my_pipeline/data",
    rows=[{"id": 1, "name": "Alice"}]
)
# Creates: .dev/data.jsonl
```

**Reading tables:**

```python
# In dev mode, reads from .dev/table_name.jsonl
rows = list(self.deps.yt_client.read_table("//tmp/my_pipeline/data"))
# Reads from: .dev/data.jsonl
```

### Map Operations (dev)

Map operations run locally using subprocess:

1. Creates sandbox directory: `.dev/sandbox_<input>-><output>/`
2. Copies input table to sandbox
3. Executes mapper.py script
4. Collects output to `.dev/<output>.jsonl`

**Example:**

```bash
# Dev mode execution
.dev/sandbox_input->output/
├── input.jsonl
├── code.tar.gz (extracted)
└── operation_wrapper_*.sh
```

### Vanilla Operations (dev)

Vanilla operations run locally using subprocess:

1. Creates sandbox directory: `.dev/<stage_name>_sandbox/`
2. Extracts code archive
3. Executes vanilla.py script
4. Logs output to `.dev/<stage_name>.log`

### YQL Operations (dev)

YQL operations are simulated using DuckDB:

- Joins, filters, aggregations run locally
- Results written to `.dev/` directory
- Full YQL syntax supported

### When to Use Dev Mode

- **Development**: Writing and testing new stages
- **Debugging**: Investigating issues locally
- **Testing**: Validating pipeline logic
- **CI/CD**: Running tests without YT cluster access
- **Learning**: Understanding framework behavior

### Advantages (dev)

- ✅ Fast iteration (no network latency)
- ✅ No YT cluster access required
- ✅ Easy debugging (files are local)
- ✅ Free (no cluster resources used)
- ✅ Works offline

### Limitations (dev)

- ❌ Not suitable for large datasets (limited by local disk)
- ❌ Some YT-specific features may differ
- ❌ Performance characteristics differ from production

## Prod Mode

### How It Works (prod)

Prod mode executes operations on the actual YT cluster:

- **Tables**: Stored on YT cluster at specified paths
- **Operations**: Executed on YT cluster nodes
- **Code Upload**: Code is packaged and uploaded to YT
- **YQL Operations**: Executed using YT's YQL engine

### Configuration (prod)

Set mode in pipeline config:

```yaml
# configs/config.yaml
pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"
```

**Required credentials** (`configs/secrets.env`):

```bash
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token
```

### Table Operations (prod)

**Writing tables:**

```python
# In prod mode, writes to YT cluster
self.deps.yt_client.write_table(
    table_path="//tmp/my_pipeline/data",
    rows=[{"id": 1, "name": "Alice"}]
)
# Creates: //tmp/my_pipeline/data on YT cluster
```

**Reading tables:**

```python
# In prod mode, reads from YT cluster
rows = list(self.deps.yt_client.read_table("//tmp/my_pipeline/data"))
# Reads from: //tmp/my_pipeline/data on YT cluster
```

### Map Operations (prod)

Map operations run on YT cluster:

1. Code is uploaded to `build_folder`
2. YT creates jobs on cluster nodes
3. Each job processes a portion of input table
4. Results are written to output table on cluster

### Vanilla Operations (prod)

Vanilla operations run on YT cluster:

1. Code is uploaded to `build_folder`
2. YT creates job on cluster node
3. Job executes vanilla.py script
4. Logs available in YT web UI

### YQL Operations (prod)

YQL operations execute on YT cluster:

- Uses YT's distributed YQL engine
- Handles large datasets efficiently
- Full YT YQL syntax supported

### When to Use Prod Mode

- **Production**: Running production workloads
- **Large Datasets**: Processing data that doesn't fit locally
- **Performance**: Need cluster performance and parallelism
- **Integration**: Integrating with other YT-based systems

### Advantages (prod)

- ✅ Handles large datasets (distributed storage)
- ✅ High performance (distributed processing)
- ✅ Scalability (cluster resources)
- ✅ Production-ready (real YT environment)

### Limitations (prod)

- ❌ Requires YT cluster access
- ❌ Slower iteration (network latency)
- ❌ Costs cluster resources
- ❌ Harder to debug (remote execution)

## Quick Comparison

```{tab-set}
```{tab-item} Configuration
**Dev Mode:**
```yaml
pipeline:
  mode: "dev"
```

**Prod Mode:**
```yaml
pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"
```

```{tab-item} Credentials
**Dev Mode:**
- No credentials required
- Works offline

**Prod Mode:**
- Requires `configs/secrets.env`
- Must have YT cluster access
```

```{tab-item} Performance
**Dev Mode:**
- Fast iteration
- Limited by local resources
- Sequential execution

**Prod Mode:**
- Distributed processing
- Scales with cluster size
- Parallel execution
```

```{tab-item} Debugging
**Dev Mode:**
- Files in `.dev/` directory
- Immediate error feedback
- Easy to inspect

**Prod Mode:**
- YT web UI for logs
- Remote debugging
- Requires cluster access
```
```

## Switching Between Modes

Switching between modes is simple - just change the `mode` setting:

```yaml
# Development
pipeline:
  mode: "dev"

# Production
pipeline:
  mode: "prod"
```

```{note}
**Same Code, Different Execution**

The same code and configuration work in both modes. The framework handles the differences automatically.
```

**Important considerations:**

1. **Table paths**: Same paths work in both modes (dev mode maps them to `.dev/`)
2. **Credentials**: Prod mode requires `secrets.env` with YT credentials
3. **Build folder**: Prod mode requires `build_folder` for code execution
4. **Code changes**: Dev mode uses local code, prod mode uploads code

## Leaky Abstractions

While the framework tries to abstract away differences, some leak through:

### File Paths

**Dev mode:**

- Tables stored as `.jsonl` files
- Path `//tmp/my_pipeline/data` becomes `.dev/data.jsonl`

**Prod mode:**

- Tables stored on YT cluster
- Path `//tmp/my_pipeline/data` is actual YT path

**What to know:**

- Same code works in both modes
- Path format is the same (`//tmp/...`)
- Dev mode automatically maps paths to local files

### Operation Execution

**Dev mode:**

- Map operations run sequentially (one job)
- Limited parallelism
- Uses local resources

**Prod mode:**

- Map operations run in parallel (multiple jobs)
- Full cluster parallelism
- Uses cluster resources

**What to know:**

- Performance characteristics differ
- Dev mode may not catch all concurrency issues
- Test in prod mode for production workloads

### Code Execution

**Dev mode:**

- Code runs directly from local filesystem
- No code upload needed
- Changes are immediately available

**Prod mode:**

- Code is packaged and uploaded
- Must upload before execution
- Changes require re-upload

**What to know:**

- Dev mode is faster for iteration
- Prod mode requires `build_folder` configuration
- Code structure must be compatible with both modes

### Error Handling

**Dev mode:**

- Errors show in terminal
- Stack traces are immediate
- Easy to debug

**Prod mode:**

- Errors in YT web UI
- Stack traces in operation logs
- Requires YT access to debug

**What to know:**

- Use dev mode for debugging
- Check YT web UI for prod errors
- Logs are crucial for prod debugging

## Debugging Tips

### Dev Mode Debugging

1. **Check `.dev/` directory**: See generated files and tables
2. **Check logs**: Operation logs in `.dev/` directory
3. **Inspect tables**: Open `.jsonl` files directly
4. **Add print statements**: Output appears immediately

### Prod Mode Debugging

1. **Check YT web UI**: View operations and logs
2. **Use logging**: `self.logger` output appears in YT logs
3. **Check operation status**: Monitor in YT web UI
4. **Download results**: Download tables for local inspection

### Common Issues

### Issue: Tables not found in prod mode

- Check table paths exist on YT cluster
- Verify YT credentials are correct
- Check YT proxy URL is accessible

### Issue: Code not updating in prod mode

- Code is uploaded once per pipeline run
- Changes require re-running pipeline
- Check `build_folder` is correct

### Issue: Different behavior in dev vs prod

- Check for YT-specific features
- Verify resource limits
- Test with similar data sizes

## Best Practices

```{tip}
**Development Workflow**

1. Develop and test in dev mode
2. Validate in prod mode with small dataset
3. Deploy to production with full dataset
```

1. **Develop in dev mode**: Faster iteration and debugging
2. **Test in prod mode**: Validate before production deployment
3. **Use same configs**: Keep dev and prod configs similar
4. **Monitor resources**: Check resource usage in prod mode
5. **Version control**: Track config changes between modes

```{warning}
**Test Before Production**

Always test your pipeline in prod mode with a small dataset before running on production data. This helps catch mode-specific issues early.
```

## Next Steps

- Learn about [Configuration](configuration/index.md) management
- Explore [Operations](operations/) for different operation types
- Check out [Examples](../examples/) for mode-specific examples
- Review [Troubleshooting](troubleshooting/configuration.md) for mode-specific issues
