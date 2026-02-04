# Configuration

YT Framework uses YAML files for configuration and environment files for secrets. Understanding the configuration system is essential for building effective pipelines.

## Configuration Files

Configuration is organized into multiple files:

- **Pipeline config** (`configs/config.yaml`): Pipeline-level settings
- **Stage configs** (`stages/<stage_name>/config.yaml`): Stage-specific settings
- **Secrets** (`configs/secrets.env`): Credentials and sensitive data

## Pipeline Configuration

The pipeline configuration file (`configs/config.yaml`) controls pipeline-level behavior:

```yaml
stages:
  enabled_stages:
    - stage1
    - stage2
    - stage3

pipeline:
  mode: "dev"  # or "prod"
  build_folder: "//tmp/my_pipeline/build"  # Required for operations with code
  build_code_dir: null  # Optional: custom code directory
```

### Stages Section

**`enabled_stages`** (required): List of stage names to execute, in order.

```yaml
stages:
  enabled_stages:
    - create_input
    - process_data
    - validate_output
```

Only stages listed here will be executed. Stages are executed in the order specified.

### Pipeline Section

**`mode`** (optional, default: "dev"): Execution mode.

- `"dev"`: Local development mode (file system simulation)
- `"prod"`: Production mode (YT cluster execution)

**`build_folder`** (required for code execution): YT path where code will be uploaded.

```yaml
pipeline:
  build_folder: "//tmp/my_pipeline/build"
```

Required if any enabled stages have `src/` directory (for map or vanilla operations).

**`build_code_dir`** (optional): Custom directory containing code to upload.

```yaml
pipeline:
  build_code_dir: "/path/to/custom/code"
```

If not specified, uses the pipeline directory. Useful for monorepos or shared code.

## Stage Configuration

Each stage has its own configuration file at `stages/<stage_name>/config.yaml`:

```yaml
# stages/my_stage/config.yaml
job:
  # Job-specific settings
  multiplier: 2
  prefix: "processed_"

client:
  # Client settings
  input_table: //tmp/my_pipeline/input
  output_table: //tmp/my_pipeline/output
  
  # Operation configurations
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      resources:
        pool: default
        memory_limit_gb: 4
        cpu_limit: 2
        job_count: 2
```

### Configuration Structure

Configuration is organized into sections:

- **`job`**: Settings used by mapper.py or vanilla.py scripts
- **`client`**: Settings used by the stage itself
  - **`operations`**: Operation-specific settings (map, vanilla, etc.)

### Accessing Configuration

In stage code:

```python
class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Access job config
        multiplier = self.config.job.multiplier
        
        # Access client config
        input_table = self.config.client.input_table
        
        # Access nested config
        memory = self.config.client.operations.map.resources.memory_limit_gb
        
        return debug
```

In mapper.py or vanilla.py:

```python
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

config = OmegaConf.load(get_config_path())
multiplier = config.job.multiplier
```

## Secrets Management

Sensitive data (credentials, tokens, etc.) is stored in `configs/secrets.env`:

```bash
# configs/secrets.env
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token

# Optional: S3 credentials
S3_ENDPOINT=https://your-s3-endpoint.com
S3_DOWNLOAD_ACCESS_KEY=your-download-access-key
S3_DOWNLOAD_SECRET_KEY=your-download-secret-key
# For upload operations:
S3_UPLOAD_ACCESS_KEY=your-upload-access-key
S3_UPLOAD_SECRET_KEY=your-upload-secret-key
```

### YT Credentials

Required for production mode:

```bash
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token
```

### S3 Credentials

Required for S3 integration:

```bash
S3_ENDPOINT=https://your-s3-endpoint.com
S3_DOWNLOAD_ACCESS_KEY=your-download-access-key
S3_DOWNLOAD_SECRET_KEY=your-download-secret-key
# For upload operations:
S3_UPLOAD_ACCESS_KEY=your-upload-access-key
S3_UPLOAD_SECRET_KEY=your-upload-secret-key
```

### Loading Secrets

Secrets are automatically loaded by the framework. Access them in stages:

```python
from yt_framework.utils.env import load_secrets

class MyStage(BaseStage):
    def __init__(self, deps, logger):
        super().__init__(deps, logger)
        
        # Load secrets
        secrets = load_secrets(self.deps.configs_dir)
        yt_proxy = secrets.get("YT_PROXY")
        yt_token = secrets.get("YT_TOKEN")
```

### Security Best Practices

1. **Never commit secrets**: Add `configs/secrets.env` to `.gitignore`
2. **Use example files**: Create `configs/secrets.example.env` with placeholder values
3. **Rotate credentials**: Regularly update tokens and keys
4. **Use environment variables**: In CI/CD, use environment variables instead of files

## Multiple Configuration Files

You can use multiple configuration files for different environments or scenarios:

```bash
configs/
├── config.yaml          # Default config
├── config_dev.yaml      # Development config
├── config_prod.yaml     # Production config
└── config_large.yaml    # Large dataset config
```

### Using Different Configs

Specify the config file via `--config` flag:

```bash
# Use default config
python pipeline.py

# Use specific config
python pipeline.py --config configs/config_prod.yaml
```

### Example: Environment-Specific Configs

**Development config** (`configs/config_dev.yaml`):

```yaml
stages:
  enabled_stages:
    - create_test_data
    - process_data

pipeline:
  mode: "dev"
  build_folder: "//tmp/my_pipeline/dev/build"
```

**Production config** (`configs/config_prod.yaml`):

```yaml
stages:
  enabled_stages:
    - create_input
    - process_data
    - validate_output
    - upload_results

pipeline:
  mode: "prod"
  build_folder: "//home/production/my_pipeline/build"
```

See [Example: 08_multiple_configs](../examples/08_multiple_configs/) for a complete example.

## Configuration Merging

The framework uses OmegaConf for configuration management, which supports:

- **Variable interpolation**: Reference other config values
- **Environment variable substitution**: Use `${ENV_VAR}` syntax
- **Config composition**: Merge multiple config files

### Variable Interpolation

```yaml
base_path: //tmp/my_pipeline

client:
  input_table: ${base_path}/input
  output_table: ${base_path}/output
```

### Environment Variables

```yaml
pipeline:
  build_folder: ${BUILD_FOLDER:://tmp/default/build}
```

Uses `BUILD_FOLDER` environment variable if set, otherwise defaults to `//tmp/default/build`.

## Configuration Validation

The framework validates configuration at runtime:

- **Required fields**: Missing required fields raise errors
- **Type checking**: Incorrect types raise errors
- **Path validation**: Invalid YT paths raise errors

### Common Configuration Errors

1. **Missing `enabled_stages`**: Pipeline won't know which stages to run
2. **Missing `build_folder`**: Required for stages with `src/` directory
3. **Invalid stage names**: Stage names must match directory names
4. **Missing stage config**: Each stage must have `config.yaml`

## Configuration Examples

### Simple Pipeline

```yaml
# configs/config.yaml
stages:
  enabled_stages:
    - create_table

pipeline:
  mode: "dev"
```

### Pipeline with Code Execution

```yaml
# configs/config.yaml
stages:
  enabled_stages:
    - process_data

pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"
```

### Pipeline with Multiple Operations

```yaml
# configs/config.yaml
stages:
  enabled_stages:
    - process_and_validate

pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"
```

```yaml
# stages/process_and_validate/config.yaml
client:
  operations:
    process:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/processed
      resources:
        memory_limit_gb: 8
        cpu_limit: 4
    validate:
      resources:
        memory_limit_gb: 4
        cpu_limit: 2
```

## Next Steps

- Learn about [Dev vs Prod](dev-vs-prod.md) modes
- Explore [Operations](operations/) configuration
- Check out [Examples](../examples/) for configuration patterns
