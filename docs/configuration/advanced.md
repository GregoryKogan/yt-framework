# Advanced Configuration

Advanced configuration features including multiple config files, merging, and validation.

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

See [Example: 08_multiple_configs](../../examples/08_multiple_configs/) for a complete example.

## Configuration Merging

The framework uses OmegaConf for configuration management, which supports:

- **Variable interpolation**: Reference other config values
- **Environment variable substitution**: Use `${ENV_VAR}` syntax
- **Config composition**: Merge multiple config files

### Variable Interpolation

Reference other config values within the same file:

```yaml
base_path: //tmp/my_pipeline

client:
  input_table: ${base_path}/input
  output_table: ${base_path}/output
```

### Environment Variables

Use environment variables in configuration:

```yaml
pipeline:
  build_folder: ${BUILD_FOLDER:://tmp/default/build}
```

Uses `BUILD_FOLDER` environment variable if set, otherwise defaults to `//tmp/default/build`.

### Config Composition

Merge multiple config files:

```python
from omegaconf import OmegaConf

base_config = OmegaConf.load("configs/config.yaml")
override_config = OmegaConf.load("configs/config_prod.yaml")
merged = OmegaConf.merge(base_config, override_config)
```

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

### Validation Examples

**Missing required field:**

```yaml
# ❌ Missing enabled_stages
stages: {}

# ✅ Correct
stages:
  enabled_stages:
    - my_stage
```

**Invalid path:**

```yaml
# ❌ Invalid YT path (missing //)
pipeline:
  build_folder: "tmp/build"

# ✅ Correct
pipeline:
  build_folder: "//tmp/build"
```

## Configuration Inheritance

Stage configs inherit from pipeline config where applicable:

```yaml
# configs/config.yaml
pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"

# stages/my_stage/config.yaml
client:
  operations:
    map:
      # Inherits build_folder from pipeline config
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
```

## Configuration Debugging

### Print Configuration

```python
from omegaconf import OmegaConf

# Print full config
print(OmegaConf.to_yaml(self.config))

# Print specific section
print(OmegaConf.to_yaml(self.config.client.operations.map))
```

### Validate Configuration

```python
# Check if key exists
if "build_folder" in self.config.pipeline:
    build_folder = self.config.pipeline.build_folder

# Get with default
build_folder = self.config.pipeline.get("build_folder", "//tmp/default")
```

## See Also

- [Configuration Guide](index.md) - Basic configuration
- [Secrets Management](secrets.md) - Managing credentials
- [Troubleshooting](../troubleshooting/configuration.md) - Configuration issues
