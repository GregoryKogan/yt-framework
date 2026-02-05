# Multiple Configs Example

Demonstrates using multiple configuration files for different environments or scenarios. Shows how to switch between configs using the `--config` flag.

## What It Demonstrates

- **Multiple Configs**: Using different config files for different scenarios
- **Config Switching**: Using `--config` flag to select config
- **Environment-Specific Configs**: Different configs for dev, prod, etc.
- **Config Organization**: Organizing configs for different use cases

## Features

- Multiple config files (default, custom, large)
- Config switching via CLI argument
- Environment-specific settings
- Flexible configuration management

## Running

**Use default config:**
```bash
python pipeline.py
```

**Use custom config:**
```bash
python pipeline.py --config configs/config_custom.yaml
```

**Use large dataset config:**
```bash
python pipeline.py --config configs/config_large.yaml
```

## Files

- `pipeline.py`: Pipeline entry point
- `configs/config.yaml`: Default configuration
- `configs/config_custom.yaml`: Custom configuration
- `configs/config_large.yaml`: Large dataset configuration
- `stages/process_data/stage.py`: Stage that processes data
- `stages/process_data/config.yaml`: Stage configuration

## Key Concepts

- Config files can have different settings for different scenarios
- Use `--config` flag to specify which config to use
- Configs can differ in mode, resources, table paths, etc.
- Useful for testing different configurations

## Config Examples

**Default config:**
```yaml
pipeline:
  mode: "dev"
```

**Custom config:**
```yaml
pipeline:
  mode: "prod"
  build_folder: "//tmp/custom/build"
```

**Large dataset config:**
```yaml
pipeline:
  mode: "prod"
  build_folder: "//tmp/large/build"

stages:
  enabled_stages:
    - process_data

# Stage config might have different resources
```

## Next Steps

- See [Configuration Guide](../../docs/configuration/index.md) for configuration details
- See [Dev vs Prod](../../docs/dev-vs-prod.md) for mode differences
