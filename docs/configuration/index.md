# Configuration

YT Framework uses YAML files for configuration and environment files for secrets. Understanding the configuration system is essential for building effective pipelines.

```{toctree}
:maxdepth: 1

secrets
advanced
../dev-vs-prod
../pipelines-and-stages
```

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

- Learn about [Secrets Management](secrets.md) for credentials
- Explore [Advanced Configuration](advanced.md) for multiple configs and merging
- Check [Dev vs Prod](../dev-vs-prod.md) for mode-specific configuration
- Review [Operations](../operations/) for operation-specific configuration
