# Configuration

YAML describes pipeline and stage behavior. Credentials live in `configs/secrets.env` (never commit that file).

```{toctree}
:maxdepth: 1

cluster-requirements
secrets
advanced
../dev-vs-prod
../pipelines-and-stages
```

## Files

| File | Role |
|------|------|
| `configs/config.yaml` | Pipeline-wide settings: mode, `build_folder`, enabled stages, optional upload lists |
| `stages/<name>/config.yaml` | Settings for one stage (tables, operation blocks, resources) |
| `configs/secrets.env` | `YT_*`, S3 keys, Docker registry auth, etc. |

```{warning}
**Prod jobs run in cluster images**

Uploaded job code imports `ytjobs` and your modules. The image YT uses for those jobs must already contain matching Python packages. See [Cluster requirements](cluster-requirements.md).
```

## Pipeline config (`configs/config.yaml`)

```yaml
stages:
  enabled_stages:
    - stage1
    - stage2
    - stage3

pipeline:
  mode: "dev"  # or "prod"
  build_folder: "//tmp/my_pipeline/build"  # required when prod uploads code
```

### `stages.enabled_stages`

Required ordered list of stage directory names. Only listed stages run.

### `pipeline.mode`

- `dev` — local simulation (see [Dev vs prod](../dev-vs-prod.md))
- `prod` — YT cluster

Default in examples is often `dev`.

### `pipeline.build_folder`

YT Cypress path for uploaded bundles when a stage ships `src/` (map, vanilla, etc.). Omit only if every enabled stage is pure driver-side work with no upload.

### `pipeline.upload_modules`

Extra importable top-level modules to pack beside `ytjobs`:

```yaml
pipeline:
  upload_modules: [my_package, company_utils]
```

`ytjobs` is always included.

### `pipeline.upload_paths`

Extra directories copied into the archive:

```yaml
pipeline:
  upload_paths:
    - { source: "./lib/shared", target: "shared" }
    - { source: "./experiments/utils" }  # target defaults to last path segment
```

Paths are relative to the pipeline root. Details: [Code upload](../advanced/code-upload.md).

## Stage config (`stages/<stage>/config.yaml`)

```yaml
job:
  multiplier: 2
  prefix: "processed_"

client:
  input_table: //tmp/my_pipeline/input
  output_table: //tmp/my_pipeline/output
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

Conventions:

- `job` — values mapper/vanilla entrypoints read via `get_config_path()`.
- `client` — values the stage class reads from `self.config`.
- `client.operations.<name>` — per-operation settings (names depend on how you wire the stage).

### Reading config in Python

Stage class:

```python
class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        multiplier = self.config.job.multiplier
        input_table = self.config.client.input_table
        memory = self.config.client.operations.map.resources.memory_limit_gb
        return debug
```

Inside uploaded `mapper.py` / `vanilla.py`:

```python
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

config = OmegaConf.load(get_config_path())
multiplier = config.job.multiplier
```

## Examples

### Dev-only pipeline

```yaml
stages:
  enabled_stages:
    - create_table

pipeline:
  mode: "dev"
```

### Prod with upload

```yaml
stages:
  enabled_stages:
    - process_data

pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"
```

### Two operations in one stage

`configs/config.yaml` as above; stage YAML:

```yaml
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

## `max_row_weight`

Default is `128M` for operations and YQL helpers that accept it; larger values are rejected at client build time.

Per-operation YAML:

```yaml
client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      max_row_weight: 64M
      resources:
        pool: default
```

Runtime YQL helper:

```python
self.deps.yt_client.select_columns(
    input_table="//tmp/in",
    output_table="//tmp/out",
    columns=["id"],
    max_row_weight="64M",
)
```

## Next steps

- [Cluster requirements](cluster-requirements.md)
- [Secrets](secrets.md)
- [Advanced configuration](advanced.md) (multiple files, merging)
- [Dev vs prod](../dev-vs-prod.md)
- [Operations](../operations/index.md)
