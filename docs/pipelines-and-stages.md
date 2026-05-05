# Pipelines and stages

A **pipeline** runs an ordered list of **stages**. The framework loads config, builds a YT client for the current mode, runs upload when needed, then calls each stage’s `run` method in sequence.

## Pipelines

The pipeline is responsible for:

- Discovering or registering stages
- Loading YAML and secrets
- Initializing the YT client (dev or prod)
- Uploading code in prod
- Invoking stages in `enabled_stages` order
- Surfacing failures (a raised exception stops the run)

### DefaultPipeline

```{tip}
**Prefer DefaultPipeline**

It discovers stages under `stages/` so you rarely need custom registration logic.
```

Point `__main__` at `DefaultPipeline.main()`:

```python
# pipeline.py
from yt_framework.core.pipeline import DefaultPipeline

if __name__ == "__main__":
    DefaultPipeline.main()
```

Discovery does the following:

1. List subdirectories of `stages/`.
2. Import `stage.py` from each.
3. Register every `BaseStage` subclass found.
4. Run stages in the order given by `configs/config.yaml` → `stages.enabled_stages`.

Example layout:

```text
my_pipeline/
├── pipeline.py
├── configs/
│   └── config.yaml
└── stages/
    ├── stage1/
    │   ├── stage.py
    │   └── config.yaml
    └── stage2/
        ├── stage.py
        └── config.yaml
```

Working tree: [01_hello_world](https://github.com/GregoryKogan/yt-framework/tree/main/examples/01_hello_world/).

### BasePipeline

Use `BasePipeline` when you need explicit registration or setup hooks (conditional stages, tests, unusual layout).

```python
# pipeline.py
from yt_framework.core.pipeline import BasePipeline
from yt_framework.core.registry import StageRegistry
from stages.stage1.stage import Stage1
from stages.stage2.stage import Stage2

class MyPipeline(BasePipeline):
    def setup(self):
        registry = StageRegistry()
        registry.add_stage(Stage1)
        registry.add_stage(Stage2)
        self.set_stage_registry(registry)

if __name__ == "__main__":
    MyPipeline.main()
```

Reasons you might choose this:

- Custom `setup()` work before stages exist
- Stages not discoverable from a flat `stages/` tree
- Tests that inject a small fixed registry

For normal repos, `DefaultPipeline` is enough.

## Stages

A **stage** is one unit of work: one `BaseStage` subclass, one `config.yaml`, and optional `src/` or `requirements.txt` for uploaded jobs.

### BaseStage

`BaseStage` gives you:

- Parsed stage config at `self.config` (from `stages/<name>/config.yaml`)
- `self.deps.yt_client`, pipeline-wide settings, paths
- `self.logger`
- Operation-related context on `self.context` where applicable

### Directory layout

```text
stages/
└── stage_name/
    ├── stage.py          # required
    ├── config.yaml       # required
    ├── src/              # optional (mapper, vanilla, etc.)
    │   ├── mapper.py
    │   └── vanilla.py
    └── requirements.txt  # optional extra pip deps for the job bundle
```

### Minimal stage

```python
# stages/my_stage/stage.py
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage

class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Running my stage")
        return debug
```

### Stage with tables

```yaml
# stages/my_stage/config.yaml
client:
  input_table: //tmp/my_pipeline/input
  output_table: //tmp/my_pipeline/output
```

```python
# stages/my_stage/stage.py
class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        input_table = self.config.client.input_table
        output_table = self.config.client.output_table

        rows = list(self.deps.yt_client.read_table(input_table))
        processed = [process_row(row) for row in rows]

        # Default overwrites the output. Pass append=True to append if the table already exists.
        self.deps.yt_client.write_table(output_table, processed)
        return debug
```

### `self.deps`

Typical fields:

- `self.deps.yt_client` — read/write tables, YQL helpers, etc.
- `self.deps.pipeline_config` — merged pipeline section from `configs/config.yaml`
- `self.deps.configs_dir` — directory that holds `secrets.env`

```python
class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        yt = self.deps.yt_client
        mode = self.deps.pipeline_config.pipeline.mode
        table_path = self.config.client.output_table
        self.logger.info("mode=%s table=%s", mode, table_path)
        return debug
```

### Passing data between stages (`debug`)

`debug` is a mutable mapping carried from stage to stage. Put small flags or summaries here; put large data in YT tables.

```python
class Stage1(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        debug["result"] = "some value"
        debug["count"] = 42
        return debug

class Stage2(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        result = debug.get("result")
        count = debug.get("count", 0)
        self.logger.info("from stage1: result=%s count=%s", result, count)
        return debug
```

```{warning}
**Keep `debug` small**

It is an in-memory dict passed through the driver process. Metadata only: for big payloads, write a table and pass the path in `debug` if needed.
```

### Stage configuration

Access nested YAML through `self.config`:

```yaml
# stages/my_stage/config.yaml
job:
  multiplier: 2
  prefix: "processed_"

client:
  input_table: //tmp/my_pipeline/input
  output_table: //tmp/my_pipeline/output
  operations:
    map:
      resources:
        memory_limit_gb: 4
        cpu_limit: 2
```

```python
class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        multiplier = self.config.job.multiplier
        prefix = self.config.job.prefix
        input_table = self.config.client.input_table
        output_table = self.config.client.output_table
        memory = self.config.client.operations.map.resources.memory_limit_gb
        self.logger.info(
            "job %s %s tables %s -> %s map_mem_gb=%s",
            prefix,
            multiplier,
            input_table,
            output_table,
            memory,
        )
        return debug
```

### Lifecycle (conceptual)

1. **Discovery** (`DefaultPipeline`): scan `stages/`.
2. **Registration**: stage classes recorded in a registry.
3. **Construction**: one instance per stage with dependencies injected.
4. **Config load**: OmegaConf-style object from `config.yaml`.
5. **Run**: `run(debug)` executes; return value becomes the next stage’s `debug`.
6. **Next stage**: repeat until the list ends or an error is raised.

### Order

```yaml
# configs/config.yaml
stages:
  enabled_stages:
    - create_input
    - process_data
    - validate_output
```

```{note}
**Sequential**

Stages run one after another. An uncaught exception aborts the pipeline.
```

### Injection details

`self.deps` follows `PipelineStageDependencies`. See **Core injection (`self.deps`)** under [API reference](reference/api.md) (`yt_framework.core.dependencies`).

### Multi-stage sample

[02_multi_stage_pipeline](https://github.com/GregoryKogan/yt-framework/tree/main/examples/02_multi_stage_pipeline/).

## Practices that tend to help

1. One main responsibility per stage.
2. Names that match what the stage does in YT terms.
3. Large payloads in tables, not in `debug`.
4. Fail fast: raise on invalid input instead of returning partial success silently.
5. Log decisions you will need when reading `.dev` logs or YT operation logs.
6. Exercise new stages in dev mode before pointing prod traffic at them.

## Next steps

- [Configuration](configuration/index.md)
- [Dev vs prod](dev-vs-prod.md)
- [Operations overview](operations/index.md)
- [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples/)
