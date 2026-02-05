# Pipelines and Stages

Pipelines and stages are the core building blocks of YT Framework. Understanding how they work is essential for building effective data processing workflows.

## Pipelines

A **pipeline** is a collection of stages that execute in sequence. The pipeline manages:

- Stage discovery and registration
- Configuration loading
- YT client initialization
- Code upload (if needed)
- Stage execution order
- Error handling

### DefaultPipeline

```{tip}
**Use DefaultPipeline**

`DefaultPipeline` is recommended for most use cases. It automatically discovers stages, reducing boilerplate and making your code cleaner.
```

`DefaultPipeline` automatically discovers and registers stages from the `stages/` directory. This is the recommended approach for most use cases.

**Usage:**

```python
# pipeline.py
from yt_framework.core.pipeline import DefaultPipeline

if __name__ == "__main__":
    DefaultPipeline.main()
```

**How it works:**

1. Scans `stages/` directory for subdirectories
2. Looks for `stage.py` files in each subdirectory
3. Imports and registers any `BaseStage` subclasses found
4. Executes stages in the order specified by `enabled_stages` in config

**Example structure:**

```plaintext
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

See [Example: 01_hello_world](https://github.com/GregoryKogan/yt-framework/tree/main/examples/01_hello_world/) for a complete example.

### BasePipeline

`BasePipeline` allows manual stage registration. Use this when you need custom pipeline logic or want explicit control over stage registration.

**Usage:**

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

**When to use BasePipeline:**

- Custom pipeline initialization logic
- Conditional stage registration
- Integration with external systems
- Advanced use cases

For most users, `DefaultPipeline` is sufficient.

## Stages

A **stage** is a single unit of work in a pipeline. Each stage:

- Has its own configuration file
- Receives dependencies (YT client, configs, etc.)
- Can pass data to subsequent stages via context
- Is executed independently

### BaseStage

All stages inherit from `BaseStage`. The base class provides:

- Automatic config loading from `stages/<stage_name>/config.yaml`
- Access to YT client via `self.deps.yt_client`
- Access to pipeline config via `self.deps.pipeline_config`
- Logger via `self.logger`
- Stage context via `self.context`

### Stage Structure

Each stage must follow this directory structure:

```plaintext
stages/
└── stage_name/
    ├── stage.py          # Stage implementation (required)
    ├── config.yaml       # Stage configuration (required)
    ├── src/              # Optional: source code for operations
    │   ├── mapper.py     # For map operations
    │   └── vanilla.py     # For vanilla operations
    └── requirements.txt  # Optional: Python dependencies
```

### Creating a Stage

**Minimal stage:**

```python
# stages/my_stage/stage.py
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage

class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Running my stage")
        # Stage logic here
        return debug
```

**Stage with configuration:**

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
        
        # Read from input
        rows = list(self.deps.yt_client.read_table(input_table))
        
        # Process data
        processed = [process_row(row) for row in rows]
        
        # Write to output
        self.deps.yt_client.write_table(output_table, processed)
        
        return debug
```

### Stage Dependencies

Stages receive dependencies through `self.deps`:

- **`self.deps.yt_client`**: YT client for table operations
- **`self.deps.pipeline_config`**: Pipeline-level configuration
- **`self.deps.configs_dir`**: Path to configs directory (for secrets)

**Example:**

```python
class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Access YT client
        yt = self.deps.yt_client
        
        # Access pipeline config
        mode = self.deps.pipeline_config.pipeline.mode
        
        # Access stage config
        table_path = self.config.client.output_table
        
        return debug
```

### Stage Context

Stages can pass data to subsequent stages via the `debug` context dictionary:

```python
class Stage1(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Add data to context
        debug["result"] = "some value"
        debug["count"] = 42
        return debug

class Stage2(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Access data from previous stage
        result = debug.get("result")
        count = debug.get("count", 0)
        return debug
```

```{warning}
**Context Size Limits**

The context is a simple dictionary. Use it for small amounts of data (metadata, counts, flags). For large datasets, use YT tables instead.
```

**Important:** The context is a simple dictionary. Use it for small amounts of data. For large datasets, use YT tables instead.

### Stage Configuration

Each stage has its own `config.yaml` file. Configuration is accessed via `self.config`:

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
# stages/my_stage/stage.py
class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Access job config
        multiplier = self.config.job.multiplier
        prefix = self.config.job.prefix
        
        # Access client config
        input_table = self.config.client.input_table
        output_table = self.config.client.output_table
        
        # Access nested config
        memory = self.config.client.operations.map.resources.memory_limit_gb
        
        return debug
```

### Stage Lifecycle

1. **Discovery** (DefaultPipeline only): Framework scans `stages/` directory
2. **Registration**: Stage class is registered in stage registry
3. **Initialization**: Stage instance is created with dependencies
4. **Config Loading**: Stage config is loaded from `config.yaml`
5. **Execution**: `run()` method is called with context
6. **Completion**: Context is returned and passed to next stage

### Stage Execution Order

Stages execute in the order specified by `enabled_stages` in the pipeline config:

```yaml
# configs/config.yaml
stages:
  enabled_stages:
    - create_input
    - process_data
    - validate_output
```

```{note}
**Sequential Execution**

Stages are executed sequentially. If a stage fails, the pipeline stops. Make sure each stage handles errors appropriately.
```

Stages are executed sequentially. If a stage fails, the pipeline stops.

### Multiple Stages Example

See [Example: 02_multi_stage_pipeline](https://github.com/GregoryKogan/yt-framework/tree/main/examples/02_multi_stage_pipeline/) for a complete example with multiple stages and context passing.

## Best Practices

1. **Keep stages focused**: Each stage should do one thing well
2. **Use descriptive names**: Stage names should clearly indicate their purpose
3. **Pass data via tables**: Use YT tables for large datasets, not context
4. **Handle errors**: Stages should raise exceptions on failure
5. **Log progress**: Use `self.logger` to log important operations
6. **Test locally**: Use dev mode for development and testing

## Next Steps

- Learn about [Configuration](configuration/index.md) management
- Understand [Dev vs Prod](dev-vs-prod.md) modes
- Explore [Operations](operations/) for different operation types
- Review [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples/) for complete pipeline examples
