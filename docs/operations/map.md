# Map Operations

Map operations process each row of a table independently, making them perfect for row-by-row transformations, filtering, and data processing tasks.

## Overview

A **map operation** takes an input table, processes each row through a mapper script, and writes results to an output table. The operation runs in parallel across multiple jobs on the YT cluster (or sequentially in dev mode).

**Key characteristics:**

- Processes each row independently
- Parallel execution (multiple jobs)
- Input/output tables required
- Custom code execution (mapper.py)

## Quick Start

### Minimal Example

**Stage** (`stages/process_data/stage.py`):

```python
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.map import run_map

class ProcessDataStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.map,
        )
        
        if not success:
            raise RuntimeError("Map operation failed")
        
        return debug
```

**Stage config** (`stages/process_data/config.yaml`):

```yaml
client:
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

**Mapper script** (`stages/process_data/src/mapper.py`):

```python
#!/usr/bin/env python3
import sys
import json
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

def main():
    # Load configuration
    config = OmegaConf.load(get_config_path())
    
    # Process each input row
    for line in sys.stdin:
        row = json.loads(line)
        
        # Transform row
        output_row = {
            "id": row["id"],
            "processed_value": row["value"] * 2
        }
        
        # Write output row
        print(json.dumps(output_row), flush=True)

if __name__ == "__main__":
    main()
```

See [Example: 04_map_operation](../../examples/04_map_operation/) for a complete example.

## Mapper Script

The mapper script (`src/mapper.py`) is executed for each row of the input table.

### Input/Output Format

- **Input**: One JSON object per line via stdin
- **Output**: One JSON object per line to stdout

**Example:**

```python
import sys
import json

for line in sys.stdin:
    # Read input row
    row = json.loads(line)
    
    # Process row
    processed = process_row(row)
    
    # Write output row
    print(json.dumps(processed), flush=True)
```

### Configuration Access

Access stage configuration in mapper script:

```python
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

config = OmegaConf.load(get_config_path())

# Access job config
multiplier = config.job.multiplier
prefix = config.job.prefix

# Access client config (read-only)
input_table = config.client.operations.map.input_table
```

**Stage config** (`stages/my_stage/config.yaml`):

```yaml
job:
  multiplier: 2
  prefix: "processed_"

client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
```

### Error Handling

Handle errors gracefully:

```python
import sys
import json
import traceback

for line in sys.stdin:
    try:
        row = json.loads(line)
        output_row = process_row(row)
        print(json.dumps(output_row), flush=True)
    except Exception as e:
        # Log error and skip row
        print(f"Error processing row: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        continue
```

**Important:** Failed jobs will cause the operation to fail if `max_failed_job_count` is exceeded.

### Logging

Use YT logging utilities:

```python
import logging
from ytjobs.logging.logger import get_logger

logger = get_logger("mapper", level=logging.INFO)

for line in sys.stdin:
    row = json.loads(line)
    logger.info(f"Processing row {row['id']}")
    # Process row...
```

Logs appear in YT operation logs (prod mode) or `.dev/` directory (dev mode).

## Configuration

### Basic Configuration

```yaml
client:
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

### Required Fields

- **`input_table`**: YT path to input table
- **`output_table`**: YT path to output table
- **`resources`**: Resource configuration

### Resource Configuration

```yaml
resources:
  pool: default              # YT pool name
  pool_tree: null            # Optional: pool tree
  memory_limit_gb: 4        # Memory per job (GB)
  cpu_limit: 2               # CPU cores per job
  job_count: 2               # Number of parallel jobs
  gpu_limit: 0               # GPU count (default: 0)
  user_slots: null           # Optional: user slots limit
```

**Resource guidelines:**

- **Memory**: Allocate based on row size and processing needs
- **CPU**: More CPUs = faster processing per job
- **Job count**: More jobs = better parallelism (up to data size)
- **GPU**: Set to 1+ for GPU workloads (requires custom Docker)

### Advanced Configuration

**Max failed jobs:**

```yaml
client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      max_failed_job_count: 1  # Fail operation after N failed jobs
      resources:
        # ...
```

**Custom Docker:**

```yaml
client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      resources:
        docker_image: my-registry/my-image:latest
        # ...
```

See [Docker Guide](../advanced/docker.md) for details.

**Checkpoints:**

```yaml
client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      checkpoint:
        checkpoint_base: //tmp/my_pipeline/checkpoints
        local_checkpoint_path: /path/to/local/model.pth
      resources:
        # ...
```

See [Checkpoints Guide](../advanced/checkpoints.md) for details.

## Running Map Operations

### From Stage

Use `run_map()` function:

```python
from yt_framework.operations.map import run_map

success = run_map(
    context=self.context,
    operation_config=self.config.client.operations.map,
)
```

### Operation Flow

1. **Code upload**: Code is packaged and uploaded to YT (prod mode)
2. **Job creation**: YT creates multiple jobs based on `job_count`
3. **Row distribution**: Input table rows are distributed across jobs
4. **Execution**: Each job runs mapper.py for its assigned rows
5. **Output collection**: Results are written to output table
6. **Completion**: Operation completes when all jobs finish

### Dev Mode Behavior

In dev mode:

- Runs sequentially (single job)
- Creates sandbox directory: `.dev/sandbox_<input>-><output>/`
- Input table copied to sandbox
- Mapper script executed locally
- Output written to `.dev/<output>.jsonl`

## Advanced Topics

### Multiple Map Operations

Run multiple map operations in one stage:

```python
class ProcessAndValidateStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # First map operation
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.process,
        )
        if not success:
            raise RuntimeError("Process failed")
        
        # Second map operation
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.validate,
        )
        if not success:
            raise RuntimeError("Validate failed")
        
        return debug
```

See [Example: 09_multiple_operations](../../examples/09_multiple_operations/) for details.

### GPU Processing

For GPU workloads:

1. **Custom Docker**: Create Docker image with GPU support
2. **GPU resources**: Set `gpu_limit: 1` or higher
3. **GPU code**: Use GPU libraries in mapper script

**Example:**

```yaml
client:
  operations:
    map:
      resources:
        docker_image: my-registry/gpu-image:latest
        gpu_limit: 1
        memory_limit_gb: 16
```

See [Example: video_gpu](../../examples/video_gpu/) for GPU processing example.

### Checkpoint Usage

Load ML models from checkpoints:

```python
import os
from ytjobs.config import get_config_path
from omegaconf import OmegaConf

# Checkpoint file is available in sandbox
checkpoint_file = os.environ.get("CHECKPOINT_FILE")
if checkpoint_file:
    # Load model from checkpoint
    model = load_model(checkpoint_file)
```

See [Checkpoints Guide](../advanced/checkpoints.md) for details.

## Best Practices

1. **Idempotent processing**: Mapper should produce same output for same input
2. **Error handling**: Handle errors gracefully, don't crash on bad rows
3. **Resource allocation**: Allocate resources based on actual needs
4. **Job count**: Balance parallelism vs overhead (start with 2-4 jobs)
5. **Logging**: Log important operations for debugging
6. **Testing**: Test mapper locally before running on cluster

## Common Patterns

### Row Transformation

```python
for line in sys.stdin:
    row = json.loads(line)
    output_row = {
        "id": row["id"],
        "transformed": transform(row["data"])
    }
    print(json.dumps(output_row), flush=True)
```

### Row Filtering

```python
for line in sys.stdin:
    row = json.loads(line)
    if should_include(row):
        print(json.dumps(row), flush=True)
```

### Row Enrichment

```python
for line in sys.stdin:
    row = json.loads(line)
    enriched = {
        **row,
        "computed_field": compute(row)
    }
    print(json.dumps(enriched), flush=True)
```

## Troubleshooting

### Issue: Operation fails immediately

- Check mapper script syntax
- Verify input table exists
- Check resource limits

### Issue: Some rows fail

- Check `max_failed_job_count` setting
- Review error logs in YT web UI
- Add error handling in mapper

### Issue: Slow performance

- Increase `job_count` for parallelism
- Increase `cpu_limit` per job
- Check for bottlenecks in mapper code

### Issue: Out of memory

- Increase `memory_limit_gb`
- Check row size and processing needs
- Optimize mapper code

## Next Steps

- Learn about [Vanilla Operations](vanilla.md)
- Explore [Advanced Topics](../advanced/) (Docker, checkpoints)
- Check out [Examples](../../examples/) for more patterns
