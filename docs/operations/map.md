# Map operations

A map job reads an input table, runs your `mapper.py` once per row (grouped into YT tasks in prod), and writes JSON lines to an output table. Use it when the transform is row-local in Python and does not fit YQL cleanly.

## Overview

```{tip}
**When map fits**

Reach for map when each output row (or zero rows) depends only on one input row and you need arbitrary Python. Prefer YQL for set-style SQL operations.
```

**Behavior:**

- Stdin/stdout JSON lines (one object per line); flush after each printed row.
- Prod: many tasks; dev: a single local subprocess and a sandbox under `.dev/`.
- Requires `input_table`, `output_table`, and `resources` in YAML.

```{warning}
**Mapper Script Requirements**

Your mapper script must read from stdin and write to stdout. Each line is a JSON-encoded row. Make sure to flush output after each row.
```

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

See [Example: 04_map_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/04_map_operation/) for a complete example.

(append-output)=
## Append output

Use `append: true` under `client.operations.map` when mapper rows should be appended to an existing output table rather than replacing it.

On the cluster, the output table must already exist and incoming rows must match its schema (including typed columns). In dev mode, if the output `.jsonl` already exists, mapper stdout is appended after the current lines.

```yaml
client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      append: true
      resources:
        pool: default
```

## Mapper Script

The mapper script (`src/mapper.py`) is executed for each row of the input table.

### Input/Output Format

- **Input**: One JSON object per line via stdin
- **Output**: One JSON object per line to stdout

**Example:**

```python
import sys
import json

def process_row(row: dict) -> dict:
    return row  # replace with your transform

for line in sys.stdin:
    row = json.loads(line)
    processed = process_row(row)
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

**Resources (rule of thumb):**

- Raise `memory_limit_gb` when a single row plus model weights no longer fits.
- `cpu_limit` helps per-task throughput; `job_count` spreads rows across tasks.
- `gpu_limit` > 0 only works with an image that actually exposes GPUs to Python.

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

**Max row weight:**

`max_row_weight` defaults to `128M` for map operations (that is also the maximum the cluster accepts). Override it per operation with a value at or below `128M`:

```yaml
client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      max_row_weight: 64M
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

See [Example: 09_multiple_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/09_multiple_operations/) for details.

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

See [Example: video_gpu](https://github.com/GregoryKogan/yt-framework/tree/main/examples/video_gpu/) for GPU processing example.

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

## Practices

1. Keep transforms deterministic for the same input row.
2. Decide whether a bad row should skip, fail the task, or poison the whole op (`max_failed_job_count`).
3. Size memory from peak RSS you observe in dev, not from guesses.
4. Log row ids sparingly; high-volume logs hurt both dev and prod.
5. Run dev mode on a slice of production schema before widening `job_count` in prod.

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

| Symptom | Checks |
|---------|--------|
| Op fails at start | Mapper entrypoint path, import errors in bundle, missing input table |
| Sparse row failures | `max_failed_job_count`, stderr in failing task |
| Slow wall clock | `job_count` too low for data size, CPU-bound Python, remote I/O inside mapper |
| OOM kills | Raise `memory_limit_gb`, shrink per-row allocations, stream instead of buffering |

## Next steps

- [Vanilla](vanilla.md) for non-table jobs
- [Advanced](../advanced/index.md) for Docker and checkpoints
- [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples/)
