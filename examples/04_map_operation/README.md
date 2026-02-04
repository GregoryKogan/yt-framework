# Map Operation Example

Demonstrates map operations for row-by-row data processing. Shows how to write mapper scripts and configure map operations.

## What It Demonstrates

- **Map Operations**: Processing each row of a table independently
- **Mapper Scripts**: Writing `mapper.py` for row processing
- **Code Upload**: Automatic code packaging and upload
- **Resource Configuration**: Setting memory, CPU, and job count

## Features

- Custom mapper script (`src/mapper.py`)
- Code upload to YT cluster
- Parallel execution (multiple jobs)
- Configuration access in mapper

## Running

```bash
python pipeline.py
```

In dev mode, runs locally. In prod mode, runs on YT cluster with parallel jobs.

## Files

- `pipeline.py`: Pipeline entry point
- `stages/run_map/stage.py`: Stage that runs map operation
- `stages/run_map/src/mapper.py`: Mapper script that processes rows
- `stages/run_map/config.yaml`: Map operation configuration
- `stages/create_input/stage.py`: Stage that creates input table
- `configs/config.yaml`: Pipeline configuration

## Key Concepts

- Mapper scripts read from stdin and write to stdout (JSON format)
- Each row is processed independently
- Multiple jobs run in parallel for better performance
- Configuration is accessible via `ytjobs.config.get_config_path()`

## Mapper Script Pattern

```python
import sys
import json
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

def main():
    config = OmegaConf.load(get_config_path())
    
    for line in sys.stdin:
        row = json.loads(line)
        # Process row
        output_row = process(row)
        print(json.dumps(output_row), flush=True)

if __name__ == "__main__":
    main()
```

## Next Steps

- See [05_vanilla_operation](../05_vanilla_operation/) for standalone jobs
- See [video_gpu](../video_gpu/) for GPU processing
