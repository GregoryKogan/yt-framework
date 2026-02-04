# Vanilla Operation Example

Demonstrates vanilla operations for standalone jobs that don't process table rows. Shows how to write vanilla scripts and configure vanilla operations.

## What It Demonstrates

- **Vanilla Operations**: Running standalone jobs without input/output tables
- **Vanilla Scripts**: Writing `vanilla.py` for standalone execution
- **Code Upload**: Automatic code packaging and upload
- **Resource Configuration**: Setting memory and CPU for vanilla jobs

## Features

- Custom vanilla script (`src/vanilla.py`)
- Standalone job execution
- Configuration access in vanilla script
- Logging utilities

## Running

```bash
python pipeline.py
```

Executes the vanilla operation as a standalone job.

## Files

- `pipeline.py`: Pipeline entry point
- `stages/run_vanilla/stage.py`: Stage that runs vanilla operation
- `stages/run_vanilla/src/vanilla.py`: Vanilla script that runs standalone
- `stages/run_vanilla/config.yaml`: Vanilla operation configuration
- `configs/config.yaml`: Pipeline configuration

## Key Concepts

- Vanilla operations run once (not per row)
- No stdin/stdout processing required
- Perfect for setup, cleanup, validation tasks
- Configuration is accessible via `ytjobs.config.get_config_path()`

## Vanilla Script Pattern

```python
#!/usr/bin/env python3
import logging
from omegaconf import OmegaConf
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path

def main():
    logger = get_logger("vanilla", level=logging.INFO)
    config = OmegaConf.load(get_config_path())
    
    logger.info("Vanilla operation started")
    # Do work...
    logger.info("Vanilla operation completed")

if __name__ == "__main__":
    main()
```

## Use Cases

- Setup tasks (create directories, initialize tables)
- Validation (check data quality, run tests)
- Cleanup (remove temporary files, archive data)
- Environment logging (log system information)

## Next Steps

- See [04_map_operation](../04_map_operation/) for row-by-row processing
- See [environment_log](../environment_log/) for comprehensive logging
- See [09_multiple_operations](../09_multiple_operations/) for combining operations
