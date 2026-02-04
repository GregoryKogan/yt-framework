# Vanilla Operations

Vanilla operations run standalone jobs without input/output tables. They're perfect for setup tasks, cleanup, validation, or any work that doesn't process table data row-by-row.

## Overview

A **vanilla operation** executes a standalone script on the YT cluster (or locally in dev mode). Unlike map operations, vanilla operations:

- Don't process table rows
- Run once per operation (not per row)
- Don't require input/output tables
- Perfect for setup, cleanup, validation tasks

**Key characteristics:**

- Standalone execution
- Single job (no parallelism)
- No table I/O required
- Custom code execution (vanilla.py)

## Quick Start

### Minimal Example

**Stage** (`stages/setup/stage.py`):

```python
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.vanilla import run_vanilla

class SetupStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.vanilla,
        )
        
        if not success:
            raise RuntimeError("Vanilla operation failed")
        
        return debug
```

**Stage config** (`stages/setup/config.yaml`):

```yaml
client:
  operations:
    vanilla:
      resources:
        pool: default
        memory_limit_gb: 2
        cpu_limit: 1
```

**Vanilla script** (`stages/setup/src/vanilla.py`):

```python
#!/usr/bin/env python3
import logging
from omegaconf import OmegaConf
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path

def main():
    logger = get_logger("vanilla", level=logging.INFO)
    
    # Load configuration
    config = OmegaConf.load(get_config_path())
    
    logger.info("Vanilla operation started")
    
    # Do some work
    greeting = config.job.get("greeting", "Hello!")
    logger.info(f"Greeting: {greeting}")
    
    # Simulate work
    for i in range(5):
        logger.info(f"Iteration {i+1}")
    
    logger.info("Vanilla operation completed")

if __name__ == "__main__":
    main()
```

See [Example: 05_vanilla_operation](../../examples/05_vanilla_operation/) for a complete example.

## Vanilla Script

The vanilla script (`src/vanilla.py`) is executed once per operation.

### Script Structure

```python
#!/usr/bin/env python3
import logging
from omegaconf import OmegaConf
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path

def main():
    # Initialize logger
    logger = get_logger("vanilla", level=logging.INFO)
    
    # Load configuration
    config = OmegaConf.load(get_config_path())
    
    # Your logic here
    logger.info("Starting work...")
    
    # Do work
    perform_task(config)
    
    logger.info("Work completed")

if __name__ == "__main__":
    main()
```

### Configuration Access

Access stage configuration:

```python
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

config = OmegaConf.load(get_config_path())

# Access job config
greeting = config.job.greeting
iterations = config.job.iterations

# Access client config (read-only)
memory = config.client.operations.vanilla.resources.memory_limit_gb
```

**Stage config** (`stages/my_stage/config.yaml`):

```yaml
job:
  greeting: "Hello from YT!"
  iterations: 10

client:
  operations:
    vanilla:
      resources:
        memory_limit_gb: 4
        cpu_limit: 2
```

### Logging

Use YT logging utilities:

```python
import logging
from ytjobs.logging.logger import get_logger

logger = get_logger("vanilla", level=logging.INFO)

logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
logger.debug("Debug message")
```

Logs appear in YT operation logs (prod mode) or `.dev/<stage_name>.log` (dev mode).

### Error Handling

Handle errors and exit with appropriate codes:

```python
import sys
import logging
from ytjobs.logging.logger import get_logger

logger = get_logger("vanilla", level=logging.INFO)

try:
    # Your logic
    perform_task()
    logger.info("Task completed successfully")
except Exception as e:
    logger.error(f"Task failed: {e}", exc_info=True)
    sys.exit(1)  # Exit with error code
```

**Important:** Non-zero exit codes will cause the operation to fail.

## Configuration

### Basic Configuration

```yaml
client:
  operations:
    vanilla:
      resources:
        pool: default
        memory_limit_gb: 2
        cpu_limit: 1
```

### Required Fields

- **`resources`**: Resource configuration

### Resource Configuration

```yaml
resources:
  pool: default              # YT pool name
  pool_tree: null            # Optional: pool tree
  memory_limit_gb: 2         # Memory (GB)
  cpu_limit: 1               # CPU cores
  gpu_limit: 0               # GPU count (default: 0)
  user_slots: null           # Optional: user slots limit
```

**Resource guidelines:**

- **Memory**: Allocate based on task needs
- **CPU**: More CPUs = faster execution
- **GPU**: Set to 1+ for GPU workloads (requires custom Docker)

### Advanced Configuration

**Max failed jobs:**

```yaml
client:
  operations:
    vanilla:
      max_failed_job_count: 1  # Fail operation after N failed jobs
      resources:
        # ...
```

**Custom Docker:**

```yaml
client:
  operations:
    vanilla:
      resources:
        docker_image: my-registry/my-image:latest
        # ...
```

See [Docker Guide](../advanced/docker.md) for details.

**Checkpoints:**

```yaml
client:
  operations:
    vanilla:
      checkpoint:
        checkpoint_base: //tmp/my_pipeline/checkpoints
        local_checkpoint_path: /path/to/local/model.pth
      resources:
        # ...
```

See [Checkpoints Guide](../advanced/checkpoints.md) for details.

## Running Vanilla Operations

### From Stage

Use `run_vanilla()` function:

```python
from yt_framework.operations.vanilla import run_vanilla

success = run_vanilla(
    context=self.context,
    operation_config=self.config.client.operations.vanilla,
)
```

### Operation Flow

1. **Code upload**: Code is packaged and uploaded to YT (prod mode)
2. **Job creation**: YT creates a single job
3. **Execution**: Job runs vanilla.py script
4. **Completion**: Operation completes when job finishes

### Dev Mode Behavior

In dev mode:

- Runs locally using subprocess
- Creates sandbox directory: `.dev/<stage_name>_sandbox/`
- Extracts code archive
- Executes vanilla.py script
- Logs output to `.dev/<stage_name>.log`

## Use Cases

### Setup Tasks

Initialize directories, create tables, prepare data:

```python
def main():
    logger = get_logger("setup", level=logging.INFO)
    config = OmegaConf.load(get_config_path())
    
    # Create directories
    create_directories(config)
    
    # Initialize tables
    initialize_tables(config)
    
    logger.info("Setup completed")
```

### Validation

Validate data, check conditions, run tests:

```python
def main():
    logger = get_logger("validate", level=logging.INFO)
    config = OmegaConf.load(get_config_path())
    
    # Validate data
    if not validate_data(config):
        logger.error("Validation failed")
        sys.exit(1)
    
    logger.info("Validation passed")
```

### Cleanup

Clean up temporary files, remove old data:

```python
def main():
    logger = get_logger("cleanup", level=logging.INFO)
    config = OmegaConf.load(get_config_path())
    
    # Clean up temporary files
    cleanup_temp_files(config)
    
    logger.info("Cleanup completed")
```

### Environment Logging

Log environment information for debugging:

```python
def main():
    logger = get_logger("logenv", level=logging.INFO)
    
    # Log environment
    log_system_info(logger)
    log_python_environment(logger)
    log_gpu_info(logger)
    
    logger.info("Environment logged")
```

See [Example: environment_log](../../examples/environment_log/) for comprehensive environment logging.

## Advanced Topics

### Multiple Vanilla Operations

Run multiple vanilla operations in one stage:

```python
class SetupAndValidateStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Setup operation
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.setup,
        )
        if not success:
            raise RuntimeError("Setup failed")
        
        # Validate operation
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.validate,
        )
        if not success:
            raise RuntimeError("Validate failed")
        
        return debug
```

### Combining with Map Operations

Run vanilla operations before or after map operations:

```python
class ProcessAndValidateStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Map operation
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.process,
        )
        if not success:
            raise RuntimeError("Process failed")
        
        # Vanilla validation
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.validate,
        )
        if not success:
            raise RuntimeError("Validate failed")
        
        return debug
```

See [Example: 09_multiple_operations](../../examples/09_multiple_operations/) for details.

### GPU Workloads

For GPU workloads:

1. **Custom Docker**: Create Docker image with GPU support
2. **GPU resources**: Set `gpu_limit: 1` or higher
3. **GPU code**: Use GPU libraries in vanilla script

**Example:**

```yaml
client:
  operations:
    vanilla:
      resources:
        docker_image: my-registry/gpu-image:latest
        gpu_limit: 1
        memory_limit_gb: 16
```

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

1. **Single responsibility**: Each vanilla operation should do one thing
2. **Error handling**: Handle errors and exit with appropriate codes
3. **Logging**: Log important operations for debugging
4. **Resource allocation**: Allocate resources based on actual needs
5. **Idempotency**: Operations should be safe to rerun
6. **Testing**: Test vanilla scripts locally before running on cluster

## Common Patterns

### Simple Task

```python
def main():
    logger = get_logger("task", level=logging.INFO)
    config = OmegaConf.load(get_config_path())
    
    logger.info("Starting task")
    perform_task(config)
    logger.info("Task completed")
```

### Task with Iterations

```python
def main():
    logger = get_logger("task", level=logging.INFO)
    config = OmegaConf.load(get_config_path())
    
    iterations = config.job.iterations
    for i in range(iterations):
        logger.info(f"Iteration {i+1}/{iterations}")
        do_work()
```

### Task with Validation

```python
def main():
    logger = get_logger("task", level=logging.INFO)
    config = OmegaConf.load(get_config_path())
    
    # Validate prerequisites
    if not check_prerequisites():
        logger.error("Prerequisites not met")
        sys.exit(1)
    
    # Perform task
    perform_task()
    logger.info("Task completed")
```

## Troubleshooting

### Issue: Operation fails immediately

- Check vanilla script syntax
- Verify script has `if __name__ == "__main__": main()` block
- Check resource limits

### Issue: Script doesn't execute

- Verify `src/vanilla.py` exists
- Check file permissions
- Review operation logs

### Issue: Out of memory

- Increase `memory_limit_gb`
- Check memory usage in script
- Optimize code

### Issue: Script hangs

- Check for infinite loops
- Verify external dependencies are available
- Review resource limits

## Next Steps

- Learn about [Map Operations](map.md)
- Explore [Advanced Topics](../advanced/) (Docker, checkpoints)
- Check out [Examples](../../examples/) for more patterns
