# Multiple Operations

You can run multiple operations (map, vanilla, or both) in a single stage. This is useful for complex workflows that need to combine different operation types.

## Overview

Running multiple operations in one stage allows you to:

- Chain operations together
- Combine map and vanilla operations
- Process data in multiple steps
- Validate results between operations

**Key points:**

- Operations run sequentially
- Share the same stage context
- Use separate operation configs
- Results flow between operations

## Quick Start

### Map Then Vanilla

Run a map operation followed by a vanilla validation:

```python
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.map import run_map
from yt_framework.operations.vanilla import run_vanilla

class ProcessAndValidateStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Step 1: Process data with map operation
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.process,
        )
        if not success:
            raise RuntimeError("Process operation failed")
        
        # Step 2: Validate with vanilla operation
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.validate,
        )
        if not success:
            raise RuntimeError("Validate operation failed")
        
        return debug
```

**Configuration:**

```yaml
# stages/process_and_validate/config.yaml
client:
  operations:
    process:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/processed
      resources:
        pool: default
        memory_limit_gb: 4
        cpu_limit: 2
    validate:
      resources:
        pool: default
        memory_limit_gb: 2
        cpu_limit: 1
```

See [Example: 09_multiple_operations](../../examples/09_multiple_operations/) for complete example.

## Use Cases

### Process and Validate

Process data with map, then validate with vanilla:

```python
class ProcessAndValidateStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Process
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.process,
        )
        if not success:
            raise RuntimeError("Process failed")
        
        # Validate
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.validate,
        )
        if not success:
            raise RuntimeError("Validation failed")
        
        return debug
```

### Multiple Map Operations

Run multiple map operations in sequence:

```python
class MultiMapStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # First map: Transform data
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.transform,
        )
        if not success:
            raise RuntimeError("Transform failed")
        
        # Second map: Enrich data
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.enrich,
        )
        if not success:
            raise RuntimeError("Enrich failed")
        
        # Third map: Aggregate data
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.aggregate,
        )
        if not success:
            raise RuntimeError("Aggregate failed")
        
        return debug
```

### Setup, Process, Cleanup

Combine setup, processing, and cleanup:

```python
class FullPipelineStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Setup
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.setup,
        )
        if not success:
            raise RuntimeError("Setup failed")
        
        # Process
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.process,
        )
        if not success:
            raise RuntimeError("Process failed")
        
        # Cleanup
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.cleanup,
        )
        if not success:
            raise RuntimeError("Cleanup failed")
        
        return debug
```

## Configuration

### Multiple Operation Configs

Define multiple operations in stage config:

```yaml
# stages/multi_ops/config.yaml
client:
  operations:
    # First operation
    process:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/processed
      resources:
        pool: default
        memory_limit_gb: 4
        cpu_limit: 2
    
    # Second operation
    validate:
      resources:
        pool: default
        memory_limit_gb: 2
        cpu_limit: 1
    
    # Third operation
    aggregate:
      input_table: //tmp/my_pipeline/processed
      output_table: //tmp/my_pipeline/aggregated
      resources:
        pool: default
        memory_limit_gb: 8
        cpu_limit: 4
```

### Operation Naming

Use descriptive names for operations:

```yaml
client:
  operations:
    transform_data:      # Clear operation name
      input_table: ...
    validate_results:    # Clear operation name
      resources: ...
    aggregate_output:    # Clear operation name
      input_table: ...
```

## Complete Example

### Stage Code

```python
# stages/process_and_validate/stage.py
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.map import run_map
from yt_framework.operations.vanilla import run_vanilla
from yt_framework.utils.logging import log_header

class ProcessAndValidateStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Step 1: Process operation
        log_header(self.logger, "Process", "Running map operation")
        
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.process,
        )
        if not success:
            raise RuntimeError("Process operation failed")
        
        output_table = self.config.client.operations.process.output_table
        row_count = self.deps.yt_client.row_count(output_table)
        self.logger.info(f"Process operation completed: {row_count} rows processed")
        
        # Step 2: Validate operation
        log_header(self.logger, "Validate", "Running vanilla operation")
        
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.validate,
        )
        if not success:
            raise RuntimeError("Validate operation failed")
        
        self.logger.info("Validate operation completed")
        
        return debug
```

### Stage Configuration

```yaml
# stages/process_and_validate/config.yaml
job:
  multiplier: 2

client:
  operations:
    process:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/processed
      resources:
        pool: default
        memory_limit_gb: 4
        cpu_limit: 2
        job_count: 2
    
    validate:
      resources:
        pool: default
        memory_limit_gb: 2
        cpu_limit: 1
```

### Mapper Script

```python
# stages/process_and_validate/src/mapper.py
import sys
import json
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

def main():
    config = OmegaConf.load(get_config_path())
    multiplier = config.job.multiplier
    
    for line in sys.stdin:
        row = json.loads(line)
        output_row = {
            "id": row["id"],
            "value": row["value"] * multiplier,
        }
        print(json.dumps(output_row), flush=True)

if __name__ == "__main__":
    main()
```

### Vanilla Script

```python
# stages/process_and_validate/src/vanilla.py
import logging
from yt_framework.utils.env import load_secrets
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path
from omegaconf import OmegaConf

def main():
    logger = get_logger("validate", level=logging.INFO)
    config = OmegaConf.load(get_config_path())
    
    # Validate processed data
    # (In real scenario, read from table and validate)
    logger.info("Validation completed successfully")

if __name__ == "__main__":
    main()
```

## Best Practices

### Error Handling

**Check each operation:**

```python
success = run_map(...)
if not success:
    raise RuntimeError("Operation failed")
```

**Provide context:**

```python
try:
    success = run_map(...)
    if not success:
        raise RuntimeError("Map operation failed")
except Exception as e:
    self.logger.error(f"Error in map operation: {e}")
    raise
```

### Operation Ordering

**Order matters:**

- Operations run sequentially
- Later operations depend on earlier results
- Ensure data flow is correct

**Example:**

```python
# Correct order: transform -> aggregate
run_map(transform_config)  # First
run_map(aggregate_config)   # Second (uses transform output)
```

### Resource Management

**Allocate resources appropriately:**

```yaml
client:
  operations:
    process:
      resources:
        memory_limit_gb: 8  # More memory for processing
        cpu_limit: 4
    validate:
      resources:
        memory_limit_gb: 2  # Less memory for validation
        cpu_limit: 1
```

### Logging

**Log operation progress:**

```python
self.logger.info("Starting process operation...")
success = run_map(...)
self.logger.info("Process operation completed")

self.logger.info("Starting validate operation...")
success = run_vanilla(...)
self.logger.info("Validate operation completed")
```

## Common Patterns

### Pipeline Pattern

```python
class PipelineStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Setup
        run_vanilla(setup_config)
        
        # Process
        run_map(process_config)
        
        # Validate
        run_vanilla(validate_config)
        
        # Cleanup
        run_vanilla(cleanup_config)
        
        return debug
```

### Transform Chain Pattern

```python
class TransformChainStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Transform 1
        run_map(transform1_config)
        
        # Transform 2
        run_map(transform2_config)
        
        # Transform 3
        run_map(transform3_config)
        
        return debug
```

### Validation Pattern

```python
class ProcessWithValidationStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Process
        run_map(process_config)
        
        # Validate
        run_vanilla(validate_config)
        
        # If validation fails, operation raises exception
        return debug
```

## Troubleshooting

### Issue: Second operation fails

- Check first operation completed successfully
- Verify output table from first operation exists
- Check table paths are correct
- Review operation logs

### Issue: Operations run out of order

- Operations run sequentially in code order
- Check operation calls are in correct sequence
- Verify no parallel execution

### Issue: Resource conflicts

- Ensure sufficient resources for all operations
- Check pool availability
- Review resource allocation

## Next Steps

- Learn about [Map Operations](../operations/map.md)
- Explore [Vanilla Operations](../operations/vanilla.md)
- Check out [Example: 09_multiple_operations](../../examples/09_multiple_operations/) for complete example
