# Multiple Operations Example

Demonstrates running multiple operations (map and vanilla) in a single stage. Shows how to chain operations together for complex workflows.

## What It Demonstrates

- **Multiple Operations**: Running multiple operations in one stage
- **Operation Chaining**: Chaining map and vanilla operations
- **Sequential Execution**: Operations execute in sequence
- **Operation Configuration**: Configuring multiple operations

## Features

- Map operation followed by vanilla operation
- Shared stage context
- Separate operation configs
- Sequential execution flow

## Running

```bash
python pipeline.py
```

Executes map operation, then vanilla operation in sequence.

## Files

- `pipeline.py`: Pipeline entry point
- `stages/process_and_validate/stage.py`: Stage with multiple operations
- `stages/process_and_validate/src/mapper.py`: Mapper script for processing
- `stages/process_and_validate/src/vanilla.py`: Vanilla script for validation
- `stages/process_and_validate/config.yaml`: Configuration for both operations
- `stages/create_input/stage.py`: Stage that creates input table
- `configs/config.yaml`: Pipeline configuration

## Key Concepts

- Operations run sequentially in code order
- Each operation has its own config section
- Operations share the same stage context
- Results from one operation can be used by the next

## Operation Flow

1. **Create Input**: Creates input table
2. **Process (Map)**: Processes data row-by-row
3. **Validate (Vanilla)**: Validates processed data

## Configuration

```yaml
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

## Next Steps

- See [Multiple Operations Guide](../../docs/advanced/multiple-operations.md) for detailed documentation
- See [04_map_operation](../04_map_operation/) for map operations
- See [05_vanilla_operation](../05_vanilla_operation/) for vanilla operations
