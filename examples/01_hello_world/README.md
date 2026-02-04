# Hello World Example

The simplest example of a YT Framework pipeline. Demonstrates basic pipeline setup, stage creation, and table operations.

## What It Demonstrates

- **DefaultPipeline**: Using automatic stage discovery
- **Basic Stage**: Creating a simple stage that writes and reads a table
- **YT Client Usage**: Using YT client for table operations
- **Dev Mode**: Running pipeline in development mode

## Features

- Automatic stage discovery (no manual registration)
- Table creation and reading
- Basic YT client operations
- Simple configuration

## Running

```bash
python pipeline.py
```

In dev mode, the table will be created as `.dev/hello_world_table.jsonl`.

## Files

- `pipeline.py`: Pipeline entry point using DefaultPipeline
- `stages/create_table/stage.py`: Stage that creates a table with sample data
- `stages/create_table/config.yaml`: Stage configuration
- `configs/config.yaml`: Pipeline configuration

## Next Steps

- See [02_multi_stage_pipeline](../02_multi_stage_pipeline/) for multiple stages
- See [03_yql_operations](../03_yql_operations/) for YQL operations
