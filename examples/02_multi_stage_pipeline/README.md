# Multi-Stage Pipeline Example

Demonstrates a pipeline with multiple stages that pass data between them. Shows how to organize complex workflows into separate stages.

## What It Demonstrates

- **Multiple Stages**: Creating and running multiple stages in sequence
- **Stage Dependencies**: Stages that depend on previous stage outputs
- **Data Flow**: Passing data between stages via YT tables
- **Stage Ordering**: Controlling execution order via `enabled_stages`

## Features

- Three stages: `create_users`, `create_orders`, `join_data`
- Sequential execution with dependencies
- Table operations between stages
- YQL join operation

## Running

```bash
python pipeline.py
```

Stages execute in order:
1. `create_users` - Creates users table
2. `create_orders` - Creates orders table
3. `join_data` - Joins users and orders tables

## Files

- `pipeline.py`: Pipeline entry point
- `stages/create_users/`: Stage that creates users table
- `stages/create_orders/`: Stage that creates orders table
- `stages/join_data/`: Stage that joins tables using YQL
- `configs/config.yaml`: Pipeline configuration with stage ordering

## Key Concepts

- Stages execute in the order specified by `enabled_stages`
- Each stage can read tables created by previous stages
- Stages are independent but can depend on previous outputs

## Next Steps

- See [03_yql_operations](../03_yql_operations/) for more YQL operations
- See [04_map_operation](../04_map_operation/) for map operations
