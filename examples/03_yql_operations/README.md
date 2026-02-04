# YQL Operations Example

Comprehensive example demonstrating all YQL (YTsaurus Query Language) operations available in the framework.

## What It Demonstrates

- **All YQL Operations**: Join, filter, select, aggregate, union, distinct, sort, limit
- **Dry Run**: Previewing queries before execution
- **Table Operations**: High-level table manipulation operations
- **Query Building**: Using YQL builder for complex queries

## Features

- 8 different YQL operations
- Dry run support for query preview
- Table operations without custom code
- Efficient distributed execution

## Running

```bash
python pipeline.py
```

Executes all YQL operations in sequence, demonstrating each operation type.

## Operations Demonstrated

1. **Join Tables**: Left join on `user_id`
2. **Filter Table**: Filter orders with `amount > 100`
3. **Select Columns**: Select specific columns
4. **Group By Aggregate**: Aggregate orders by user
5. **Union Tables**: Combine multiple tables
6. **Distinct**: Get distinct cities
7. **Sort Table**: Sort by amount (descending)
8. **Limit Table**: Get top 3 orders

## Files

- `pipeline.py`: Pipeline entry point
- `stages/yql_examples/stage.py`: Stage with all YQL operations
- `stages/yql_examples/config.yaml`: Configuration with table paths
- `configs/config.yaml`: Pipeline configuration

## Key Concepts

- YQL operations are high-level and don't require custom code
- Dry run mode allows previewing queries
- Operations execute efficiently on YT cluster

## Next Steps

- See [04_map_operation](../04_map_operation/) for row-by-row processing
- See [05_vanilla_operation](../05_vanilla_operation/) for standalone jobs
