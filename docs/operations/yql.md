# YQL Operations

YQL (YTsaurus Query Language) operations provide a high-level interface for table manipulation operations like joins, filters, aggregations, and more. These operations are executed efficiently on the YT cluster.

## Overview

YQL operations use YT's distributed query engine to perform table operations. They're perfect for:

- Joining multiple tables
- Filtering and selecting data
- Aggregations and grouping
- Union and distinct operations
- Sorting and limiting

**Key characteristics:**

- High-level table operations
- Efficient distributed execution
- No custom code required
- Dry run support for query preview

## Available Operations

### Join Tables

Join two tables on a common column.

```python
self.deps.yt_client.join_tables(
    left_table="//tmp/my_pipeline/orders",
    right_table="//tmp/my_pipeline/users",
    output_table="//tmp/my_pipeline/joined",
    on="user_id",
    how="left",  # or "inner", "right", "full"
    select_columns=[
        "a.order_id",
        "a.user_id",
        "a.amount",
        "b.name",
        "b.email",
    ],
)
```

**Parameters:**

- **`left_table`**: Left table path
- **`right_table`**: Right table path
- **`output_table`**: Output table path
- **`on`**: Column name(s) to join on:
  - `str`: Same column name on both sides (e.g., `"user_id"`)
  - `List[str]`: Multiple columns with same names (e.g., `["user_id", "region"]`)
  - `Dict[str, str]`: Different column names (e.g., `{"left": "input_s3_path", "right": "path"}`)
- **`how`**: Join type (`"left"`, `"inner"`, `"right"`, `"full"`)
- **`select_columns`**: Columns to select (prefix with `a.` or `b.` for table alias)

### Filter Table

Filter rows based on a condition.

```python
self.deps.yt_client.filter_table(
    input_table="//tmp/my_pipeline/orders",
    output_table="//tmp/my_pipeline/filtered",
    condition="amount > 100",
)
```

**Parameters:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`condition`**: SQL-like condition (e.g., `"amount > 100"`, `"status == 'active'"`)

### Select Columns

Select specific columns from a table.

```python
self.deps.yt_client.select_columns(
    input_table="//tmp/my_pipeline/users",
    output_table="//tmp/my_pipeline/selected",
    columns=["user_id", "name", "email"],
)
```

**Parameters:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`columns`**: List of column names to select

### Group By Aggregate

Group rows and compute aggregations.

```python
self.deps.yt_client.group_by_aggregate(
    input_table="//tmp/my_pipeline/orders",
    output_table="//tmp/my_pipeline/aggregated",
    group_by="user_id",
    aggregations={
        "order_count": "count",
        "total_amount": "sum",
        "avg_amount": "avg",
        "max_amount": "max",
        "min_amount": "min",
    },
)
```

**Parameters:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`group_by`**: Column name(s) to group by
- **`aggregations`**: Dictionary mapping output column names to aggregation functions

**Aggregation functions:**

- `"count"`: Count rows
- `"sum"`: Sum values
- `"avg"`: Average values
- `"max"`: Maximum value
- `"min"`: Minimum value

### Union Tables

Combine multiple tables into one.

```python
self.deps.yt_client.union_tables(
    tables=[
        "//tmp/my_pipeline/orders_2023",
        "//tmp/my_pipeline/orders_2024",
    ],
    output_table="//tmp/my_pipeline/all_orders",
)
```

**Parameters:**

- **`tables`**: List of table paths to union
- **`output_table`**: Output table path

**Note:** All tables must have the same schema.

### Distinct

Get distinct values from columns.

```python
self.deps.yt_client.distinct(
    input_table="//tmp/my_pipeline/users",
    output_table="//tmp/my_pipeline/distinct_cities",
    columns=["city"],
)
```

**Parameters:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`columns`**: List of column names for distinct operation

### Sort Table

Sort table by one or more columns.

```python
self.deps.yt_client.sort_table(
    input_table="//tmp/my_pipeline/orders",
    output_table="//tmp/my_pipeline/sorted",
    order_by="amount",
    ascending=False,  # True for ascending, False for descending
)
```

**Parameters:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`order_by`**: Column name(s) to sort by
- **`ascending`**: Sort order (default: `True`)

### Limit Table

Limit the number of rows in a table.

```python
self.deps.yt_client.limit_table(
    input_table="//tmp/my_pipeline/orders",
    output_table="//tmp/my_pipeline/limited",
    limit=100,
)
```

**Parameters:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`limit`**: Maximum number of rows to include

## Dry Run

All YQL operations support dry run mode to preview queries before execution:

```python
# Preview query without executing
query = self.deps.yt_client.join_tables(
    left_table="//tmp/my_pipeline/orders",
    right_table="//tmp/my_pipeline/users",
    output_table="//tmp/my_pipeline/joined",
    on="user_id",
    how="left",
    select_columns=["a.order_id", "b.name"],
    dry_run=True,  # Enable dry run
)

self.logger.info(f"Query preview:\n{query}")

# Execute actual query
self.deps.yt_client.join_tables(
    left_table="//tmp/my_pipeline/orders",
    right_table="//tmp/my_pipeline/users",
    output_table="//tmp/my_pipeline/joined",
    on="user_id",
    how="left",
    select_columns=["a.order_id", "b.name"],
    dry_run=False,  # Execute query
)
```

Dry run returns the YQL query string without executing it.

## Complete Example

```python
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.utils.logging import log_header

class YqlExamplesStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # 1. Join tables
        log_header(self.logger, "YQL", "1. JOIN TABLES")
        self.deps.yt_client.join_tables(
            left_table=self.config.client.orders_table,
            right_table=self.config.client.users_table,
            output_table=self.config.client.output.joined,
            on="user_id",
            how="left",
            select_columns=[
                "a.order_id",
                "a.user_id",
                "a.amount",
                "b.name",
                "b.email",
            ],
        )
        
        # 2. Filter table
        log_header(self.logger, "YQL", "2. FILTER TABLE")
        self.deps.yt_client.filter_table(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.filtered,
            condition="amount > 100",
        )
        
        # 3. Select columns
        log_header(self.logger, "YQL", "3. SELECT COLUMNS")
        self.deps.yt_client.select_columns(
            input_table=self.config.client.users_table,
            output_table=self.config.client.output.selected,
            columns=["user_id", "name"],
        )
        
        # 4. Group by aggregate
        log_header(self.logger, "YQL", "4. GROUP BY AGGREGATE")
        self.deps.yt_client.group_by_aggregate(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.aggregated,
            group_by="user_id",
            aggregations={
                "order_count": "count",
                "total_amount": "sum",
            },
        )
        
        # 5. Union tables
        log_header(self.logger, "YQL", "5. UNION TABLES")
        self.deps.yt_client.union_tables(
            tables=[
                self.config.client.orders_table,
                self.config.client.archive_orders_table,
            ],
            output_table=self.config.client.output.united,
        )
        
        # 6. Distinct
        log_header(self.logger, "YQL", "6. DISTINCT")
        self.deps.yt_client.distinct(
            input_table=self.config.client.users_table,
            output_table=self.config.client.output.distinct,
            columns=["city"],
        )
        
        # 7. Sort table
        log_header(self.logger, "YQL", "7. SORT TABLE")
        self.deps.yt_client.sort_table(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.sorted,
            order_by="amount",
            ascending=False,
        )
        
        # 8. Limit table
        log_header(self.logger, "YQL", "8. LIMIT TABLE")
        self.deps.yt_client.limit_table(
            input_table=self.config.client.output.sorted,
            output_table=self.config.client.output.limited,
            limit=10,
        )
        
        return debug
```

See [Example: 03_yql_operations](../../examples/03_yql_operations/) for a complete example with all operations.

## Configuration

YQL operations don't require special configuration in stage config files. They use the YT client directly:

```yaml
# stages/yql_examples/config.yaml
client:
  orders_table: //tmp/my_pipeline/orders
  users_table: //tmp/my_pipeline/users
  archive_orders_table: //tmp/my_pipeline/orders_archive
  output:
    joined: //tmp/my_pipeline/joined
    filtered: //tmp/my_pipeline/filtered
    selected: //tmp/my_pipeline/selected
    aggregated: //tmp/my_pipeline/aggregated
    united: //tmp/my_pipeline/united
    distinct: //tmp/my_pipeline/distinct
    sorted: //tmp/my_pipeline/sorted
    limited: //tmp/my_pipeline/limited
```

## Dev Mode Behavior

In dev mode, YQL operations are simulated using DuckDB:

- Operations run locally
- Results written to `.dev/` directory
- Full YQL syntax supported
- Performance differs from production

**Note:** Some advanced YQL features may behave differently in dev mode.

## Best Practices

1. **Use dry run**: Preview queries before execution
2. **Check schemas**: Ensure table schemas match for joins/unions
3. **Optimize joins**: Use appropriate join types
4. **Filter early**: Apply filters before expensive operations
5. **Limit results**: Use limit for large result sets
6. **Test locally**: Use dev mode for testing

## Common Patterns

### Multi-Table Join

```python
# Join multiple tables sequentially
joined1 = self.deps.yt_client.join_tables(
    left_table="table1",
    right_table="table2",
    output_table="temp_joined",
    on="id",
)

joined2 = self.deps.yt_client.join_tables(
    left_table="temp_joined",
    right_table="table3",
    output_table="final_joined",
    on="id",
)
```

### Filtered Aggregation

```python
# Filter then aggregate
filtered = self.deps.yt_client.filter_table(
    input_table="orders",
    output_table="temp_filtered",
    condition="amount > 100",
)

aggregated = self.deps.yt_client.group_by_aggregate(
    input_table="temp_filtered",
    output_table="result",
    group_by="user_id",
    aggregations={"total": "sum"},
)
```

### Top N Results

```python
# Sort then limit
sorted_table = self.deps.yt_client.sort_table(
    input_table="orders",
    output_table="temp_sorted",
    order_by="amount",
    ascending=False,
)

top_n = self.deps.yt_client.limit_table(
    input_table="temp_sorted",
    output_table="top_orders",
    limit=10,
)
```

## Troubleshooting

### Issue: Join fails

- Check column names match
- Verify table schemas are compatible
- Check table paths exist

### Issue: Filter condition syntax error

- Use proper SQL-like syntax
- Escape special characters
- Check column names exist

### Issue: Aggregation fails

- Verify column types are numeric (for sum/avg)
- Check column names exist
- Ensure group_by columns exist

### Issue: Union fails

- Verify all tables have same schema
- Check column order matches
- Ensure table paths exist

## Next Steps

- Learn about [Map Operations](map.md) for row-by-row processing
- Explore [S3 Operations](s3.md) for file integration
- Check out [Examples](../../examples/) for more patterns
