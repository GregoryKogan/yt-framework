# YQL operations

YQL helpers on `self.deps.yt_client` run YTsaurus SQL against Cypress tables. In **prod** that is cluster YQL; in **dev** many helpers go through DuckDB-backed simulation (behavior may differ for edge cases).

## Overview

```{tip}
**When YQL fits**

Use YQL helpers for joins, filters, aggregates, unions, and similar set operations. Use map when you need arbitrary Python per row or libraries YQL cannot call.
```

Typical uses:

- Join or reshape tables without shipping `mapper.py`.
- Let YT choose execution plans for large inputs.
- Preview SQL with dry-run APIs where supported.

**Defaults**

- `PRAGMA yt.MaxRowWeight` is injected at `128M` unless you override (max `128M`).

```{note}
**YQL vs map**

YQL expresses set logic declaratively. Map runs your Python on each row stream.
```

## Request API

Each helper is a `*_request` method on the YT client. It takes a single frozen dataclass from `yt_framework.yt.clients.yql.yql_requests` (for example `JoinTablesRequest`). The same types are what `yt_framework.yt.clients.yql.yql_builder` uses to build SQL strings.

For filter, union, sort, and limit, you can leave `columns` unset on the request; the client fills them from the input table schema when needed.

## Row Weight Defaults

All YQL helpers and raw `run_yql(...)` execution include:

```sql
PRAGMA yt.MaxRowWeight = "128M";
```

by default.

Override per call when needed (must not exceed `128M`; larger values raise `ValueError`):

```python
from yt_framework.yt.clients.yql.yql_requests import JoinTablesRequest

self.deps.yt_client.join_tables_request(
    JoinTablesRequest(
        left_table="//tmp/a",
        right_table="//tmp/b",
        output_table="//tmp/out",
        on="id",
        max_row_weight="64M",
    ),
)
```

For raw queries:

```python
self.deps.yt_client.run_yql(
    "INSERT INTO `//tmp/out` WITH TRUNCATE SELECT * FROM `//tmp/in`;",
    max_row_weight="64M",
)
```

If the SQL already contains `PRAGMA yt.MaxRowWeight`, that value is checked too; overrides and embedded pragmas cannot exceed `128M`.

## Available Operations

### Join Tables

Join two tables on a common column.

```python
from yt_framework.yt.clients.yql.yql_requests import JoinTablesRequest

self.deps.yt_client.join_tables_request(
    JoinTablesRequest(
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
    ),
)
```

**`JoinTablesRequest` fields:**

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
from yt_framework.yt.clients.yql.yql_requests import FilterTableRequest

self.deps.yt_client.filter_table_request(
    FilterTableRequest(
        input_table="//tmp/my_pipeline/orders",
        output_table="//tmp/my_pipeline/filtered",
        condition="amount > 100",
    ),
)
```

**`FilterTableRequest` fields:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`condition`**: SQL-like condition (e.g., `"amount > 100"`, `"status == 'active'"`)
- **`columns`**: Optional; when omitted, the client resolves columns from the table

### Select Columns

Select specific columns from a table.

```python
from yt_framework.yt.clients.yql.yql_requests import SelectColumnsRequest

self.deps.yt_client.select_columns_request(
    SelectColumnsRequest(
        input_table="//tmp/my_pipeline/users",
        output_table="//tmp/my_pipeline/selected",
        columns=["user_id", "name", "email"],
    ),
)
```

**`SelectColumnsRequest` fields:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`columns`**: List of column names to select

### Group By Aggregate

Group rows and compute aggregations.

```python
from yt_framework.yt.clients.yql.yql_requests import GroupByAggregateRequest

self.deps.yt_client.group_by_aggregate_request(
    GroupByAggregateRequest(
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
    ),
)
```

**`GroupByAggregateRequest` fields:**

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
from yt_framework.yt.clients.yql.yql_requests import UnionTablesRequest

self.deps.yt_client.union_tables_request(
    UnionTablesRequest(
        tables=(
            "//tmp/my_pipeline/orders_2023",
            "//tmp/my_pipeline/orders_2024",
        ),
        output_table="//tmp/my_pipeline/all_orders",
    ),
)
```

**`UnionTablesRequest` fields:**

- **`tables`**: Tuple of table paths to union
- **`output_table`**: Output table path
- **`columns`**: Optional; when omitted, the client uses the first table's columns

**Note:** All tables must have the same schema.

### Distinct

Get distinct values from columns.

```python
from yt_framework.yt.clients.yql.yql_requests import DistinctRequest

self.deps.yt_client.distinct_request(
    DistinctRequest(
        input_table="//tmp/my_pipeline/users",
        output_table="//tmp/my_pipeline/distinct_cities",
        columns=["city"],
    ),
)
```

**`DistinctRequest` fields:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`columns`**: List of column names for distinct operation (optional; omit for `SELECT DISTINCT *`)

### Sort Table

Sort table by one or more columns.

```python
from yt_framework.yt.clients.yql.yql_requests import SortTableRequest

self.deps.yt_client.sort_table_request(
    SortTableRequest(
        input_table="//tmp/my_pipeline/orders",
        output_table="//tmp/my_pipeline/sorted",
        order_by="amount",
        ascending=False,  # True for ascending, False for descending
    ),
)
```

**`SortTableRequest` fields:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`order_by`**: Column name(s) to sort by
- **`ascending`**: Sort order (default: `True`)
- **`columns`**: Optional; when omitted, the client resolves columns from the input table

### Limit Table

Limit the number of rows in a table.

```python
from yt_framework.yt.clients.yql.yql_requests import LimitTableRequest

self.deps.yt_client.limit_table_request(
    LimitTableRequest(
        input_table="//tmp/my_pipeline/orders",
        output_table="//tmp/my_pipeline/limited",
        limit=100,
    ),
)
```

**`LimitTableRequest` fields:**

- **`input_table`**: Input table path
- **`output_table`**: Output table path
- **`limit`**: Maximum number of rows to include
- **`columns`**: Optional; when omitted, the client resolves columns from the input table

## Dry Run

All YQL operations support dry run mode to preview queries before execution:

```python
from yt_framework.yt.clients.yql.yql_requests import JoinTablesRequest

# Preview query without executing
query = self.deps.yt_client.join_tables_request(
    JoinTablesRequest(
        left_table="//tmp/my_pipeline/orders",
        right_table="//tmp/my_pipeline/users",
        output_table="//tmp/my_pipeline/joined",
        on="user_id",
        how="left",
        select_columns=["a.order_id", "b.name"],
        dry_run=True,
    ),
)

self.logger.info(f"Query preview:\n{query}")

# Execute actual query
self.deps.yt_client.join_tables_request(
    JoinTablesRequest(
        left_table="//tmp/my_pipeline/orders",
        right_table="//tmp/my_pipeline/users",
        output_table="//tmp/my_pipeline/joined",
        on="user_id",
        how="left",
        select_columns=["a.order_id", "b.name"],
        dry_run=False,
    ),
)
```

Dry run returns the YQL query string without executing it.

## Complete Example

The live example under `examples/03_yql_operations` imports request types and calls `*_request` methods from the stage. See that tree for the full listing.

See [Example: 03_yql_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/03_yql_operations/) for a complete example with all operations.

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
from yt_framework.yt.clients.yql.yql_requests import JoinTablesRequest

joined1 = self.deps.yt_client.join_tables_request(
    JoinTablesRequest(
        left_table="table1",
        right_table="table2",
        output_table="temp_joined",
        on="id",
    ),
)

joined2 = self.deps.yt_client.join_tables_request(
    JoinTablesRequest(
        left_table="temp_joined",
        right_table="table3",
        output_table="final_joined",
        on="id",
    ),
)
```

### Filtered Aggregation

```python
from yt_framework.yt.clients.yql.yql_requests import (
    FilterTableRequest,
    GroupByAggregateRequest,
)

filtered = self.deps.yt_client.filter_table_request(
    FilterTableRequest(
        input_table="orders",
        output_table="temp_filtered",
        condition="amount > 100",
    ),
)

aggregated = self.deps.yt_client.group_by_aggregate_request(
    GroupByAggregateRequest(
        input_table="temp_filtered",
        output_table="result",
        group_by="user_id",
        aggregations={"total": "sum"},
    ),
)
```

### Top N Results

```python
from yt_framework.yt.clients.yql.yql_requests import LimitTableRequest, SortTableRequest

sorted_table = self.deps.yt_client.sort_table_request(
    SortTableRequest(
        input_table="orders",
        output_table="temp_sorted",
        order_by="amount",
        ascending=False,
    ),
)

top_n = self.deps.yt_client.limit_table_request(
    LimitTableRequest(
        input_table="temp_sorted",
        output_table="top_orders",
        limit=10,
    ),
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
- Check out [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples/) for more patterns
