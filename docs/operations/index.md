# Operations Overview

YT Framework supports several types of operations for processing data on YTsaurus clusters.

## Operation Types

```{card-grid} 2
:padding: 2

**Map Operations**
^^^
Process each row of a table independently. Perfect for row-by-row transformations, data enrichment, and parallel processing.

+++
[Learn More →](map.md)

**Vanilla Operations**
^^^
Run standalone jobs without input/output tables. Perfect for setup, cleanup, validation, or any task that doesn't require table I/O.

+++
[Learn More →](vanilla.md)

**YQL Operations**
^^^
Perform table operations using YQL (YTsaurus Query Language). Includes joins, filters, aggregations, sorting, and more.

+++
[Learn More →](yql.md)

**S3 Operations**
^^^
Integrate with S3 for file listing, downloading, and processing. Perfect for working with external data sources.

+++
[Learn More →](s3.md)
```

## When to Use Each Operation

| Operation | Best For | Input/Output | Parallelization |
|-----------|----------|--------------|-----------------|
| **Map** | Row-by-row processing, transformations | Table → Table | Automatic (per row) |
| **Vanilla** | Setup, cleanup, standalone tasks | None | Single job |
| **YQL** | SQL-like queries, joins, aggregations | Table(s) → Table | Automatic (query-level) |
| **S3** | External data integration | S3 → Table | File-level |

## Quick Comparison

### Map vs YQL

- **Use Map** when you need custom Python logic per row
- **Use YQL** when you need SQL-like operations (joins, aggregations)

### Vanilla vs Map

- **Use Vanilla** when you don't need table input/output
- **Use Map** when processing table rows

### S3 Integration

- **Use S3** when working with external data sources
- Often combined with Map or YQL operations

## Common Patterns

### Pattern 1: Extract → Transform → Load

```yaml
stages:
  enabled_stages:
    - extract_from_s3      # S3 operation
    - transform_data       # Map operation
    - load_to_table        # YQL operation
```

### Pattern 2: Setup → Process → Validate

```yaml
stages:
  enabled_stages:
    - setup_environment    # Vanilla operation
    - process_data         # Map operation
    - validate_results     # Vanilla operation
```

### Pattern 3: Join → Filter → Aggregate

```yaml
stages:
  enabled_stages:
    - join_tables         # YQL operation
    - filter_data         # YQL operation
    - aggregate_results   # YQL operation
```

## See Also

- [Map Operations](map.md) - Detailed map operation guide
- [Vanilla Operations](vanilla.md) - Detailed vanilla operation guide
- [YQL Operations](yql.md) - Detailed YQL operation guide
- [S3 Operations](s3.md) - Detailed S3 operation guide
- [Multiple Operations](../advanced/multiple-operations.md) - Running multiple operations in one stage
