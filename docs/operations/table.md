# Table helpers

The module `yt_framework.operations.table` provides small **orchestration-side** helpers around the YT client:

- `get_row_count` — log and return row count.
- `read_table` — load all rows into a `list` of dicts (use only when the table fits in memory).
- `download_table` — export a table to a local **JSONL** file (dev/prod via the client).

## When to use helpers vs `yt_client`

| Approach | Use when |
|----------|----------|
| `from yt_framework.operations.table import read_table, get_row_count, download_table` | You want consistent logging and a single import in stage code. |
| `self.deps.yt_client.read_table(...)`, `row_count`, etc. | You need streaming, partial reads, or lower-level control (as in many examples). |

Both are valid; examples in [Pipelines and Stages](../pipelines-and-stages.md) and [S3 operations](s3.md) often use `yt_client` directly.

## Example

```python
from yt_framework.operations.table import get_row_count, read_table

n = get_row_count(self.deps.yt_client, "//tmp/pipeline/data", self.logger)
rows = read_table(self.deps.yt_client, "//tmp/pipeline/data", self.logger)
```

## API

Autodoc: **Table operations** in [API Reference](../reference/api.md) (`yt_framework.operations.table`).

## See also

- [YQL operations](yql.md) for SQL-style table processing
- [Map operations](map.md) for per-row jobs
