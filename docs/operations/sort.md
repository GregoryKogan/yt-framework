# Sort operations

A **sort operation** runs a YTsaurus **sort** on an existing table (in-place), using pool and resource hints from YAML—same pattern as `run_map` / `run_map_reduce`.

## When to use

- You need a sorted table before reduce, merge, or downstream YQL.
- You prefer an explicit sort stage instead of embedding sort logic elsewhere.

## Configuration

Under `client.operations.sort` in the stage `config.yaml`:

| Key | Required | Description |
|-----|----------|-------------|
| `input_table` | Yes | YT path of the table to sort. |
| `sort_by` | Yes | List of column names to sort by (YT sort order). |
| `resources.pool` / `resources.pool_tree` | No | Scheduler pool (same as map / map-reduce). |
| `resources.memory_limit_gb`, `resources.cpu_limit` | No | Resource hints. |

## Stage code

```python
from omegaconf import OmegaConf
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.sort import run_sort


class SortStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        sort_cfg = OmegaConf.merge(
            self.config.client.operations.sort,
            {"input_table": "//tmp/my_pipeline/unsorted"},
        )
        if not run_sort(context=self.context, operation_config=sort_cfg):
            raise RuntimeError("Sort failed")
        return debug
```

## API

See the **Sort operations** section in [API Reference](../reference/api.md) for `run_sort` parameters and errors.

## See also

- [Map-reduce (TypedJob)](map-reduce-typed-jobs.md)
- [Operations overview](index.md)
