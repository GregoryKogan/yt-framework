# YQL examples

One stage exercises the high-level YQL helpers on `self.deps.yt_client` (join, filter, select, aggregate, union, distinct, sort, limit). Includes dry-run style previews where the stage wires them.

## Run

```bash
python pipeline.py
```

## Operations in `stages/yql_examples/stage.py`

1. Join users/orders  
2. Filter high-value orders  
3. Column projection  
4. Group-by aggregate  
5. Union  
6. Distinct cities  
7. Sort  
8. Limit top rows  

`max_row_weight` defaults to `128M` (override per call if needed, still capped at `128M`).

## Files

- `stages/yql_examples/stage.py` — all calls
- `stages/yql_examples/config.yaml` — table paths
- `configs/config.yaml` — pipeline mode

## Next

- [04_map_operation](../04_map_operation/)
- [05_vanilla_operation](../05_vanilla_operation/)
