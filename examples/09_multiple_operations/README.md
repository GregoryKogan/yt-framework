# Map then vanilla in one stage

`process_and_validate` calls `run_map` then `run_vanilla` inside one `run()` method. Both operations read their own subtree under `client.operations`.

## Run

```bash
python pipeline.py
```

Flow: seed input table → map → vanilla validation script.

## Config pattern

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

## See also

- [Multiple operations](../../docs/advanced/multiple-operations.md)
- [04_map_operation](../04_map_operation/), [05_vanilla_operation](../05_vanilla_operation/)
