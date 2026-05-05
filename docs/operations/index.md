# Operations

Stages call helpers (`run_map`, `run_vanilla`, YT client methods, etc.) declared under `client.operations` in YAML. This section documents each pattern.

```{toctree}
:maxdepth: 1

map
map-reduce-typed-jobs
command-mode-map-reduce
vanilla
yql
s3
table
sort
```

## Guides

| Topic | Link |
|-------|------|
| Map | [map.md](map.md) |
| Map-reduce (TypedJob) | [map-reduce-typed-jobs.md](map-reduce-typed-jobs.md) |
| Map-reduce (command mode) | [command-mode-map-reduce.md](command-mode-map-reduce.md) |
| Vanilla | [vanilla.md](vanilla.md) |
| YQL | [yql.md](yql.md) |
| S3 | [s3.md](s3.md) |
| Table helpers | [table.md](table.md) |
| Sort | [sort.md](sort.md) |

## Picking a tool

| Pattern | Input / output | Parallelism |
|---------|----------------|-------------|
| Map | Table → table | YT splits input across tasks |
| Map-reduce / reduce | Sorted or grouped table work | Map + reduce phases |
| Vanilla | None required | Single job |
| YQL | One or more tables → table | Query planner |
| S3 | Object store → table (typical) | Driver listing + cluster for follow-up |
| Table helpers | Driver-side Python | None on cluster |
| Sort | Table → sorted table | YT sort operation |

### Map vs YQL

- Custom Python per row → map.
- Declarative SQL shape → YQL.

### Vanilla vs map

- No table contract → vanilla.
- Row stream → map.

### S3 plus tables

S3 stages often feed map or YQL; compose them as separate stages or multiple operations in one stage ([Multiple operations](../advanced/multiple-operations.md)).

## Example stage lists

Extract / transform / load:

```yaml
stages:
  enabled_stages:
    - extract_from_s3
    - transform_data
    - load_to_table
```

Setup / process / validate:

```yaml
stages:
  enabled_stages:
    - setup_environment
    - process_data
    - validate_results
```

## See also

- [Command-mode map-reduce](command-mode-map-reduce.md)
- [Multiple operations](../advanced/multiple-operations.md)
