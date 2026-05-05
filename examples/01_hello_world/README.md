# Hello world

Minimal `DefaultPipeline` repo: one stage writes rows with the YT client, then reads them back. Runs in **dev** mode by default (`.dev/hello_world_table.jsonl`).

## What to notice

- Stage discovery from `stages/create_table/`
- `write_table` / `read_table` on `self.deps.yt_client`
- `configs/config.yaml` lists `enabled_stages`

## Run

```bash
python pipeline.py
```

## Layout

| Path | Role |
|------|------|
| `pipeline.py` | `DefaultPipeline.main()` |
| `stages/create_table/stage.py` | Sample table write/read |
| `stages/create_table/config.yaml` | Table path |
| `configs/config.yaml` | `mode: dev`, enabled stages |

## Next

- [02_multi_stage_pipeline](../02_multi_stage_pipeline/)
- [03_yql_operations](../03_yql_operations/)
