# Map-reduce / reduce: command mode and `tar_command_bootstrap`

## TypedJob vs command strings

Production client (`client_prod`) treats each leg as either:

- **`yt.TypedJob`** — pickled spec, typed table I/O helpers.
- **Command string** — `JsonFormat` on stdin/stdout (e.g. `ytjobs.mapper.BatchMapper`).

Mapper and reducer **must use the same kind** (both TypedJob or both strings). Mixing them raises `ValueError` from `run_map_reduce`.

## Problem this solves

For **map**, the framework always ships a bootstrap command: extract `source.tar.gz`, then run `operation_wrapper_<stage>_map.sh`.

Historically, **map_reduce** / **reduce** only listed the tarball as a file dependency without that bootstrap, so **command-string** legs could run with the archive present but **not extracted**.

## Opt-in: `tar_command_bootstrap`

In `client.operations.map_reduce` or `client.operations.reduce`, set:

```yaml
tar_command_bootstrap: true
```

Behavior:

| Leg kind | Effect |
|----------|--------|
| Both TypedJob (map-reduce) | Unchanged: no bash bootstrap; YT handles TypedJob. |
| Both strings (map-reduce) | Each leg becomes `bash -c 'tar -xzf source.tar.gz && ./operation_wrapper_<stage>_map_reduce_mapper.sh'` (mapper) and the analogous `_reducer.sh` for the reducer. |
| TypedJob (reduce-only) | Flag ignored for packaging (TypedJob path unchanged). |
| String (reduce-only) | Reducer becomes `bash -c '… && ./operation_wrapper_<stage>_reduce.sh'`. |

Default is `false` so existing TypedJob pipelines need no config change.

## Wrappers in the tarball

`build_code_locally(..., create_wrappers=True)` (used before upload) generates:

- `operation_wrapper_<stage>_map_reduce_mapper.sh`
- `operation_wrapper_<stage>_map_reduce_reducer.sh`
- `operation_wrapper_<stage>_reduce.sh`

They mirror map wrappers: `PYTHONPATH`, `JOB_CONFIG_PATH`, optional `pip install -r requirements.txt`, then `python3 stages/<stage>/src/<script>.py`.

### Which Python files run

Optional entries in **`stages/<stage>/config.yaml`**:

```yaml
job:
  map_reduce_command:
    mapper_script: mapper.py        # default
    reducer_script: reducer_mds.py  # if not `reducer.py`
  reduce_command:
    reducer_script: reducer_index.py
```

If omitted, the uploader picks defaults (e.g. first existing among `reducer.py`, `reducer_mds.py`, … for map-reduce reducer; a similar order for reduce-only).

## Removability

- Logic lives mainly in `yt_framework/operations/tar_command_wiring.py`, `job_command.py`, and branches in `dependency_strategy.py` / `map_reduce.py`.
- Disable by omitting `tar_command_bootstrap` or setting it `false`.

## Dev client

`client_dev` does not execute map-reduce legs; it logs whether each leg would be TypedJob or command (`JsonFormat` in prod) for sanity checks.
