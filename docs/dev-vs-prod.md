# Dev vs prod modes

The pipeline `mode` field is either `dev` or `prod`. Same Python and YAML shape; execution differs (local files vs YT cluster).

## Overview

```{tip}
**Start in dev**

No `secrets.env` requirement, fast feedback, artifacts under `.dev/`.
```

- **Dev**: tables and many operations are simulated on disk; good for development and CI without a cluster.
- **Prod**: real YT operations and uploads; needs credentials and a compatible cluster image.

```{warning}
**Prod needs credentials**

Set `YT_PROXY` and `YT_TOKEN` in `configs/secrets.env` before switching to `prod`.
```

## Dev mode

### Behavior

- **Tables**: JSONL files under `.dev/`, keyed from logical YT-style paths.
- **Map / vanilla style jobs**: local subprocess plus a sandbox directory under `.dev/`.
- **Code upload**: skipped; Python runs from your working tree.
- **YQL**: translated through DuckDB where the dev client supports it (not identical to cluster YQL in every edge case).

### Config

```yaml
# configs/config.yaml
pipeline:
  mode: "dev"
```

### Layout after a run

```text
my_pipeline/
├── .dev/
│   ├── table1.jsonl
│   ├── table2.jsonl
│   └── operation.log
├── configs/
├── stages/
└── pipeline.py
```

### Tables

Write:

```python
# Writes .dev/data.jsonl for logical path //tmp/my_pipeline/data
self.deps.yt_client.write_table(
    table_path="//tmp/my_pipeline/data",
    rows=[{"id": 1, "name": "Alice"}],
)
```

Append without truncating (same keyword as prod when the target already exists):

```python
self.deps.yt_client.write_table(
    table_path="//tmp/my_pipeline/data",
    rows=[{"id": 2, "name": "Bob"}],
    append=True,
)
```

Read:

```python
rows = list(self.deps.yt_client.read_table("//tmp/my_pipeline/data"))
```

### Map (dev)

Typical flow:

1. Sandbox: `.dev/sandbox_<input>-><output>/` (exact name may vary by config).
2. Copy or link input JSONL into the sandbox.
3. Run the mapper entrypoint.
4. Mapper stdout becomes `.dev/<output>.jsonl`, or appends when the map op sets `append: true`.

See [Map operations — Append output](operations/map.md) (`append: true` section).

### Vanilla (dev)

1. Sandbox under `.dev/<stage>_sandbox/` (name depends on stage).
2. Extract the uploaded archive layout locally.
3. Run `vanilla.py` (or configured entry).
4. Stdout/stderr captured to `.dev/<stage>.log` (see operations docs for exact file names).

### YQL (dev)

Runs through the dev client’s DuckDB-backed path for supported statements. Treat results as representative, not a full YT SQL conformance suite.

### When dev mode is enough

- Writing stages and unit-style checks.
- Debugging mapper I/O with small fixtures.
- CI that should not depend on YT network.

### Tradeoffs (dev)

**Pros**

- Fast edit-run cycles.
- No cluster account required for basic flows.
- Easy to inspect `.jsonl` and logs on disk.
- Works offline for many pipelines.

**Cons**

- Dataset size bounded by your machine.
- Parallelism and timing differ from prod.
- Rare YT-only behavior may not appear until prod.

## Prod mode

### Behavior

- **Tables**: real Cypress paths on YT.
- **Operations**: cluster jobs with the resources you request.
- **Upload**: framework packages code to `build_folder` before starting jobs.
- **YQL**: cluster YQL engine.

```{warning}
**Image must match imports**

Job code imports `ytjobs` and your own modules. The Docker image for those jobs must ship matching Python deps. See [Cluster requirements](configuration/cluster-requirements.md).
```

### Config

```yaml
# configs/config.yaml
pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"
```

`configs/secrets.env`:

```bash
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token
```

### Tables (prod)

```python
self.deps.yt_client.write_table(
    table_path="//tmp/my_pipeline/data",
    rows=[{"id": 1, "name": "Alice"}],
)
```

```python
rows = list(self.deps.yt_client.read_table("//tmp/my_pipeline/data"))
```

### Map (prod)

1. Upload bundle to `build_folder`.
2. YT schedules tasks over input chunks.
3. Reducers (if any) follow map semantics you configured.
4. Output lands in the configured output table.

### Vanilla (prod)

Upload, single or few cluster tasks, logs in YT UI.

### YQL (prod)

Distributed engine, cluster-sized inputs.

### When you need prod

- Production schedules.
- Data larger than fits comfortably on a laptop disk.
- Real concurrency and YT-native features.

### Tradeoffs (prod)

**Pros**

- Scales with cluster storage and CPU.
- Matches how batch jobs actually run in YT.

**Cons**

- Needs credentials and network.
- Slower iteration than dev.
- Debugging means YT logs and UI, not only local files.

## Quick comparison

| Topic | Dev | Prod |
|-------|-----|------|
| Config snippet | `pipeline.mode: "dev"` | `pipeline.mode: "prod"` plus `build_folder` when uploading |
| Credentials | No YT `secrets.env` for basic flows | `YT_PROXY` / `YT_TOKEN` required |
| Throughput | One machine, subprocess-style map/vanilla | Cluster scheduling and distributed tables |
| Debugging | `.dev/*.jsonl`, local stderr | YT operation UI, remote stderr |

## Switching modes

Change one field:

```yaml
pipeline:
  mode: "dev"   # or "prod"
```

```{note}
**Same repo, different backend**

The framework picks dev vs prod implementations from `mode`; your stage classes stay the same.
```

Checklist when going to prod:

1. Logical table paths stay the same string format; dev maps them to files.
2. `secrets.env` exists and points at the right cluster.
3. `build_folder` is set and writable for your service user.
4. Docker image includes everything imported inside uploaded job code.

## Where behavior diverges

### Paths

- Dev: `//tmp/.../name` maps to `.dev/name.jsonl` (see client implementation for exact mapping rules).
- Prod: the same string is a Cypress path.

### Parallelism

- Dev map runs are closer to “one local subprocess story” than thousands of tiny tasks.
- Prod uses YT scheduling; race conditions that never show up locally can appear under load.

### Code freshness

- Dev reads your tree directly.
- Prod needs a successful upload each run; if you change only local files, rerun the pipeline to refresh the bundle.

### Errors

- Dev: tracebacks in your terminal.
- Prod: fetch stderr and system logs from YT for the failing operation.

## Debugging

### Dev

1. List `.dev/` after the stage runs.
2. Open the JSONL you think should have changed.
3. Read `operation.log` and stage logs next to it.
4. Print debugging is fine; you see it immediately.

### Prod

1. Open the operation in the YT UI and read stderr.
2. Use `self.logger` consistently; it ends up in the same places operations already aggregate logs.
3. For stubborn issues, reproduce with a tiny input table, then widen.

### Common symptoms

**Table missing in prod**

- Path typo or table never created in that Cypress tree.
- Credentials or proxy pointing at the wrong cluster.

**Code changes ignored in prod**

- You did not re-run the pipeline after editing sources, or upload failed silently earlier (check logs).

**Different results dev vs prod**

- DuckDB vs cluster SQL differences for YQL-heavy stages.
- Resource limits killing tasks only on the cluster.

## Workflow suggestion

```{tip}
**Smoke in prod early**

After dev passes, run prod once on a small slice of real schema before full-scale backfill jobs.
```

1. Implement in dev.
2. Promote to prod with a narrow date range or row limit.
3. Only then open the floodgates.

## Next steps

- [Cluster requirements](configuration/cluster-requirements.md)
- [Configuration](configuration/index.md)
- [Operations](operations/index.md)
- [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples/)
- [Troubleshooting: configuration](troubleshooting/configuration.md)
