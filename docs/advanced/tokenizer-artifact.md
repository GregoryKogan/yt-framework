# Tokenizer artifacts (tarball uploads)

**Tokenizer artifacts** are separate from [checkpoint management](checkpoints.md) **model files**. They package a **directory or `.tar.gz`** (tokenizers, small processor bundles, etc.), upload it to a Cypress **file** path, and mount it into map / map-reduce / vanilla jobs. The framework wires **file dependencies** and **environment variables** so `StageBootstrapTypedJob` (or command-mode wrappers) can extract the archive in the sandbox.

## When to use

- Reducers or mappers need a **tarball** of assets that are **not** a single PyTorch/HF checkpoint file.
- You want idempotent upload (skip if the YT path already exists) and the same pool/resource model as other operations.

## Configuration (`tokenizer_artifact`)

Under `client.operations.<operation>.tokenizer_artifact` (map, map_reduce, reduce, vanilla—where enabled):

| Key | Required | Description |
|-----|----------|-------------|
| `artifact_base` | Yes (to enable) | Cypress **directory** for uploaded tarballs (e.g. `//tmp/pipeline/tokenizers`). |
| `artifact_name` | No* | Logical name; defaults from `job.tokenizer_name`, `job.model_name` basename, or `local_artifact_path` filename. |
| `local_artifact_path` | No | Local **directory** (packed to temp `.tar.gz`) or existing **`.tar.gz`** to upload if missing in YT. |

\*If `artifact_base` is set, a resolvable `artifact_name` is required (directly or via `job` / local path).

Example (map):

```yaml
client:
  operations:
    map:
      input_table: //tmp/pipeline/in
      output_table: //tmp/pipeline/out
      tokenizer_artifact:
        artifact_base: //tmp/pipeline/tokenizer_artifacts
        local_artifact_path: /path/to/my_tokenizer_bundle   # dir or .tar.gz
      resources:
        pool: default
```

## Sandbox behavior

1. The uploaded file is named `<artifact_name>.tar.gz` under `artifact_base`.
2. Workers receive `TOKENIZER_ARTIFACT_FILE`, `TOKENIZER_ARTIFACT_DIR`, and optionally `TOKENIZER_ARTIFACT_NAME`.
3. `StageBootstrapTypedJob` extracts the tarball once per sandbox (see [Environment variables](../reference/environment-variables.md)).

## Relation to checkpoints

- **Checkpoints** (`checkpoint:` block): usually a **single model file** mounted as `CHECKPOINT_FILE`.
- **Tokenizer artifacts**: **tarball** workflow, separate Cypress path and env vars. You can use both in one stage if needed.

## API

- `init_tokenizer_artifact_directory` and helpers: **Tokenizer artifact** in [API Reference](../reference/api.md) (`yt_framework.operations.tokenizer_artifact`).
- Exported on `yt_framework.operations` for advanced callers.

## See also

- [Map-reduce TypedJob](../operations/map-reduce-typed-jobs.md)
- [Cluster requirements](../configuration/cluster-requirements.md)
