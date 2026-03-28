# Environment variables reference

Variables used by **dev simulation**, **pipeline drivers**, or **job sandboxes** (map/vanilla/TypedJob workers). Secrets such as `YT_TOKEN` and `S3_*` are documented in [Secrets](../configuration/secrets.md).

## Dev mode (`YTDevClient`)

| Variable | When set | Purpose |
|----------|----------|---------|
| `YT_PIPELINE_DIR` | Optional | If `pipeline_dir` is not passed to the client, this path is used as the pipeline root; otherwise falls back to `cwd` (with a warning). |

## Job sandbox (workers)

Set by **wrappers** or **TypedJob bootstrap** (`StageBootstrapTypedJob`, command-mode scripts), not by you in typical pipeline configs.

| Variable | Set by | Purpose |
|----------|--------|---------|
| `JOB_CONFIG_PATH` | Wrapper / `StageBootstrapTypedJob` | Absolute path to `stages/<stage>/config.yaml` inside the extracted archive. Used by `ytjobs.config.get_config_path`. |
| `YT_STAGE_NAME` | Production job wiring | Stage name; used by `StageBootstrapTypedJob` to locate `stages/<name>/src` and config. |
| `TOKENIZER_ARTIFACT_FILE` | Dependency packaging | Basename or relative path to the tokenizer **tarball** inside the sandbox (e.g. under archive root). |
| `TOKENIZER_ARTIFACT_DIR` | Bootstrap (optional) | Directory (relative to sandbox root) where the tarball is extracted; derived from `TOKENIZER_ARTIFACT_NAME` if omitted. |
| `TOKENIZER_ARTIFACT_NAME` | Bootstrap | Logical name for default extract dir `tokenizer_artifacts/<name>`. |

See [Tokenizer artifacts](../advanced/tokenizer-artifact.md) for how tarball paths and config relate.

## Checkpoint / model files in jobs

| Variable | Typical source | Purpose |
|----------|----------------|---------|
| `CHECKPOINT_FILE` | Framework checkpoint mounting | Path to mounted model checkpoint in the operation sandbox (see [Checkpoint management](../advanced/checkpoints.md)). |

## Related docs

- [YT jobs library (`ytjobs`)](ytjobs.md) — `JOB_CONFIG_PATH`, job-side helpers
- [Code upload](../advanced/code-upload.md) — archive layout and wrappers
- [Map-reduce TypedJob](../operations/map-reduce-typed-jobs.md) — `StageBootstrapTypedJob` behavior
