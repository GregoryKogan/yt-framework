# Advanced topics

Pages here cover packaging, images, checkpoints, tokenizer tarballs, and running several operations inside one stage.

```{toctree}
:maxdepth: 1

code-upload
docker
checkpoints
tokenizer-artifact
multiple-operations
```

## Guides

| Page | What it covers |
|------|----------------|
| [Code upload](code-upload.md) | Tar layout, `upload_modules`, `upload_paths`, wrappers |
| [Docker](docker.md) | `docker_image`, registry auth, GPU images |
| [Checkpoints](checkpoints.md) | Single-file model upload + mount |
| [Tokenizer artifact](tokenizer-artifact.md) | Tarball upload + env vars for processor trees |
| [Multiple operations](multiple-operations.md) | Several `run_*` calls in one `run()` |

## Before you read

- [Pipelines and stages](../pipelines-and-stages.md)
- [Configuration](../configuration/index.md)
- [Operations](../operations/index.md)

## When these pages matter

| Topic | Typical trigger |
|-------|-----------------|
| Code upload | Any stage with `src/` in prod |
| Docker | Import errors for `torch`, CUDA, or system libs on workers |
| Checkpoints | ML inference map jobs needing a `.pt` / `.bin` |
| Tokenizer tarball | NLP models needing `tokenizer.json` plus vocab dirs |
| Multiple operations | One stage that maps then validates without splitting stages |

## See also

- [Operations index](../operations/index.md)
- [Troubleshooting](../troubleshooting/index.md)
