# Troubleshooting

Symptoms grouped by area. Start from the section that matches where the failure shows up (driver config vs job logs).

```{toctree}
:maxdepth: 1

pipeline
operations
configuration
debugging
```

## Sections

- [Pipeline](pipeline.md) — discovery, ordering, `enabled_stages`
- [Operations](operations.md) — map, vanilla, YQL, S3, checkpoints, Docker
- [Configuration](configuration.md) — dev vs prod, secrets, paths
- [Debugging](debugging.md) — logging, reproducing, asking for help

## How failures cluster

1. **YAML / OmegaConf** — missing keys, wrong types, bad table paths
2. **Filesystem** — missing `stage.py`, wrong working directory
3. **Operations** — mapper stderr, YQL pragma limits, S3 403
4. **Environment** — Python version, missing pip package in **cluster** image
5. **Mode mismatch** — code that only works locally, or vice versa

## Fast checks

| Symptom | First look |
|---------|------------|
| Pipeline exits before stages | `configs/config.yaml`, `python pipeline.py` cwd |
| Stage raises immediately | Stage `config.yaml`, imports in `stage.py` |
| Cluster job fails | Task stderr in YT UI, Docker image packages |
| Works in dev, fails in prod | Credentials, `build_folder`, upload size |

## If you are stuck

1. Reproduce in dev with the smallest table or stdin fixture.
2. Capture **full** stderr from the failing task (prod) or `.dev` logs (dev).
3. Compare cluster image package list against imports in uploaded code.

## See also

- [Configuration](../configuration/index.md)
- [Dev vs prod](../dev-vs-prod.md)
- [Operations](../operations/index.md)
