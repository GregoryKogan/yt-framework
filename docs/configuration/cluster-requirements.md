# Cluster requirements (prod)

In **prod** mode, Python you import inside uploaded jobs (`ytjobs`, your own packages) runs **inside the cluster’s Docker image**, not on your laptop. If a package is missing there, the job fails at import time.

API details for `ytjobs`: [YT jobs (`ytjobs`)](../reference/ytjobs.md).

```{warning}
**Image must match imports**

Default cell image or your `docker_image` override must ship the same major Python and pip packages your mapper / vanilla / reducer code imports.
```

## What runs where

1. Driver process (your laptop or CI): `yt_framework`, OmegaConf, etc.—whatever you installed with `pip install -e ".[dev]"` or similar.
2. YT worker container: only what the image already contains plus files from the upload bundle (not a full `pip install` of your laptop env).

## Python

**3.11+** matches `requires-python` in the distribution. Older interpreters are unsupported.

## Packages used by `ytjobs`

### `ytsaurus-client` (>= 0.13.0)

Needed for `ytjobs.checkpoint` and other helpers that call `yt.wrapper`.

```bash
pip install "ytsaurus-client>=0.13.0"
```

### `boto3` / `botocore` (pinned in this repo)

The framework pins `boto3==1.35.99` / `botocore==1.35.99` because newer botocore changed connection-pooling defaults this codebase was validated against. Match the image to that pin unless you know you are deviating on purpose.

```bash
pip install boto3==1.35.99
```

### `omegaconf` (>= 2.3.0)

Not mandatory for import, but job samples load YAML through OmegaConf. Include it unless you parse YAML yourself.

```bash
pip install "omegaconf>=2.3.0"
```

## By submodule

| Module | Extra deps beyond stdlib |
|--------|---------------------------|
| `ytjobs.mapper` | stdlib for basics |
| `ytjobs.logging` | stdlib |
| `ytjobs.config` | stdlib (OmegaConf recommended) |
| `ytjobs.checkpoint` | `ytsaurus-client` |
| `ytjobs.s3` | `boto3` / `botocore` as above |

## Minimal Dockerfile sketch

```dockerfile
FROM python:3.11-slim

RUN pip install --no-cache-dir \
    "ytsaurus-client>=0.13.0" \
    boto3==1.35.99 \
    "omegaconf>=2.3.0"

WORKDIR /app
```

Point operations at it:

```yaml
client:
  operations:
    map:
      resources:
        docker_image: registry.example/team/yt-jobs:3.11-2026-01
        memory_limit_gb: 4
```

Full walkthrough: [Docker](../advanced/docker.md).

## Two ways to stay compatible

1. **Cell default image** already contains the stack above (coordinate with admins).
2. **Custom `docker_image`** per operation when you need extra system libs or GPU drivers.

## Smoke tests

**Python version** — vanilla job:

```python
import sys
print(sys.version)
```

**Imports** — vanilla job:

```python
try:
    import yt.wrapper as yt  # noqa: F401
    print("ytsaurus-client: import ok")
except ImportError as e:
    print("ytsaurus-client: MISSING", e)

try:
    import boto3
    print("boto3:", boto3.__version__)
except ImportError as e:
    print("boto3: MISSING", e)
```

Read stderr in the YT UI after running prod once.

## Related pages

- [Docker](../advanced/docker.md)
- [Dev vs prod](../dev-vs-prod.md)
- [Code upload](../advanced/code-upload.md)
- [Troubleshooting](../troubleshooting/index.md)
