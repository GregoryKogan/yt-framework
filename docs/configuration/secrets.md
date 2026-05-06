# Secrets

Put credentials in `configs/secrets.env` (key=value, one per line). Non-secret runtime knobs are listed in [Environment variables](../reference/environment-variables.md).

```bash
# configs/secrets.env
YT_PROXY=https://your-proxy.example
YT_TOKEN=your-token

# Optional: S3
S3_ENDPOINT=https://your-s3-endpoint.example
S3_DOWNLOAD_ACCESS_KEY=...
S3_DOWNLOAD_SECRET_KEY=...
S3_UPLOAD_ACCESS_KEY=...
S3_UPLOAD_SECRET_KEY=...
```

## YT

Prod mode needs a reachable proxy and token:

```bash
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token
```

Get values from whoever runs your YT cell.

## S3

Reads typically use the download pair; writes use the upload pair unless one credential set has both roles.

## How secrets reach your code

The pipeline loader reads `secrets.env` early. For values you must pass explicitly into helpers (for example constructing `S3Client`), call `load_secrets` on the configs directory:

```python
from yt_framework.utils.env import load_secrets

def run(self, debug: DebugContext) -> DebugContext:
    secrets = load_secrets(self.deps.configs_dir)
    _proxy = secrets.get("YT_PROXY")
    return debug
```

### Common variables

| Variable | When you need it |
|----------|------------------|
| `YT_PROXY`, `YT_TOKEN` | Prod driver talking to YT |
| `S3_*` | S3-backed operations |
| `DOCKER_AUTH_USERNAME`, `DOCKER_AUTH_PASSWORD` | Private registry pulls for `docker_image` |

## Hygiene

```{warning}
**Do not commit `secrets.env`**
```

- Add `configs/secrets.env` to `.gitignore`.
- Commit a `secrets.example.env` with dummy values for onboarding.
- Rotate tokens on the schedule your security team expects.
- In CI, inject the same keys via the environment; the loader also reads process env when the file is absent.

For optional **pytest** runs against a real cell (separate from pipeline `secrets.env`), see [Real cluster integration tests](../testing/yt-cluster-integration.md).

Example ignore rules:

```text
configs/secrets.env
*.env
!*example.env
```

Example template:

```bash
# configs/secrets.example.env
YT_PROXY=https://proxy.example
YT_TOKEN=replace-me
# S3 optional...
```

## CI

```bash
export YT_PROXY="https://..."
export YT_TOKEN="..."
python pipeline.py
```

If `secrets.env` is missing, variables already present in the process environment still work.

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| File ignored / not found | Path is `configs/` next to `pipeline.py`, filename `secrets.env` unless you customized loading |
| Auth errors | Typos, expired token, wrong cluster proxy |
| HTTP 403 from S3 | Endpoint URL, bucket policy, wrong access/secret pair for the operation |

## See also

- [Configuration index](index.md)
- [Dev vs prod](../dev-vs-prod.md)
- [Troubleshooting: configuration](../troubleshooting/configuration.md)
