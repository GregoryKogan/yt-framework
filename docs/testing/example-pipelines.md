# Example pipelines (verification)

The runnable trees under `examples/` are part of how we document the framework. Their layout and commands are listed in `examples/manifest.yaml`. Pytest checks that the manifest matches the tree on disk and runs selected pipelines as subprocess smoke tests.

## Manifest

Each top-level entry under `pipelines` describes one `examples/<slug>/` directory that contains `pipeline.py`.

| Field | Meaning |
|-------|---------|
| `slug` | Directory name under `examples/`. |
| `ci_tier` | `always` — run on every default pytest/CI invocation (dev mode in the shipped configs). `cluster_optional` — prod-oriented demos; tests live under `tests/integration/examples_cluster/` and follow the same collection rules as [Real cluster integration tests](yt-cluster-integration.md). `manual` — not part of default CI; run locally when you opt in (see below). |
| `requirements` | Tags for humans and for cluster gating logic (`yt_cluster`, `s3`, `docker_image`, `heavy_python`, etc.). |
| `commands` | Non-empty list of argv lists (after `python`). Usually `[["pipeline.py"]]`. See `examples/08_multiple_configs/README.md` for a tree that lists extra configs so each entry point is exercised. |

If someone adds `examples/<new>/pipeline.py` but forgets the manifest, pytest fails with a pointer to `examples/manifest.yaml`.

## Default CI (no YT cell)

From the repo root, with the usual dev install:

```bash
conda run -n yt-framework -- pytest tests/integration/example_pipelines/test_smoke.py -m examples --tb=short
```

The same tests are collected when you run the full suite with `pytest -m "not yt_cluster"` (GitHub Actions uses that filter). They execute `python pipeline.py` (and any extra argv from the manifest) under each example directory. Timeouts are per slug so map-heavy demos get more wall clock.

Tests prepend the repository root to `PYTHONPATH` for each subprocess so `import yt_framework` works even when the active interpreter is a minimal tool venv (for example the pre-commit hook environment). They also prepend the active interpreter’s `bin` directory to `PATH` so dev-mode job shells resolve `python3` to an environment that has `omegaconf` and the rest of the dev extra installed.

## Manual opt-in (`video_gpu`)

The GPU sample pulls in `torch` and related packages that we do not install on the default CI image. To run it locally:

```bash
export YT_FRAMEWORK_EXAMPLE_VIDEO_GPU=1
conda run -n yt-framework -- pytest tests/integration/example_pipelines/test_smoke.py::test_example_pipeline_manual_when_enabled -q
```

Without the env var, the test skips immediately.

## Real cluster (optional)

Prod-only examples (`06_s3_integration`, `07_custom_docker`) are covered in [Example pipelines on a real cell](yt-cluster-integration.md#example-pipelines-cluster).

## See also

- [Real cluster integration tests](yt-cluster-integration.md)
- [Secrets and env files](../configuration/secrets.md)
