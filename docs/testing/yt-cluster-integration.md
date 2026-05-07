# Real cluster integration tests

These tests call a live YTsaurus cell through `YTProdClient` and `yt.wrapper`. They live under `tests/integration/yt_cluster/` and use the pytest marker `yt_cluster`.

## When pytest collects them

pytest loads that directory only if both `YT_PROXY` and `YT_TOKEN` are set after this lookup:

1. `YT_FRAMEWORK_CLUSTER_TEST_ENV` — path to a `KEY=value` file (same format as `configs/secrets.env`).
2. Otherwise, if `yt-cluster-test.env` exists at the **repository root**, that file is read.
3. Values from the process environment override the file for the keys listed in the example file.

If the pair is missing, the whole package is skipped at collection time. On CI hosts, `CI=true` also forces that skip. **GitHub Actions** additionally runs `pytest -m "not yt_cluster"`, so those tests never run in CI even if credentials were present by mistake.

## Setup

Copy the template and fill in real values (the real file is gitignored):

```bash
cp yt-cluster-test.example.env yt-cluster-test.env
```

Never commit `yt-cluster-test.env`. You can instead export `YT_PROXY` and `YT_TOKEN` in the shell or in CI secrets.

## Run

From the repo root, using the project Conda env:

```bash
conda run -n yt-framework -- pytest -m yt_cluster -xvs
```

Narrower:

```bash
conda run -n yt-framework -- pytest tests/integration/yt_cluster/test_yql.py -xvs
```

## What the tests assume

- **Cypress namespace**: `//tmp/yt-framework/testing/<session_id>/…` for tables and files. The session fixture creates that node and deletes it recursively after the run.
- **Host scratch**: `/tmp/yt-framework/testing/<session_id>/…` for local files used in `upload_file` / `upload_directory`. It is removed when the session ends.
- **Docker**: map, map-reduce, reduce, and vanilla jobs do **not** pass `docker_image` in `OperationResources`, so the cell’s **default** job image is used. The scripts call `python3` and trivial shell (`true`). Secure-env command wrapping also relies on `python3` and `bash`. If a job fails with “command not found”, your default image may differ; adjust the command in the test to match the image, not the framework.
- **YQL helpers** on the client call `run_yql` with pool name `default` inside the library. Map/sort operations use `YT_TEST_POOL` (default `default`) when the test passes `op_resources.pool`.
- **Optional knobs** in the env file: `YT_TEST_POOL`, `YT_TEST_POOL_TREE`, `YT_TEST_MEMORY_GB`, `YT_TEST_CPU_LIMIT`, `YT_TEST_JOB_COUNT`.
- **Map extras** (`test_map_operations.py`): mapper `env` propagation, a second `run_map` with `append=True`, the `job=` alias (no `command`), two Cypress file dependencies, forwarded options such as `title`, `max_row_weight`, and `max_failed_jobs`, and a check that sensitive env keys are **not** present in the operation’s plain `mapper.environment` while jobs still read them at runtime (via `secure_vault` + promotion).

## See also

- [Secrets and env files](../configuration/secrets.md)
- [Cluster requirements](../configuration/cluster-requirements.md)
