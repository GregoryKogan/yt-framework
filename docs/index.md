# YT Framework documentation

This documentation explains how to build and run data processing pipelines on YTsaurus (YT) with the YT Framework Python package.

## Table of contents

```{toctree}
:maxdepth: 3
:titlesonly:

configuration/index
testing/yt-cluster-integration
operations/index
advanced/index
reference/api
reference/ytjobs
reference/environment-variables
troubleshooting/index
```

## Introduction

YT Framework is a Python library for defining pipelines as ordered stages, running them against a YT cluster in production, or against the local filesystem in development.

You get:

- Pipelines built from stages under `stages/`, with YAML configuration per stage and for the pipeline.
- A dev mode that mimics table and job behavior locally (no cluster required for basic work).
- Operations such as map, vanilla, YQL (via the YT client), S3 helpers, and related utilities.
- Packaging and upload of job code when you run on the cluster.

### When it helps

- Less wiring for stage discovery and config than rolling everything by hand.
- One codebase: flip `pipeline.mode` between dev and prod instead of maintaining two runners.
- YQL and table helpers exposed on the same client you use for reads and writes.

## Installation

### Prerequisites

- Python 3.11 or newer
- For **prod** mode: network access to YT, valid credentials, and a cluster whose images match your job dependencies (see [Cluster requirements](configuration/cluster-requirements.md))

#### YT cluster requirements

```{warning}
**Cluster Docker image**

In prod mode, code from `ytjobs` runs inside jobs on the cluster. The default or custom Docker image for those jobs must include the Python packages your mappers, reducers, and vanilla scripts import.
```

Details: [Cluster requirements](configuration/cluster-requirements.md).

### Install from PyPI

```bash
pip install yt-framework
```

### Install from source

```bash
pip install -e .
```

### Verify installation

```bash
python -c "import yt_framework; print(yt_framework.__version__)"
```

The PyPI distribution is named `yt-framework`. Import paths are `yt_framework` (driver) and `ytjobs` (job-side helpers).

### Credentials for prod

```{warning}
**Secrets for production**

Prod mode expects YT (and optionally S3) credentials in `configs/secrets.env`. Without them, the client cannot talk to the cluster.
```

Create `configs/secrets.env` in your pipeline repo:

```bash
# configs/secrets.env
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token
```

For S3-backed operations, add the keys your stage uses (names vary by operation; see [Secrets](configuration/secrets.md)):

```bash
S3_ENDPOINT=https://your-s3-endpoint.com
S3_DOWNLOAD_ACCESS_KEY=your-download-access-key
S3_DOWNLOAD_SECRET_KEY=your-download-secret-key
S3_UPLOAD_ACCESS_KEY=your-upload-access-key
S3_UPLOAD_SECRET_KEY=your-upload-secret-key
```

More detail: [Secrets management](configuration/secrets.md).

## Quick start

Minimal pipeline: one stage that writes a small table.

### Step 1: Layout

```bash
mkdir my_first_pipeline
cd my_first_pipeline
mkdir -p stages/create_data configs
```

### Step 2: Entry point

`pipeline.py` at the repo root:

```python
from yt_framework.core.pipeline import DefaultPipeline

if __name__ == "__main__":
    DefaultPipeline.main()
```

### Step 3: Pipeline config

`configs/config.yaml`:

```yaml
stages:
  enabled_stages:
    - create_data

pipeline:
  mode: "dev"  # use "prod" on the cluster
```

### Step 4: Stage

`stages/create_data/stage.py`:

```python
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage

class CreateDataStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Creating data table...")

        rows = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300},
        ]

        self.deps.yt_client.write_table(
            table_path=self.config.client.output_table,
            rows=rows,
        )

        self.logger.info("Created table with %s rows", len(rows))
        return debug
```

`stages/create_data/config.yaml`:

```yaml
client:
  output_table: //tmp/my_first_pipeline/data
```

### Step 5: Run

```bash
python pipeline.py
```

In **dev** mode, rows land under something like `my_first_pipeline/.dev/data.jsonl`. In **prod** mode, the same logical path is a YT table at `//tmp/my_first_pipeline/data`.

### Where to go next

- [Pipelines and stages](pipelines-and-stages.md)
- [Configuration](configuration/index.md)
- [Dev vs prod](dev-vs-prod.md)
- [Examples on GitHub](https://github.com/GregoryKogan/yt-framework/tree/main/examples/)

## Core concepts

### Pipelines and stages

A **pipeline** runs **stages** in order. Each stage is a class with a `run` method.

- `DefaultPipeline`: discovers `BaseStage` subclasses under `stages/`.
- `BasePipeline`: you register stages yourself.
- `BaseStage`: base class for stage implementations.

More: [Pipelines and stages](pipelines-and-stages.md).

### Dev vs prod

```{tip}
**Start in dev**

Use dev mode first: no cluster credentials, fast feedback, files under `.dev/`.
```

- **Dev**: tables as `.jsonl` under `.dev/`, local subprocesses for map/vanilla-style work, YQL backed by DuckDB where applicable.
- **Prod**: real YT operations, code upload to `build_folder`, jobs on the cluster.

[Dev vs prod](dev-vs-prod.md) has a full comparison.

### Configuration

- `configs/config.yaml`: pipeline mode, enabled stages, shared options.
- `stages/<name>/config.yaml`: settings for that stage.
- `configs/secrets.env`: credentials (not committed).

[Configuration](configuration/index.md).

## Operations

### Map

Row-wise transforms with uploaded mapper code. [Map operations](operations/map.md) — example [04_map_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/04_map_operation/).

### Vanilla

Jobs without mandatory input/output tables (setup, maintenance, one-off scripts). [Vanilla](operations/vanilla.md) — example [05_vanilla_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/05_vanilla_operation/).

### YQL

Table operations through YQL via the YT client (joins, filters, aggregates, etc.). [YQL](operations/yql.md) — example [03_yql_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/03_yql_operations/).

### S3

List, download, and related patterns against S3-compatible storage. [S3 operations](operations/s3.md) — example [06_s3_integration](https://github.com/GregoryKogan/yt-framework/tree/main/examples/06_s3_integration/).

## Advanced topics

- [Code upload](advanced/code-upload.md) — how job bundles are built and sent to YT.
- [Docker](advanced/docker.md) — custom images for GPU or extra system deps — example [07_custom_docker](https://github.com/GregoryKogan/yt-framework/tree/main/examples/07_custom_docker/).
- [Checkpoints](advanced/checkpoints.md) — model artifacts for inference-style stages.
- [Multiple operations](advanced/multiple-operations.md) — more than one operation in a stage — example [09_multiple_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/09_multiple_operations/).

## Reference

- [API reference](reference/api.md) — `yt_framework` (autodoc from docstrings)
- [YT jobs (`ytjobs`)](reference/ytjobs.md) — mapper helpers, S3, logging, job config path, Cypress checkpoints
- [Environment variables](reference/environment-variables.md)
- [Troubleshooting](troubleshooting/index.md)

## Examples

Under [`examples/`](https://github.com/GregoryKogan/yt-framework/tree/main/examples/) on GitHub:

| Example | What it shows |
|---------|----------------|
| [01_hello_world](https://github.com/GregoryKogan/yt-framework/tree/main/examples/01_hello_world/) | Minimal pipeline |
| [02_multi_stage_pipeline](https://github.com/GregoryKogan/yt-framework/tree/main/examples/02_multi_stage_pipeline/) | Several stages and context |
| [03_yql_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/03_yql_operations/) | YQL |
| [04_map_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/04_map_operation/) | Map |
| [05_vanilla_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/05_vanilla_operation/) | Vanilla |
| [06_s3_integration](https://github.com/GregoryKogan/yt-framework/tree/main/examples/06_s3_integration/) | S3 |
| [07_custom_docker](https://github.com/GregoryKogan/yt-framework/tree/main/examples/07_custom_docker/) | Custom Docker image |
| [08_multiple_configs](https://github.com/GregoryKogan/yt-framework/tree/main/examples/08_multiple_configs/) | Multiple config files |
| [09_multiple_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/09_multiple_operations/) | Multiple operations in one stage |
| [10_custom_upload](https://github.com/GregoryKogan/yt-framework/tree/main/examples/10_custom_upload/) | Custom upload layout |
| [environment_log](https://github.com/GregoryKogan/yt-framework/tree/main/examples/environment_log/) | Environment logging |
| [video_gpu](https://github.com/GregoryKogan/yt-framework/tree/main/examples/video_gpu/) | GPU-oriented sample |

Each example directory has its own README.
