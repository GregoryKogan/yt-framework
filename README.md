# YT Framework

[![PyPI - Version](https://img.shields.io/pypi/v/yt_framework)](https://pypi.org/project/yt-framework/)
[![Documentation Status](https://app.readthedocs.org/projects/yt-framework/badge/?version=latest)](https://yt-framework.readthedocs.io)
[![CI](https://github.com/GregoryKogan/yt-framework/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GregoryKogan/yt-framework/actions/workflows/ci.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/GregoryKogan/yt-framework)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/yt_framework)
[![coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2FGregoryKogan%2F293242803e295690b612e93aff8c151e%2Fraw%2Fyt-framework-coverage.json&cacheSeconds=60)](https://github.com/GregoryKogan/yt-framework/actions/workflows/ci.yml)
![GitHub License](https://img.shields.io/github/license/GregoryKogan/yt-framework)

**[PyPI](https://pypi.org/project/yt-framework/) | [Docs](https://yt-framework.readthedocs.io/en/latest/) | [DeepWiki](https://deepwiki.com/GregoryKogan/yt-framework) | [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples)**

---

## Overview

Python helpers and conventions for [YTsaurus](https://ytsaurus.tech/) pipelines: YAML config, ordered stages under `stages/`, dev mode that mirrors many prod behaviors on disk, and prod mode that uploads `src/` bundles to the cluster.

## Architecture

- **Pipeline** — loads config, builds the YT client, walks `enabled_stages`.
- **Stage** — one `BaseStage` subclass plus `config.yaml` (and optional `src/` for jobs).
- **Operations** — map, vanilla, map-reduce/reduce, YQL via the client, S3 helpers, sorts, etc.
- **Configuration** — OmegaConf-backed YAML; secrets in `configs/secrets.env`.

## What ships in the box

- Stage discovery (`DefaultPipeline`) from the filesystem layout.
- `dev` / `prod` switch on the same code paths where possible.
- Map, vanilla, YQL helpers, S3 listing/download patterns, table helpers, checkpoint upload wiring.
- Optional custom Docker images, tokenizer tarballs, and multi-operation stages.


## Installation

### For Users

Install from [PyPI](https://pypi.org/project/yt-framework/) into any Python 3.11+ environment (system Python, a virtualenv, or a Conda env):

```bash
pip install yt-framework
```

### For Developers and Contributors

**Recommended: one Conda environment** for tests, formatting, pre-commit, and local documentation builds (avoids reinstalling tooling for each task):

```bash
git clone https://github.com/GregoryKogan/yt-framework.git
cd yt-framework
conda create -n yt-framework python=3.11
conda activate yt-framework
pip install -e ".[dev,docs]"
```

Use `conda-forge` as the channel when creating the env if that matches your setup (`conda create -n yt-framework python=3.11 -c conda-forge`).

**Alternative: pip only** — install in editable mode from source:

```bash
git clone https://github.com/GregoryKogan/yt-framework.git
cd yt-framework
pip install -e .
```

For development with testing tools (without the docs extra):

```bash
pip install -e ".[dev]"
```

For local Sphinx builds without the full dev extra, use `pip install -e ".[docs]"`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development setup and [Installation Guide](https://yt-framework.readthedocs.io/en/latest/#installation) for prerequisites.

## Quick start

Three files: layout, entrypoint, stage + pipeline config.

1. **Layout**

   ```bash
   mkdir my_pipeline && cd my_pipeline
   mkdir -p stages/my_stage configs
   ```

2. **`pipeline.py`**

   ```python
   from yt_framework.core.pipeline import DefaultPipeline
   
   if __name__ == "__main__":
       DefaultPipeline.main()
   ```

3. **Stage + config**

   ```python
   # stages/my_stage/stage.py
   from yt_framework.core.stage import BaseStage
   
   class MyStage(BaseStage):
       def run(self, debug):
           self.logger.info("Hello from YT Framework!")
           return debug
   ```

   ```yaml
   # configs/config.yaml
   stages:
     enabled_stages:
       - my_stage
   
   pipeline:
     mode: "dev"  # Use "dev" for local development
   ```

```bash
python pipeline.py
```

**Next:** [Docs quick start](https://yt-framework.readthedocs.io/en/latest/#quick-start) (table write), [examples/](https://github.com/GregoryKogan/yt-framework/tree/main/examples), [Pipelines and stages](https://yt-framework.readthedocs.io/en/latest/pipelines-and-stages.html).

## Examples

[`examples/`](https://github.com/GregoryKogan/yt-framework/tree/main/examples) holds runnable trees; each folder has a README with scope and commands.

## Requirements

### Prerequisites

- Python **3.11+**
- YT **proxy + token** when you run `pipeline.mode: prod`

### YT Cluster Requirements

When running pipelines in production mode, code from `ytjobs` executes on YT cluster nodes. The cluster's Docker image (default or custom) must include:

- **Python 3.11+**
- **ytsaurus-client** >= 0.13.0 (for checkpoint operations)
- **boto3** == 1.35.99 (for S3 operations)
- **botocore** == 1.35.99 (auto-installed with boto3)

If the cell default image lacks those pins, build a [custom Docker image](https://yt-framework.readthedocs.io/en/latest/advanced/docker.html). Background: [Cluster requirements](https://yt-framework.readthedocs.io/en/latest/configuration/cluster-requirements.html).

## Documentation

- Published: [yt-framework.readthedocs.io](https://yt-framework.readthedocs.io/en/latest/)
- Source: [`docs/`](docs/)
- [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples)

## Getting help

- [Troubleshooting](https://yt-framework.readthedocs.io/en/latest/troubleshooting/index.html)
- [GitHub Issues](https://github.com/GregoryKogan/yt-framework/issues) (bugs, features, questions with the `question` label)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
