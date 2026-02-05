# YT Framework

![GitHub License](https://img.shields.io/github/license/GregoryKogan/yt-framework)
[![PyPI - Version](https://img.shields.io/pypi/v/yt_framework)](https://pypi.org/project/yt-framework/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/yt_framework)
![Documentation Status](https://app.readthedocs.org/projects/yt-framework/badge/?version=latest)

**[PyPI](https://pypi.org/project/yt-framework/) | [Documentation](https://yt-framework.readthedocs.io/en/latest/) | [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples)**

---

## Overview

A powerful Python framework for building and executing data processing pipelines on [YTsaurus](https://ytsaurus.tech/) (YT) clusters. YT Framework simplifies pipeline development with automatic stage discovery, seamless dev/prod mode switching, and comprehensive support for YT operations.

## Architecture

YT Framework follows a pipeline-based architecture where pipelines consist of stages, and stages execute operations.

**Key Components:**

- **Pipeline**: Orchestrates stages, their execution order, and configuration management
- **Stages**: Reusable units of work that execute operations
- **Operations**: Specific tasks (Map, Vanilla, YQL, S3, Table operations)
- **Configuration**: YAML-based configuration system for flexible pipeline setup

## Key Features

- **Pipeline & Stage Architecture**: Organize complex workflows into reusable stages
- **Automatic Stage Discovery**: No manual registration needed - just create stages and run
- **Dev/Prod Modes**: Develop locally with file system simulation, deploy to YT cluster seamlessly
- **Multiple Operation Types**: Support for Map, Vanilla, YQL, and S3 operations
- **Code Upload**: Automatic code packaging and deployment to YT cluster
- **Docker Support**: Custom Docker images for special dependencies
- **Checkpoint Management**: Built-in support for ML model checkpoints
- **Configuration Management**: Flexible YAML-based configuration with multiple config support

## Installation

### For Users

Install from [PyPI](https://pypi.org/project/yt-framework/):

```bash
pip install yt-framework
```

### For Developers and Contributors

Install in editable mode from source:

```bash
git clone https://github.com/GregoryKogan/yt-framework.git
cd yt-framework
pip install -e .
```

For development with testing tools:

```bash
pip install -e ".[dev]"
```

See [Installation Guide](https://yt-framework.readthedocs.io/en/latest/#installation) for prerequisites and detailed setup instructions.

## Quick Start

Create your first pipeline in 3 steps:

**What you'll build:** A simple pipeline that creates a stage, logs a message, and demonstrates the basic framework structure.

1. **Create pipeline structure**:

   ```bash
   mkdir my_pipeline && cd my_pipeline
   mkdir -p stages/my_stage configs
   ```

2. **Create `pipeline.py`**:

   ```python
   from yt_framework.core.pipeline import DefaultPipeline
   
   if __name__ == "__main__":
       DefaultPipeline.main()
   ```

3. **Create stage and config**:

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

**Run your pipeline:**

```bash
python pipeline.py
```

**Next Steps:**

- See the [Quick Start Guide](https://yt-framework.readthedocs.io/en/latest/#quick-start) for a complete example with table operations
- Explore [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples) to see more complex use cases
- Read about [Pipelines and Stages](https://yt-framework.readthedocs.io/en/latest/pipelines-and-stages.html) in the documentation

## Examples

The [`examples/`](https://github.com/GregoryKogan/yt-framework/tree/main/examples) directory contains comprehensive examples demonstrating most framework features.
Each example includes a README explaining what it demonstrates and how to run it.

## Requirements

### Prerequisites Checklist

- [ ] **Python 3.11+** installed
- [ ] **YT cluster access and credentials** (for production mode)

### YT Cluster Requirements

When running pipelines in production mode, code from `ytjobs` executes on YT cluster nodes. The cluster's Docker image (default or custom) must include:

- **Python 3.11+**
- **ytsaurus-client** >= 0.13.0 (for checkpoint operations)
- **boto3** == 1.35.99 (for S3 operations)
- **botocore** == 1.35.99 (auto-installed with boto3)

**Important:** Ensure your cluster's default Docker image satisfies these dependencies, or always use custom Docker images for your pipelines. See [Cluster Requirements](https://yt-framework.readthedocs.io/en/latest/configuration/cluster-requirements.html) and [Custom Docker Images](https://yt-framework.readthedocs.io/en/latest/advanced/docker.html) for details.

## Documentation

**Full documentation available at: [yt-framework.readthedocs.io](https://yt-framework.readthedocs.io/en/latest/)**

For local development, source documentation is available in the [`docs/`](docs/) directory.

**[Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples)** - Complete working examples for most features

## Getting Help

- **Documentation**: Check the [full documentation](https://yt-framework.readthedocs.io/en/latest/) for detailed guides
- **Troubleshooting**: See the [Troubleshooting Guide](https://yt-framework.readthedocs.io/en/latest/troubleshooting/index.html) for common issues
- **Examples**: Browse [working examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples) to see how features are used
- **GitHub Issues**: Report bugs or request features on [GitHub Issues](https://github.com/GregoryKogan/yt-framework/issues)
- **Questions**: Open a GitHub issue with the `question` label

## Contributing

We welcome contributions! Whether it's bug fixes, new features, documentation improvements, or examples, your help makes YT Framework better.  
See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.
