# YT Framework Documentation

Welcome to the YT Framework documentation! This guide will help you get started with building data processing pipelines on YTsaurus.

```{toctree}
:maxdepth: 2
:caption: Getting Started
:hidden:

configuration/index
configuration/secrets
configuration/advanced
dev-vs-prod
pipelines-and-stages
```

```{toctree}
:maxdepth: 2
:caption: Operations
:hidden:

operations/index
operations/map
operations/vanilla
operations/yql
operations/s3
```

```{toctree}
:maxdepth: 2
:caption: Advanced Topics
:hidden:

advanced/index
advanced/code-upload
advanced/docker
advanced/checkpoints
advanced/multiple-operations
```

```{toctree}
:maxdepth: 2
:caption: Reference
:hidden:

reference/api
troubleshooting/index
troubleshooting/pipeline
troubleshooting/operations
troubleshooting/configuration
troubleshooting/debugging
```

## Introduction

YT Framework is a Python framework designed to simplify the development and execution of data processing pipelines on YTsaurus (YT) clusters. It provides:

- **Simple Pipeline Architecture**: Organize your workflows into stages
- **Seamless Development**: Develop locally, deploy to production with minimal changes
- **Comprehensive Operations**: Support for Map, Vanilla, YQL, and S3 operations
- **Automatic Code Management**: Handles code upload, dependencies, and execution automatically

### Why YT Framework?

- **Fast Development**: Automatic stage discovery means less boilerplate
- **Local Testing**: Dev mode simulates YT operations locally using file system
- **Production Ready**: Same code runs in dev and prod modes
- **Flexible**: Supports everything from simple table operations to complex ML inference pipelines

## Installation

### Prerequisites

- Python 3.11 or higher
- Access to YTsaurus cluster (for production mode)
- YT credentials (for production mode)

### Install from Source

For local development, install the package in editable mode:

```bash
pip install -e .
```

### Verify Installation

```bash
python -c "import yt_framework; print(yt_framework.__version__)"
```

### Configuration Setup

```{warning}
**Secrets Required for Production Mode**

Production mode requires YT credentials. Make sure to set up `secrets.env` before running in prod mode.
```

After installation, you'll need to set up your YT credentials for production mode. Create a `secrets.env` file in your pipeline's `configs/` directory:

```bash
# configs/secrets.env
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token
```

For S3 integration, also add:

```bash
S3_ENDPOINT=https://your-s3-endpoint.com
S3_DOWNLOAD_ACCESS_KEY=your-download-access-key
S3_DOWNLOAD_SECRET_KEY=your-download-secret-key
S3_UPLOAD_ACCESS_KEY=your-upload-access-key
S3_UPLOAD_SECRET_KEY=your-upload-secret-key
```

See [Secrets Management](configuration/secrets.md) for more details.

## Quick Start

Let's create a simple pipeline that creates a table with some data.

### Step 1: Create Pipeline Structure

```bash
mkdir my_first_pipeline
cd my_first_pipeline
mkdir -p stages/create_data configs
```

### Step 2: Create Pipeline Entry Point

Create `pipeline.py` in the root directory:

```python
from yt_framework.core.pipeline import DefaultPipeline

if __name__ == "__main__":
    DefaultPipeline.main()
```

### Step 3: Create Stage Configuration

Create `configs/config.yaml`:

```yaml
stages:
  enabled_stages:
    - create_data

pipeline:
  mode: "dev"  # Use "prod" for production
```

### Step 4: Create Stage

Create `stages/create_data/stage.py`:

```python
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage

class CreateDataStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Creating data table...")
        
        # Create some sample data
        rows = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300},
        ]
        
        # Write to YT table
        self.deps.yt_client.write_table(
            table_path=self.config.client.output_table,
            rows=rows,
        )
        
        self.logger.info(f"Created table with {len(rows)} rows")
        return debug
```

Create `stages/create_data/config.yaml`:

```yaml
client:
  output_table: //tmp/my_first_pipeline/data
```

### Step 5: Run the Pipeline

```bash
python pipeline.py
```

In dev mode, the table will be created as `my_first_pipeline/.dev/data.jsonl`. In prod mode, it will be created on the YT cluster at `//tmp/my_first_pipeline/data`.

### Next Steps

- Learn about [Pipelines and Stages](pipelines-and-stages.md)
- Explore [Configuration](configuration/index.md) options
- Understand [Dev vs Prod modes](dev-vs-prod.md)
- Check out [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples/) for more complex scenarios

## Core Concepts

### Pipelines and Stages

A **pipeline** is a collection of **stages** that execute in sequence. Each stage performs a specific task (e.g., create table, process data, upload results).

- **DefaultPipeline**: Automatically discovers stages from `stages/` directory
- **BasePipeline**: Manual stage registration (for advanced use cases)
- **BaseStage**: Base class for all stages

See [Pipelines and Stages](pipelines-and-stages.md) for details.

### Dev vs Prod Modes

```{tip}
**Start with Dev Mode**

Always develop and test your pipelines in dev mode first. It's faster, doesn't require YT credentials, and makes debugging easier.
```

- **Dev Mode**: Simulates YT operations locally using file system. Tables are stored as `.jsonl` files in `.dev/` directory. Perfect for development and testing.
- **Prod Mode**: Executes operations on actual YT cluster. Requires YT credentials and cluster access.

See [Dev vs Prod](dev-vs-prod.md) for complete comparison.

### Configuration System

Configuration is managed through YAML files:

- **Pipeline config** (`configs/config.yaml`): Pipeline-level settings (mode, build_folder)
- **Stage configs** (`stages/<stage_name>/config.yaml`): Stage-specific settings
- **Secrets** (`configs/secrets.env`): Credentials and sensitive data

See [Configuration Guide](configuration/index.md) for details.

## Operations

YT Framework supports several types of operations:

### Map Operations

Process each row of a table independently. Perfect for row-by-row transformations.

- [Map Operations Guide](operations/map.md)
- Example: [04_map_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/04_map_operation/)

### Vanilla Operations

Run standalone jobs without input/output tables. Perfect for setup, cleanup, or validation tasks.

- [Vanilla Operations Guide](operations/vanilla.md)
- Example: [05_vanilla_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/05_vanilla_operation/)

### YQL Operations

Perform table operations using YQL (YTsaurus Query Language). Includes joins, filters, aggregations, and more.

- [YQL Operations Guide](operations/yql.md)
- Example: [03_yql_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/03_yql_operations/)

### S3 Operations

Integrate with S3 for file listing, downloading, and processing.

- [S3 Operations Guide](operations/s3.md)
- Example: [06_s3_integration](https://github.com/GregoryKogan/yt-framework/tree/main/examples/06_s3_integration/)

## Advanced Topics

### Code Upload

Learn how the framework handles code packaging and deployment to YT cluster.

- [Code Upload Guide](advanced/code-upload.md)

### Docker Support

Use custom Docker images for GPU workloads or special dependencies.

- [Docker Guide](advanced/docker.md)
- Example: [07_custom_docker](https://github.com/GregoryKogan/yt-framework/tree/main/examples/07_custom_docker/)

### Checkpoint Management

Handle ML model checkpoints for inference pipelines.

- [Checkpoints Guide](advanced/checkpoints.md)

### Multiple Operations

Run multiple operations in a single stage.

- [Multiple Operations Guide](advanced/multiple-operations.md)
- Example: [09_multiple_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/09_multiple_operations/)

## Reference

- [API Reference](reference/api.md) - Complete API documentation
- [Troubleshooting](troubleshooting/index.md) - Common issues and solutions

## Examples

The `examples/` directory contains complete working examples:

- **[01_hello_world](https://github.com/GregoryKogan/yt-framework/tree/main/examples/01_hello_world/)** - Basic pipeline
- **[02_multi_stage_pipeline](https://github.com/GregoryKogan/yt-framework/tree/main/examples/02_multi_stage_pipeline/)** - Multiple stages
- **[03_yql_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/03_yql_operations/)** - YQL operations
- **[04_map_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/04_map_operation/)** - Map operation
- **[05_vanilla_operation](https://github.com/GregoryKogan/yt-framework/tree/main/examples/05_vanilla_operation/)** - Vanilla operation
- **[06_s3_integration](https://github.com/GregoryKogan/yt-framework/tree/main/examples/06_s3_integration/)** - S3 integration
- **[07_custom_docker](https://github.com/GregoryKogan/yt-framework/tree/main/examples/07_custom_docker/)** - Custom Docker
- **[08_multiple_configs](https://github.com/GregoryKogan/yt-framework/tree/main/examples/08_multiple_configs/)** - Multiple configs
- **[09_multiple_operations](https://github.com/GregoryKogan/yt-framework/tree/main/examples/09_multiple_operations/)** - Multiple operations
- **[environment_log](https://github.com/GregoryKogan/yt-framework/tree/main/examples/environment_log/)** - Environment logging
- **[video_gpu](https://github.com/GregoryKogan/yt-framework/tree/main/examples/video_gpu/)** - GPU processing

Each example includes a README explaining what it demonstrates.
