# YT Framework

![GitHub License](https://img.shields.io/github/license/GregoryKogan/yt-framework)
![PyPI - Version](https://img.shields.io/pypi/v/yt_framework)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/yt_framework)

A powerful Python framework for building and executing data processing pipelines on YTsaurus (YT) clusters. YT Framework simplifies pipeline development with automatic stage discovery, seamless dev/prod mode switching, and comprehensive support for YT operations.

## Key Features

- **Pipeline & Stage Architecture**: Organize complex workflows into reusable stages
- **Automatic Stage Discovery**: No manual registration needed - just create stages and run
- **Dev/Prod Modes**: Develop locally with file system simulation, deploy to YT cluster seamlessly
- **Multiple Operation Types**: Support for Map, Vanilla, YQL, and S3 operations
- **Code Upload**: Automatic code packaging and deployment to YT cluster
- **Docker Support**: Custom Docker images for GPU workloads and special dependencies
- **Checkpoint Management**: Built-in support for ML model checkpoints
- **Configuration Management**: Flexible YAML-based configuration with multiple config support

## Quick Links

- **[Installation & Quick Start](docs/index.md#installation)** - Get up and running in minutes
- **[Pipelines & Stages](docs/pipelines-and-stages.md)** - Core concepts and architecture
- **[Operations Guide](docs/operations/)** - Map, Vanilla, YQL, and S3 operations
- **[Advanced Topics](docs/advanced/)** - Docker, checkpoints, code upload, and more
- **[Examples](examples/)** - Complete working examples for every feature

## Installation

### For Users

Install from PyPI:

```bash
pip install yt_framework
```

### For Developers and Contributors

Install in editable mode from source:

```bash
pip install -e .
```

See [Installation Guide](docs/index.md#installation) for prerequisites and detailed setup instructions.

## Quick Start

Create your first pipeline in 3 steps:

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

See [Quick Start Guide](docs/index.md#quick-start) for complete example.

## Examples

The `examples/` directory contains comprehensive examples demonstrating all framework features:

- **[01_hello_world](examples/01_hello_world/)** - Basic pipeline and table operations
- **[02_multi_stage_pipeline](examples/02_multi_stage_pipeline/)** - Multiple stages with data flow
- **[03_yql_operations](examples/03_yql_operations/)** - All YQL table operations
- **[04_map_operation](examples/04_map_operation/)** - Map operations with custom code
- **[05_vanilla_operation](examples/05_vanilla_operation/)** - Vanilla standalone jobs
- **[06_s3_integration](examples/06_s3_integration/)** - S3 file listing and processing
- **[07_custom_docker](examples/07_custom_docker/)** - Custom Docker images
- **[08_multiple_configs](examples/08_multiple_configs/)** - Multiple configuration files
- **[09_multiple_operations](examples/09_multiple_operations/)** - Combining operations in one stage
- **[environment_log](examples/environment_log/)** - Comprehensive environment logging
- **[video_gpu](examples/video_gpu/)** - GPU processing workflows

## Documentation

Full documentation is available in the [`docs/`](docs/) directory:

- **[Main Documentation](docs/index.md)** - Installation, quick start, and overview
- **[Pipelines & Stages](docs/pipelines-and-stages.md)** - Core architecture
- **[Configuration](docs/configuration/index.md)** - Config files and secrets management
- **[Dev vs Prod](docs/dev-vs-prod.md)** - Development and production modes
- **[Operations](docs/operations/)** - Map, Vanilla, YQL, S3 operations
- **[Advanced Topics](docs/advanced/)** - Docker, checkpoints, code upload
- **[API Reference](docs/reference/api.md)** - Complete API documentation
- **[Troubleshooting](docs/troubleshooting/index.md)** - Common issues and solutions

## Requirements

- Python 3.11+
- YTsaurus cluster access (for production mode)
- YT credentials (for production mode)
