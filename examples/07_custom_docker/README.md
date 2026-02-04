# Custom Docker Example

Demonstrates using custom Docker images for operations that require special dependencies or GPU support.

## What It Demonstrates

- **Custom Docker Images**: Creating and using custom Docker images
- **Dockerfile**: Building Docker images for YT operations
- **Platform Requirements**: Building for linux/amd64 platform
- **Custom Dependencies**: Installing custom tools and libraries

## Features

- Custom Dockerfile with additional tools (cowsay)
- Docker image configuration in operation config
- Platform-specific builds (linux/amd64)
- Custom dependencies in Docker image

## Running

**Prerequisites:**

1. Build Docker image:
```bash
docker buildx build --platform linux/amd64 --tag my-registry/my-image:latest --load .
```

2. Update image name in `stages/run_in_docker/config.yaml`

3. Run pipeline (prod mode required):
```bash
python pipeline.py
```

## Files

- `pipeline.py`: Pipeline entry point
- `Dockerfile`: Custom Docker image definition
- `stages/run_in_docker/stage.py`: Stage that runs vanilla operation in Docker
- `stages/run_in_docker/src/vanilla.py`: Vanilla script using custom tools
- `stages/run_in_docker/config.yaml`: Docker image configuration
- `configs/config.yaml`: Pipeline configuration (prod mode)

## Key Concepts

- Docker images must be built for `linux/amd64` platform
- Docker image is specified in operation resources
- Custom tools installed in image are available in operations
- Prod mode is required for Docker operations

## Dockerfile Pattern

```dockerfile
# Build for linux/amd64 platform
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    custom-tool \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    package1 \
    package2

WORKDIR /app
```

## Configuration

```yaml
client:
  operations:
    vanilla:
      resources:
        docker_image: my-registry/my-image:latest
        pool: default
        memory_limit_gb: 2
        cpu_limit: 1
```

## Next Steps

- See [Docker Guide](../../docs/advanced/docker.md) for detailed Docker documentation
- See [video_gpu](../video_gpu/) for GPU-enabled Docker images
