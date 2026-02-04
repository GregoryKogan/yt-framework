# Docker Support

YT Framework supports custom Docker images for operations that require special dependencies, GPU support, or custom environments.

## Overview

Custom Docker images allow you to:

- Install custom dependencies
- Use GPU-enabled environments
- Customize the execution environment
- Ensure consistent environments across operations

**Key points:**

- Specify Docker image in operation config
- Image must be compatible with YT cluster
- GPU support requires GPU-enabled images
- Docker authentication supported

## When to Use Custom Docker

### GPU Workloads

For GPU processing, you need a GPU-enabled Docker image:

```yaml
client:
  operations:
    map:
      resources:
        docker_image: nvidia/cuda:11.8.0-runtime-ubuntu22.04
        gpu_limit: 1
        memory_limit_gb: 16
```

### Custom Dependencies

For operations requiring specific libraries or tools:

```yaml
client:
  operations:
    vanilla:
      resources:
        docker_image: my-registry/my-custom-image:latest
        memory_limit_gb: 4
```

### Consistent Environments

For reproducible environments across teams:

```yaml
client:
  operations:
    map:
      resources:
        docker_image: my-registry/standard-python:3.11
        memory_limit_gb: 4
```

## Creating Docker Images

### Basic Dockerfile

Create a `Dockerfile` in your pipeline or stage directory:

```dockerfile
# Build for linux/amd64 platform (required for YT cluster compatibility)
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    numpy>=1.20.0 \
    pandas>=1.3.0

WORKDIR /app
```

### Platform Requirements

**Important:** YT cluster requires `linux/amd64` platform:

```bash
# Build for correct platform
docker buildx build --platform linux/amd64 --tag my-image:latest --load .
```

Or use buildx:

```bash
docker buildx build --platform linux/amd64 --tag my-image:latest --push .
```

### GPU Dockerfile

For GPU workloads:

```dockerfile
# Use NVIDIA CUDA base image
FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

# Install Python
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install GPU-enabled libraries
RUN pip3 install --no-cache-dir \
    torch>=2.0.0 \
    torchvision>=0.15.0

WORKDIR /app
```

**Note:** GPU images are larger and take longer to pull.

### Minimal Dockerfile

For simple operations:

```dockerfile
FROM python:3.11-slim

# Install only what you need
RUN pip install --no-cache-dir omegaconf

WORKDIR /app
```

## Configuration

### Basic Configuration

Specify Docker image in operation config:

```yaml
# stages/my_stage/config.yaml
client:
  operations:
    map:
      resources:
        docker_image: my-registry/my-image:latest
        pool: default
        memory_limit_gb: 4
        cpu_limit: 2
```

### Docker Image Location

Docker images can be:

- **Public registry**: `python:3.11-slim`, `nvidia/cuda:11.8.0`
- **Private registry**: `my-registry/my-image:latest`
- **YT registry**: `//path/to/image` (if using YT's Docker registry)

### GPU Configuration

For GPU workloads:

```yaml
client:
  operations:
    map:
      resources:
        docker_image: nvidia/cuda:11.8.0-runtime-ubuntu22.04
        gpu_limit: 1              # Request 1 GPU
        memory_limit_gb: 16       # More memory for GPU workloads
        cpu_limit: 4
```

**GPU requirements:**

- GPU-enabled Docker image
- `gpu_limit` set to 1 or higher
- Sufficient memory (GPU workloads need more)

## Docker Authentication

For private registries, configure Docker authentication via environment variables in `secrets.env`:

### Authentication Configuration

Add Docker credentials to `configs/secrets.env`:

```bash
# configs/secrets.env
DOCKER_AUTH_USERNAME=myuser
DOCKER_AUTH_PASSWORD=mypassword
```

The framework automatically uses these credentials when a Docker image is specified in the operation config:

```yaml
client:
  operations:
    map:
      resources:
        docker_image: my-registry/private-image:latest
        # Docker auth is automatically loaded from secrets.env
```

**Note:** Docker authentication is only used if all three are present: `docker_image`, `DOCKER_AUTH_USERNAME`, and `DOCKER_AUTH_PASSWORD` in secrets.env.

## Complete Example

### Dockerfile

```dockerfile
# Build for linux/amd64 platform
FROM python:3.11-slim

# Install system tools
RUN apt-get update && apt-get install -y \
    cowsay \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    omegaconf \
    botocore \
    boto3

# Make cowsay available
RUN ln -sf /usr/games/cowsay /usr/local/bin/cowsay

WORKDIR /app
```

### Stage Configuration

```yaml
# stages/run_in_docker/config.yaml
client:
  operations:
    vanilla:
      resources:
        docker_image: my-registry/my-image:latest
        pool: default
        memory_limit_gb: 2
        cpu_limit: 1
```

### Stage Code

```python
# stages/run_in_docker/stage.py
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.vanilla import run_vanilla

class RunInDockerStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.vanilla,
        )
        
        if not success:
            raise RuntimeError("Vanilla operation failed")
        
        return debug
```

### Vanilla Script

```python
# stages/run_in_docker/src/vanilla.py
#!/usr/bin/env python3
import subprocess
import logging
from ytjobs.logging.logger import get_logger

def main():
    logger = get_logger("docker-example", level=logging.INFO)
    
    # Use custom tool from Docker image
    result = subprocess.run(
        ["cowsay", "Hello from Docker!"],
        capture_output=True,
        text=True,
    )
    
    logger.info(result.stdout)

if __name__ == "__main__":
    main()
```

See [Example: 07_custom_docker](../../examples/07_custom_docker/) for complete example.

## Best Practices

### Image Size

**Keep images small:**

- Use slim base images (`python:3.11-slim`)
- Remove unnecessary packages
- Use multi-stage builds if needed
- Clean up apt cache

**Example:**

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
```

### Dependency Management

**Install dependencies in image:**

- Pre-install common dependencies
- Use `requirements.txt` for stage-specific deps
- Pin versions for reproducibility

**Example:**

```dockerfile
FROM python:3.11-slim

# Pre-install common dependencies
RUN pip install --no-cache-dir \
    numpy>=1.20.0 \
    pandas>=1.3.0

# Stage-specific deps installed at runtime via requirements.txt
WORKDIR /app
```

### Version Tagging

**Tag images with versions:**

```dockerfile
# Build with version tag
docker buildx build --platform linux/amd64 \
    --tag my-registry/my-image:v1.2.3 \
    --push .
```

**Use in config:**

```yaml
docker_image: my-registry/my-image:v1.2.3
```

### Testing Images

**Test images locally:**

```bash
# Build image
docker buildx build --platform linux/amd64 --tag my-image:test --load .

# Test image
docker run --rm my-image:test python3 -c "import numpy; print(numpy.__version__)"
```

## Common Patterns

### Python with ML Libraries

```dockerfile
FROM python:3.11-slim

RUN pip install --no-cache-dir \
    numpy>=1.20.0 \
    pandas>=1.3.0 \
    scikit-learn>=1.0.0 \
    transformers>=4.20.0
```

### GPU with PyTorch

```dockerfile
FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y \
    python3.11 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    torch>=2.0.0 \
    torchvision>=0.15.0
```

### Custom Tools

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    imagemagick \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    opencv-python>=4.5.0
```

## Troubleshooting

### Issue: Image not found

- Check image name and tag
- Verify image exists in registry
- Check Docker authentication

### Issue: Platform mismatch

- Build for `linux/amd64` platform
- Use `docker buildx` for cross-platform builds

### Issue: GPU not available

- Verify GPU-enabled image
- Check `gpu_limit` is set
- Verify cluster has GPU nodes

### Issue: Slow image pull

- Use smaller base images
- Cache layers effectively
- Use local registry if possible

### Issue: Dependencies missing

- Check image includes required packages
- Verify `requirements.txt` is correct
- Review installation logs

## Next Steps

- Learn about [Checkpoints](checkpoints.md) for model files
- Explore [Code Upload](code-upload.md) for code packaging
- Check out [Example: 07_custom_docker](../../examples/07_custom_docker/) for complete example
