# Video GPU Example

Demonstrates GPU processing workflows using custom Docker images and GPU resources. Shows how to process video data with GPU acceleration.

## What It Demonstrates

- **GPU Processing**: Using GPU resources for compute-intensive workloads
- **Custom Docker**: GPU-enabled Docker images
- **GPU Resources**: Configuring GPU limits and resources
- **Video Processing**: Processing video data with GPU acceleration

## Features

- GPU-enabled Docker image
- GPU resource configuration
- Map operation with GPU support
- Video processing pipeline

## Running

**Prerequisites:**

1. Build GPU-enabled Docker image:

```bash
docker buildx build --platform linux/amd64 --tag my-registry/gpu-image:latest --load .
```

1. Update Docker image in `stages/run_map/config.yaml`

2. Run pipeline (prod mode required):

```bash
python pipeline.py
```

## Files

- `pipeline.py`: Pipeline entry point
- `stages/create_table/stage.py`: Stage that creates input table
- `stages/join_tables/stage.py`: Stage that joins tables
- `stages/run_map/stage.py`: Stage that runs GPU map operation
- `stages/run_map/src/mapper.py`: Mapper script with GPU processing
- `stages/run_map/src/processor.py`: GPU processing logic
- `stages/run_map/Dockerfile`: GPU-enabled Docker image
- `stages/run_map/config.yaml`: GPU operation configuration
- `configs/config.yaml`: Pipeline configuration

## Key Concepts

- GPU operations require GPU-enabled Docker images
- Set `gpu_limit` to 1 or higher in resources
- Allocate sufficient memory for GPU workloads
- GPU code uses CUDA libraries (PyTorch, etc.)

## Configuration

```yaml
client:
  operations:
    map:
      resources:
        docker_image: my-registry/gpu-image:latest
        gpu_limit: 1              # Request GPU
        memory_limit_gb: 16       # More memory for GPU
        cpu_limit: 4
        pool: default
```

## Dockerfile Pattern

```dockerfile
FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y \
    python3.11 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    torch>=2.0.0 \
    torchvision>=0.15.0 \
    opencv-python>=4.5.0

WORKDIR /app
```

## GPU Processing Pattern

```python
import torch

# Check GPU availability
if torch.cuda.is_available():
    device = torch.device("cuda")
else:
    device = torch.device("cpu")

# Move model to GPU
model = model.to(device)

# Process on GPU
output = model(input_data.to(device))
```

## Next Steps

- See [Docker Guide](../../docs/advanced/docker.md) for Docker documentation
- See [04_map_operation](../04_map_operation/) for map operations
