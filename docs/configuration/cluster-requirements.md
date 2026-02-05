# YT Cluster Requirements

When running pipelines in production mode, code from the `ytjobs` package executes on YT cluster nodes. This means the cluster's Docker image (whether default or custom) must include all dependencies required by your `ytjobs` code.

```{warning}
**Critical: Cluster Dependencies**

Unlike local development, where dependencies are installed on your machine, production mode requires dependencies to be present in the cluster's Docker image. Missing dependencies will cause job failures.
```

## Why Cluster Dependencies Matter

In production mode:

1. **Code execution location**: Your `ytjobs` code runs on YT cluster nodes, not on your local machine
2. **Docker isolation**: Each job runs in a Docker container on the cluster
3. **Dependency availability**: Only packages installed in the Docker image are available to your code

## Python Version Requirement

**Minimum: Python 3.11+**

The framework requires Python 3.11 or higher. Ensure your cluster's Docker image includes Python 3.11 or newer. Lower versions are not guaranteed to work.

## Core Dependencies

These dependencies are required for basic `ytjobs` functionality:

### ytsaurus-client

**Version:** >= 0.13.0

**Required for:**
- Checkpoint operations (`ytjobs.checkpoint`)
- YT file system operations

**Usage:**
```python
from ytjobs.checkpoint import save_checkpoint, load_checkpoint
```

**Installation:**
```bash
pip install ytsaurus-client>=0.13.0
```

### boto3 and botocore

**Versions:**
- `boto3 == 1.35.99`
- `botocore == 1.35.99` (auto-installed with boto3)

**Note:** `1.35.xx` version is fixed because it is possible to control how many pool connections are used by boto3 in this version.

**Required for:**
- S3 operations (`ytjobs.s3`)
- S3 file listing, downloading, uploading

**Usage:**
```python
from ytjobs.s3 import S3Client
```

**Installation:**
```bash
pip install boto3==1.35.99
```

## Optional Dependencies

These dependencies are not strictly required but are recommended for optimal functionality:

### omegaconf

**Version:** >= 2.3.0

**Recommended for:**
- Reading configuration YAML files (`config.yaml`) passed to jobs
- Optimal way to load and access job configuration

**Usage:**
```python
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

config = OmegaConf.load(get_config_path())
# Access config values
value = config.job.some_setting
```

**Installation:**
```bash
pip install omegaconf>=2.3.0
```

**Note:** While not strictly required, `omegaconf` is the recommended way to read configuration files in your job code. Without it, you would need to manually parse YAML files using the standard library.

## Dependency Breakdown by Module

### Core Modules (Standard Library Only)

These modules require **no external dependencies** for basic functionality:

- `ytjobs.config` - Configuration utilities (note: `omegaconf` recommended for reading config files)
- `ytjobs.logging` - Logging utilities
- `ytjobs.mapper` - Mapper utilities
- `ytjobs.chatml` - ChatML formatting

### Feature-Specific Modules

**Checkpoint module** (`ytjobs.checkpoint`):
- Requires: `ytsaurus-client >= 0.13.0`

**S3 module** (`ytjobs.s3`):
- Requires: `boto3 == 1.35.99`, `botocore == 1.35.99`

## Minimum Requirements for Full Functionality

If you use all `ytjobs` features, your cluster Docker image must include:

```bash
Python >= 3.11
ytsaurus-client >= 0.13.0
boto3 == 1.35.99
botocore == 1.35.99
```

**Recommended additions:**

```bash
omegaconf >= 2.3.0              # Recommended for reading config files
```

## Solutions

You have two options to ensure dependencies are available:

### Option 1: Default Cluster Image

Ensure your YT cluster's **default Docker image** includes all required dependencies.

**Advantages:**
- No configuration needed
- Works automatically for all pipelines
- Consistent environment across teams

**Disadvantages:**
- Requires cluster administrator access
- May not be possible if you don't control the cluster
- All teams must agree on dependencies

**How to check:**
Contact your cluster administrator to verify the default Docker image includes:
- Python 3.11+
- Required Python packages (ytsaurus-client, boto3, etc.)

### Option 2: Custom Docker Images

Always use **custom Docker images** for your pipelines that include the required dependencies.

**Advantages:**
- Full control over dependencies
- No need to modify cluster defaults
- Can include additional dependencies as needed
- Version pinning for reproducibility

**Disadvantages:**
- Must specify `docker_image` in each operation config
- Requires Docker image building and registry access

**How to use:**
See [Custom Docker Images](../advanced/docker.md) for complete guide on creating and using custom Docker images.

**Example Dockerfile:**
```dockerfile
FROM python:3.11-slim

# Install required dependencies
RUN pip install --no-cache-dir \
    ytsaurus-client>=0.13.0 \
    boto3==1.35.99 \
    omegaconf>=2.3.0

WORKDIR /app
```

**Example config:**
```yaml
client:
  operations:
    map:
      resources:
        docker_image: my-registry/my-image:latest
        memory_limit_gb: 4
```

## Verifying Cluster Compatibility

### Check Python Version

Create a test vanilla operation to check Python version:

```python
# stages/test_python/src/vanilla.py
import sys
print(f"Python version: {sys.version}")
```

Run in prod mode and check logs for Python version.

### Check Dependencies

Create a test operation to verify dependencies:

```python
# stages/test_deps/src/vanilla.py
try:
    import yt.wrapper as yt
    print("✓ ytsaurus-client available")
except ImportError:
    print("✗ ytsaurus-client missing")

try:
    import boto3
    print(f"✓ boto3 available: {boto3.__version__}")
except ImportError:
    print("✗ boto3 missing")

try:
    import omegaconf
    print(f"✓ omegaconf available: {omegaconf.__version__}")
except ImportError:
    print("✗ omegaconf missing (recommended for config reading)")

try:
    import cv2
    print(f"✓ opencv available: {cv2.__version__}")
except ImportError:
    print("✗ opencv missing")
```

### Common Issues

**Issue: ImportError for ytsaurus-client**
- **Solution:** Install `ytsaurus-client>=0.13.0` in Docker image
- **Check:** Verify you're using checkpoint operations

**Issue: ImportError for boto3**
- **Solution:** Install `boto3==1.35.99` in Docker image
- **Check:** Verify you're using S3 operations

**Issue: Python version too old**
- **Solution:** Use Docker image with Python 3.11+
- **Check:** Verify Python version in cluster image

## Best Practices

### 1. Document Your Dependencies

List all `ytjobs` modules you use in your pipeline documentation:

```markdown
## Dependencies

This pipeline uses:
- `ytjobs.s3` (requires boto3)
- `ytjobs.checkpoint` (requires ytsaurus-client)
```

### 2. Use Custom Docker Images

For production pipelines, always use custom Docker images with pinned dependency versions:

```dockerfile
FROM python:3.11-slim

RUN pip install --no-cache-dir \
    ytsaurus-client==0.13.0 \
    boto3==1.35.99 \
    botocore==1.35.99 \
    omegaconf>=2.3.0
```

### 3. Test Dependencies Early

Create a simple test stage that imports all `ytjobs` modules you use:

```python
# stages/test_dependencies/src/vanilla.py
from ytjobs.s3 import S3Client
from ytjobs.checkpoint import save_checkpoint
print("All dependencies available!")
```

### 4. Version Pinning

Pin exact versions in your Docker images for reproducibility:

```dockerfile
RUN pip install --no-cache-dir \
    ytsaurus-client==0.13.0 \
    boto3==1.35.99 \
    botocore==1.35.99 \
    omegaconf>=2.3.0
```

### 5. Minimal Images

Only install dependencies you actually use:

- If you don't use S3, don't install boto3
- If you don't use checkpoints, don't install ytsaurus-client
- **Note:** `omegaconf` is recommended even for minimal images if you read config files in your jobs

## Related Documentation

- [Custom Docker Images](../advanced/docker.md) - Complete guide to using custom Docker images
- [Dev vs Prod Modes](../dev-vs-prod.md) - Understanding execution modes
- [Code Upload](../advanced/code-upload.md) - How code is packaged and uploaded
- [Troubleshooting](../troubleshooting/index.md) - Common issues and solutions

## Summary

**Key Points:**

1. **Code runs on cluster**: `ytjobs` code executes on YT cluster nodes, not locally
2. **Docker image must have dependencies**: All required packages must be pre-installed in the Docker image
3. **Python 3.11+ required**: Minimum Python version for the framework
4. **Core dependencies**: ytsaurus-client (checkpoints), boto3 (S3 operations)
5. **Recommended**: omegaconf for optimal config file reading
6. **Two solutions**: Use default cluster image with dependencies OR always use custom Docker images

**Action Items:**

- [ ] Verify your cluster's default Docker image includes required dependencies
- [ ] If not, create custom Docker images with required dependencies
- [ ] Test dependencies early with a simple test operation
- [ ] Document which `ytjobs` modules your pipeline uses
- [ ] Pin dependency versions in Docker images for reproducibility
