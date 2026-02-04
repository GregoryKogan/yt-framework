# Checkpoint Management

Checkpoint management allows you to use ML model files in your operations. The framework handles checkpoint upload, mounting, and access automatically.

## Overview

Checkpoints are model files (e.g., PyTorch `.pth` files, HuggingFace models) that need to be available to operations. The framework:

- Uploads checkpoints to YT
- Mounts checkpoints in operation sandboxes
- Provides access via environment variables
- Handles both local and YT-stored checkpoints

**Key points:**

- Checkpoints are mounted as files in operation sandboxes
- Access via `CHECKPOINT_FILE` environment variable (dev mode) or `model_name` from config (prod mode)
- Supports local upload and YT-stored checkpoints
- Works in both dev and prod modes

## Quick Start

### Basic Setup

**Stage config** (`stages/inference/config.yaml`):

```yaml
job:
  model_name: my_model.pth

client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      checkpoint:
        checkpoint_base: //tmp/my_pipeline/checkpoints
        local_checkpoint_path: /path/to/local/my_model.pth
      resources:
        pool: default
        memory_limit_gb: 8
        cpu_limit: 2
```

**Mapper script** (`stages/inference/src/mapper.py`):

```python
import os
import torch
from ytjobs.config import get_config_path
from omegaconf import OmegaConf

def main():
    config = OmegaConf.load(get_config_path())
    model_name = config.job.model_name
    
    # Checkpoint file is available in sandbox
    checkpoint_file = os.environ.get("CHECKPOINT_FILE", model_name)
    
    # Load model from checkpoint
    model = torch.load(checkpoint_file)
    model.eval()
    
    # Process input rows...
```

## Configuration

### Checkpoint Configuration

Configure checkpoints in operation config:

```yaml
client:
  operations:
    map:
      checkpoint:
        checkpoint_base: //tmp/my_pipeline/checkpoints
        local_checkpoint_path: /path/to/local/model.pth
```

**Fields:**

- **`checkpoint_base`**: YT path where checkpoints are stored
- **`local_checkpoint_path`**: Local path to checkpoint file (for upload)

### Model Name

Specify model name in job config:

```yaml
job:
  model_name: my_model.pth
```

The model name is used as the checkpoint filename in YT.

## Checkpoint Initialization

### Automatic Initialization

The framework automatically initializes checkpoints:

1. Creates checkpoint directory in YT (if needed)
2. Uploads local checkpoint (if `local_checkpoint_path` is set)
3. Validates checkpoint exists before operation

### Manual Initialization

For more control, initialize checkpoints manually:

```python
from yt_framework.operations.checkpoint import init_checkpoint_directory

class InferenceStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Initialize checkpoint directory
        # Note: model_name is automatically read from context.config.job.model_name
        init_checkpoint_directory(
            context=self.context,
            checkpoint_config=self.config.client.operations.map.checkpoint,
        )
        
        # Run operation
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.map,
        )
        
        return debug
```

## Using Checkpoints

### In Mapper Script

Access checkpoint in mapper script:

```python
import os
import torch
from ytjobs.config import get_config_path
from omegaconf import OmegaConf

def main():
    config = OmegaConf.load(get_config_path())
    
    # Get checkpoint file path
    # In dev mode: CHECKPOINT_FILE env var is set automatically
    # In prod mode: use model_name from config (checkpoint is mounted with model_name as filename)
    checkpoint_file = os.environ.get("CHECKPOINT_FILE", config.job.model_name)
    
    # Load model
    model = torch.load(checkpoint_file)
    model.eval()
    
    # Process rows...
    for line in sys.stdin:
        row = json.loads(line)
        # Use model for inference...
```

### In Vanilla Script

Access checkpoint in vanilla script:

```python
import os
import torch
from ytjobs.config import get_config_path
from omegaconf import OmegaConf

def main():
    config = OmegaConf.load(get_config_path())
    
    # Get checkpoint file
    # In dev mode: CHECKPOINT_FILE env var is set automatically
    # In prod mode: use model_name from config (checkpoint is mounted with model_name as filename)
    checkpoint_file = os.environ.get("CHECKPOINT_FILE", config.job.model_name)
    
    # Load model
    model = torch.load(checkpoint_file)
    
    # Use model...
```

## Checkpoint Paths

### YT Path Structure

Checkpoints are stored in YT at:

```plaintext
{checkpoint_base}/{model_name}
```

**Example:**

```yaml
checkpoint_base: //tmp/my_pipeline/checkpoints
model_name: my_model.pth
```

Results in YT path: `//tmp/my_pipeline/checkpoints/my_model.pth`

### Local Path

For local upload, specify `local_checkpoint_path`:

```yaml
local_checkpoint_path: /home/user/models/my_model.pth
```

The framework uploads this file to YT before the operation runs.

## Dev Mode Behavior

In dev mode, checkpoints are handled locally:

1. **Local checkpoint path**: If `local_checkpoint_path` is set, file is copied to sandbox
2. **Checkpoint file**: Available in sandbox as `CHECKPOINT_FILE` environment variable (set automatically)
3. **No YT upload**: Checkpoints are not uploaded to YT in dev mode

## Prod Mode Behavior

In prod mode, checkpoints are handled on YT:

1. **Checkpoint upload**: Local checkpoint is uploaded to YT if `local_checkpoint_path` is set
2. **Checkpoint mounting**: Checkpoint file is mounted in operation sandbox with `model_name` as filename
3. **Access**: Use `model_name` from config (CHECKPOINT_FILE env var is not set in prod mode)
4. **File location**: Checkpoint file is available in sandbox root directory

**Example:**

```yaml
# Dev mode config
client:
  operations:
    map:
      checkpoint:
        checkpoint_base: //tmp/my_pipeline/checkpoints  # Not used in dev
        local_checkpoint_path: /path/to/local/model.pth  # Used in dev
```

The checkpoint file is copied to `.dev/sandbox_*/` directory.

## Complete Example

### Stage Configuration

```yaml
# stages/run_inference/config.yaml
job:
  model_name: gpt2_model.pth

client:
  operations:
    map:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      checkpoint:
        checkpoint_base: //tmp/my_pipeline/checkpoints
        local_checkpoint_path: /home/user/models/gpt2_model.pth
      resources:
        pool: default
        memory_limit_gb: 16
        cpu_limit: 4
```

### Stage Code

```python
# stages/run_inference/stage.py
from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.map import run_map

class RunInferenceStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.map,
        )
        
        if not success:
            raise RuntimeError("Inference failed")
        
        return debug
```

### Mapper Script

```python
# stages/run_inference/src/mapper.py
#!/usr/bin/env python3
import sys
import json
import os
import torch
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

def main():
    config = OmegaConf.load(get_config_path())
    
    # Get checkpoint file from environment
    checkpoint_file = os.environ.get("CHECKPOINT_FILE")
    if not checkpoint_file:
        checkpoint_file = config.job.model_name
    
    # Load model
    model = torch.load(checkpoint_file, map_location="cpu")
    model.eval()
    
    # Process input rows
    for line in sys.stdin:
        row = json.loads(line)
        
        # Run inference
        input_data = row["input"]
        with torch.no_grad():
            output = model(input_data)
        
        # Output result
        output_row = {
            "id": row["id"],
            "output": output.tolist(),
        }
        print(json.dumps(output_row), flush=True)

if __name__ == "__main__":
    main()
```

## Best Practices

### Checkpoint Organization

**Organize checkpoints by model:**

```plaintext
//tmp/my_pipeline/checkpoints/
├── model_v1.pth
├── model_v2.pth
└── model_v3.pth
```

**Use versioned names:**

```yaml
job:
  model_name: model_v2.pth
```

### Checkpoint Upload

**Upload once, use many times:**

- Upload checkpoint to YT once
- Reuse across multiple operations
- Don't re-upload if already exists

**Check before upload:**

The framework checks if checkpoint exists before uploading:

```python
# Framework automatically checks
if checkpoint_exists_in_yt:
    skip_upload()
else:
    upload_checkpoint()
```

### Memory Management

**Load checkpoints efficiently:**

```python
# Load to CPU first (for large models)
model = torch.load(checkpoint_file, map_location="cpu")

# Move to GPU if needed
if torch.cuda.is_available():
    model = model.cuda()
```

**Use model caching:**

```python
# Cache model in memory (for multiple rows)
model = load_model(checkpoint_file)

for line in sys.stdin:
    # Reuse same model instance
    process_row(model, line)
```

### Error Handling

**Handle missing checkpoints:**

```python
checkpoint_file = os.environ.get("CHECKPOINT_FILE")
if not checkpoint_file or not os.path.exists(checkpoint_file):
    raise FileNotFoundError("Checkpoint file not found")
```

**Validate checkpoint format:**

```python
try:
    model = torch.load(checkpoint_file)
except Exception as e:
    raise ValueError(f"Invalid checkpoint format: {e}")
```

## Advanced Topics

### Multiple Checkpoints

For operations requiring multiple checkpoints:

```yaml
job:
  model_name: model.pth
  tokenizer_name: tokenizer.pth

client:
  operations:
    map:
      checkpoint:
        checkpoint_base: //tmp/my_pipeline/checkpoints
        local_checkpoint_path: /path/to/model.pth
```

Access both in mapper:

```python
model_file = os.environ.get("CHECKPOINT_FILE")
tokenizer_file = config.job.tokenizer_name  # Manual access
```

### Checkpoint Versioning

**Use versioned checkpoint paths:**

```yaml
checkpoint_base: //tmp/my_pipeline/checkpoints/v2
model_name: model.pth
```

**Switch versions:**

```yaml
# Version 1
checkpoint_base: //tmp/my_pipeline/checkpoints/v1

# Version 2
checkpoint_base: //tmp/my_pipeline/checkpoints/v2
```

### Large Checkpoints

**For very large checkpoints:**

- Use YT file storage (not tables)
- Consider checkpoint compression
- Use streaming loading if possible
- Allocate sufficient memory

## Troubleshooting

### Issue: Checkpoint not found

- Verify `checkpoint_base` path exists
- Check `model_name` matches filename
- Verify checkpoint was uploaded
- Check YT permissions

### Issue: Checkpoint upload fails

- Check `local_checkpoint_path` exists
- Verify file permissions
- Check YT credentials
- Review upload logs

### Issue: Out of memory

- Increase `memory_limit_gb`
- Use CPU loading (`map_location="cpu"`)
- Consider model quantization
- Check checkpoint file size

### Issue: Checkpoint format error

- Verify checkpoint format (PyTorch, etc.)
- Check model loading code
- Review checkpoint creation process
- Test checkpoint loading locally

## Next Steps

- Learn about [Docker Support](docker.md) for custom environments
- Explore [Code Upload](code-upload.md) for code packaging
