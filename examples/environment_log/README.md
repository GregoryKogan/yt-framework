# Environment Log Example

Demonstrates comprehensive environment logging for debugging and reproducibility. Shows how to log system, hardware, and software information.

## What It Demonstrates

- **Environment Logging**: Logging comprehensive environment information
- **System Information**: CPU, memory, disk, network details
- **Software Versions**: Python, libraries, frameworks
- **GPU Information**: CUDA, GPU details, PyTorch CUDA support
- **Configuration Logging**: Logging configuration values

## Features

- Comprehensive environment logging
- System and hardware information
- Software version tracking
- GPU and CUDA information
- Configuration value logging
- File structure logging

## Running

```bash
python pipeline.py
```

Logs comprehensive environment information to operation logs.

## Files

- `pipeline.py`: Pipeline entry point
- `stages/logenv/stage.py`: Stage that runs environment logging
- `stages/logenv/src/vanilla.py`: Comprehensive logging script
- `stages/logenv/config.yaml`: Stage configuration
- `configs/config.yaml`: Pipeline configuration

## Key Concepts

- Environment logging helps with debugging and reproducibility
- Logs system, hardware, software, and configuration information
- Useful for troubleshooting environment-related issues
- Can be run as vanilla operation

## Logged Information

1. **GPU & CUDA**: nvidia-smi, CUDA version, PyTorch CUDA support
2. **Python Environment**: Python version, packages, virtual environment
3. **System Information**: OS, CPU, memory, disk
4. **Network**: Network interfaces, DNS, connectivity
5. **File Structure**: Directory structure and file sizes
6. **Software Versions**: Installed software versions
7. **Process Information**: User, groups, resource limits
8. **Container Information**: Container/sandbox details
9. **Deep Learning Frameworks**: PyTorch, TensorFlow, JAX versions
10. **Configuration Values**: Logged configuration (with masking)

## Use Cases

- Debugging environment issues
- Reproducibility documentation
- Environment validation
- Troubleshooting setup problems

## Next Steps

- See [05_vanilla_operation](../05_vanilla_operation/) for vanilla operations
- See [Troubleshooting Guide](../../docs/troubleshooting.md) for debugging tips
