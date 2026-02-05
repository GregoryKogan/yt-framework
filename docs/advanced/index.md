# Advanced Topics

Advanced features and techniques for complex YT Framework pipelines.

## Advanced Features

```{card-grid} 2
:padding: 2

**Code Upload**
^^^
Learn how the framework handles code packaging and deployment to YT cluster. Understand the tar archive strategy and dependency management.

+++
[Learn More →](code-upload.md)

**Docker Support**
^^^
Use custom Docker images for GPU workloads or special dependencies. Configure Docker authentication and multi-stage builds.

+++
[Learn More →](docker.md)

**Checkpoint Management**
^^^
Handle ML model checkpoints for inference pipelines. Upload, version, and manage checkpoints in YT.

+++
[Learn More →](checkpoints.md)

**Multiple Operations**
^^^
Run multiple operations (map, vanilla, YQL) in a single stage. Optimize pipeline execution and reduce overhead.

+++
[Learn More →](multiple-operations.md)
```

## Prerequisites

Before diving into advanced topics, make sure you understand:

- [Pipelines and Stages](../pipelines-and-stages.md) - Basic pipeline structure
- [Configuration](../configuration/index.md) - Configuration system
- [Operations](../operations/) - Basic operation types

## When to Use Advanced Features

### Code Upload

Use when:
- Your stages have `src/` directories with Python code
- You need to deploy custom dependencies
- Working with complex codebases

### Docker Support

Use when:
- You need GPU acceleration
- Requiring specific system dependencies
- Using custom runtime environments

### Checkpoint Management

Use when:
- Running ML inference pipelines
- Need to version model checkpoints
- Managing large model files

### Multiple Operations

Use when:
- Combining multiple operations in one stage
- Reducing pipeline overhead
- Optimizing execution flow

## See Also

- [Code Upload Guide](code-upload.md) - Complete code upload documentation
- [Docker Guide](docker.md) - Docker configuration guide
- [Checkpoints Guide](checkpoints.md) - Checkpoint management guide
- [Multiple Operations Guide](multiple-operations.md) - Running multiple operations
