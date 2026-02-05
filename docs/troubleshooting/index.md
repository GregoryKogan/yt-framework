# Troubleshooting

Common issues and solutions for YT Framework pipelines.

This troubleshooting guide is organized by category to help you quickly find solutions to common problems.

```{toctree}
:maxdepth: 1

pipeline
operations
configuration
debugging
```

## Quick Navigation

- [Pipeline Issues](pipeline.md) - Problems with pipeline setup, stages, and configuration
- [Operation Issues](operations.md) - Issues with Map, Vanilla, YQL, S3, Docker, and Checkpoint operations
- [Configuration Issues](configuration.md) - Dev mode and Prod mode specific problems
- [Debugging Guide](debugging.md) - Debugging tips, getting help, and prevention strategies

## Common Error Patterns

Most issues fall into these categories:

1. **Configuration Errors**: Missing or incorrect configuration values
2. **File/Path Errors**: Missing files, incorrect paths, permission issues
3. **Operation Errors**: Failed map/vanilla/YQL operations
4. **Environment Errors**: Missing dependencies, wrong Python version, etc.
5. **Mode-Specific Errors**: Issues that only occur in dev or prod mode

## Quick Fixes

### Pipeline won't start
- Check `enabled_stages` in config
- Verify config file exists
- Check Python version (3.11+)

### Operation fails
- Test in dev mode first
- Check operation logs
- Verify input tables exist
- Review resource limits

### Can't find files/tables
- Verify paths are correct
- Check file permissions
- Ensure previous stages completed successfully

## Getting Started

If you're new to troubleshooting:

1. Start with [Pipeline Issues](pipeline.md) if your pipeline won't run
2. Check [Configuration Issues](configuration.md) for mode-specific problems
3. Review [Operation Issues](operations.md) for operation-specific errors
4. Use [Debugging Guide](debugging.md) for advanced troubleshooting

## See Also

- [Configuration Guide](../configuration/index.md) - Complete configuration reference
- [Dev vs Prod](../dev-vs-prod.md) - Understanding execution modes
- [Operations Guide](../operations/) - Operation-specific documentation
