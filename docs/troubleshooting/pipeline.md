# Pipeline Issues

Common issues related to pipeline setup, stage configuration, and execution.

## Stage Configuration

### No enabled_stages found

**Error:**

```plaintext
ValueError: No enabled_stages found in stages config section.
```

**Solution:**
Add `enabled_stages` to pipeline config:

```yaml
stages:
  enabled_stages:
    - stage1
    - stage2
```

### Stage not found

**Error:**

```plaintext
ValueError: Unknown stage: my_stage. Available stages: [...]
```

**Solution:**

- Check stage name matches directory name
- Verify `stages/my_stage/stage.py` exists
- Ensure stage class inherits from `BaseStage`
- Check stage is registered (for `BasePipeline`)

### Config file not found

**Error:**

```plaintext
FileNotFoundError: Config file not found: configs/config.yaml
```

**Solution:**

- Verify config file exists at specified path
- Check file path is correct
- Use `--config` flag to specify different config

## Stage Execution

### Stage fails immediately

**Error:**

```plaintext
AttributeError: Stage setup failed
```

**Solution:**

- Check stage `__init__` method
- Verify dependencies are injected correctly
- Review stage setup code
- Check stage config file exists

### Stage context issues

**Error:**

```plaintext
KeyError: Context key not found
```

**Solution:**

- Verify previous stages set context values
- Check context key names match
- Ensure stages execute in correct order
- Review context passing between stages

## Pipeline Execution

### Pipeline won't start

**Error:**

```plaintext
ValueError: Pipeline initialization failed
```

**Solution:**

- Check pipeline directory exists
- Verify Python version (3.11+)
- Check all dependencies installed
- Review pipeline setup() method

### Code upload issues

**Error:**

```plaintext
ValueError: build_folder not found
```

**Solution:**

- Add `build_folder` to pipeline config
- Only required if stages have `src/` directory
- Verify YT path format (starts with `//`)

## See Also

- [Configuration Guide](../configuration/index.md) - Complete configuration reference
- [Pipelines and Stages](../pipelines-and-stages.md) - Understanding pipeline structure
- [Operation Issues](operations.md) - Operation-specific problems
