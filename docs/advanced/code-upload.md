# Code Upload

This page describes how the framework packs `stages/<name>/src`, dependency metadata, and optional extras into the archive YT runs in prod, and how to tune `upload_modules` / `upload_paths`.

## Overview

When stages have `src/` directories (for map or vanilla operations), the framework automatically packages and uploads code to the YT cluster. This process happens transparently, but understanding it helps with debugging and optimization.

**Key points:**

- Code is packaged into a tar archive
- Archive is uploaded to YT build folder
- Wrapper scripts extract and execute code
- Only happens when stages need code execution

## How It Works

### Automatic Detection

The framework automatically detects if code upload is needed:

1. Checks if any enabled stages have `src/` directory
2. Looks for `mapper.py` or `vanilla.py` files
3. Only uploads if code execution is needed

**Example:**

```text
stages/
└── my_stage/
    ├── stage.py
    ├── config.yaml
    └── src/              # Code upload needed
        └── mapper.py
```

### Upload Process

1. **Code Packaging**: Creates `source.tar.gz` archive containing:
   - `yt_framework/` package
   - `ytjobs/` package
   - `stages/` directory with all stage code
   - Stage config files (`config.yaml`)
   - `requirements.txt` files (if present)

2. **Upload to YT**: Uploads archive to `build_folder`:

   ```text
   //tmp/my_pipeline/build/source.tar.gz
   ```

3. **Wrapper Scripts**: Generates wrapper scripts for each operation in build folder root:

   ```text
   //tmp/my_pipeline/build/operation_wrapper_my_stage_map.sh
   //tmp/my_pipeline/build/operation_wrapper_my_stage_vanilla.sh
   ```

4. **Execution**: Operations extract archive and run wrapper scripts

## Build Folder

The build folder is where code is uploaded on the YT cluster.

### Configuration

Set build folder in pipeline config:

```yaml
# configs/config.yaml
pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"
```

**Required for:** Stages with `src/` directory (map or vanilla operations)

**Not required for:** Stages that only use YT client operations (YQL, table operations)

### Build Folder Structure

After upload, build folder contains:

```text
//tmp/my_pipeline/build/
└── source.tar.gz                                    # Code archive (contains everything)
```

The `source.tar.gz` archive contains (when extracted):

```text
source.tar.gz (extracted contents)
├── ytjobs/                                        # YT jobs package
├── stages/
│   └── my_stage/
│       ├── config.yaml                            # Stage config
│       ├── requirements.txt                       # Dependencies (if present)
│       └── src/
│           └── mapper.py                          # Mapper script
└── operation_wrapper_my_stage_map.sh              # Wrapper script (in archive root)
```

Note: Wrapper scripts are in the archive root, not in `stages/` subdirectories.

### Custom Upload Modules and Paths

You can upload additional packages beyond the implicit `ytjobs` package:

```yaml
# configs/config.yaml
pipeline:
  build_folder: "//tmp/my_pipeline/build"
  upload_modules: [my_package_1, my_package_2]   # Import by module name
  upload_paths:                                  # Or by local path
    - { source: "./lib/ad_hoc", target: "ad_hoc" }
    - { source: "./shared_utils" }               # target defaults to "shared_utils"
```

**`upload_modules`** (optional): List of Python module/package names. Each module is resolved via `import`, and its directory is copied into the archive. Modules must be importable (installed in your environment).

**`upload_paths`** (optional): List of dicts with:

- **`source`** (required): Path relative to pipeline directory (or absolute, but must resolve within it)
- **`target`** (optional): Directory name in the archive. Defaults to the last component of `source` (e.g., `./lib/ad_hoc` → `ad_hoc`)

**Implicit:** The `ytjobs` package is always uploaded; you do not need to list it.

**Path resolution:** All paths in `upload_paths` are resolved relative to the pipeline directory. **Path containment:** The resolved path must stay within the pipeline directory—paths that escape (e.g., `../other_dir`) are rejected with a clear error.

**`.ytignore`:** Applied to all upload sources (ytjobs, upload_modules, upload_paths). Place `.ytignore` in the source directory to exclude files.

**Reserved targets:** `stages` and `ytjobs` cannot be used as target names.

**Target conflicts:** If two sources map to the same target (e.g., `upload_modules: [my_utils]` and `upload_paths: [{ source: "./lib/my_utils", target: "my_utils" }]`), the framework raises an error.

## Code Archive Contents

The `source.tar.gz` archive contains:

### Framework Packages

- **`ytjobs/`**: YT jobs utilities package (always uploaded, read-only)

### Custom Packages (Optional)

- **`<module_name>/`**: Packages from `upload_modules`
- **`<target>/`**: Directories from `upload_paths`

### Stage Code

- **`stages/<stage_name>/src/`**: Stage source code
  - `mapper.py` (for map operations)
  - `vanilla.py` (for vanilla operations)
  - Other Python files

### Configuration Files

- **`stages/<stage_name>/config.yaml`**: Stage configuration

### Map Operation Dependencies

- **`stages/<stage_name>/requirements.txt`**: Python dependencies (if present)

### Archive Structure

```text
source.tar.gz
├── ytjobs/
│   └── ...
├── my_package_1/          # From upload_modules (if configured)
├── ad_hoc/                # From upload_paths (if configured)
└── stages/
    └── my_stage/
        ├── config.yaml
        ├── requirements.txt
        └── src/
            └── mapper.py
```

## Wrapper Scripts

Wrapper scripts handle code extraction and execution.

### Map Operation Wrapper

```bash
#!/bin/bash
set -e

# Extract code archive
tar -xzf source.tar.gz

# Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Set config path
export JOB_CONFIG_PATH="$(pwd)/stages/my_stage/config.yaml"

# Install requirements if present
if [ -f "stages/my_stage/requirements.txt" ]; then
    pip install --quiet --no-cache-dir -r stages/my_stage/requirements.txt
fi

# Execute mapper
python3 stages/my_stage/src/mapper.py
```

### Vanilla Operation Wrapper

Similar structure but executes `vanilla.py` instead of `mapper.py`.

## Vanilla Operation Dependencies

### What Gets Uploaded

**Always uploaded:**

- `ytjobs/` package
- Stage source code (`src/`)
- Stage config files

**Conditionally uploaded:**

- `requirements.txt` (if present in stage directory)
- Packages from `upload_modules` (if configured)
- Directories from `upload_paths` (if configured)

### Requirements.txt

If a stage has `requirements.txt`, dependencies are installed during operation execution:

```text
# stages/my_stage/requirements.txt
numpy>=1.20.0
pandas>=1.3.0
transformers>=4.20.0
```

**Installation happens:**

- During operation execution (not during upload)
- In the operation sandbox
- Using `pip install --quiet --no-cache-dir`

**Best practices:**

- Pin versions for reproducibility
- Only include necessary dependencies
- Keep file size reasonable

## Local Build Directory

Code is assembled under the pipeline directory in a local `.build` directory before being packed into `source.tar.gz` and uploaded. The `.build` directory is cleaned at the start of each upload run so its contents always match the current config (`upload_modules`, `upload_paths`, and stages).

## Dev Mode Behavior

In dev mode, code is still built locally (`.build/` and `source.tar.gz` are created) but not uploaded to YT:

- Code runs directly from local filesystem
- Archive is created locally in `.build/source.tar.gz`
- No upload to YT (upload is a no-op in dev)

- Faster iteration

**Dev mode execution:**

```text
.dev/sandbox_input->output/
├── input.jsonl
├── source.tar.gz (extracted)
└── operation_wrapper_*.sh
```

Code is still packaged (for consistency) but runs locally.

## Leaky Abstractions

While the framework tries to abstract code upload, some details leak through:

### File Paths

**In mapper.py or vanilla.py:**

```python
# Config path is set automatically
from ytjobs.config import get_config_path
config_path = get_config_path()
# Returns: /path/to/sandbox/stages/my_stage/config.yaml
```

**What to know:**

- Config path is absolute in sandbox
- Don't hardcode paths
- Use `get_config_path()` helper

### PYTHONPATH

**PYTHONPATH is set automatically:**

```python
# These imports work automatically
from ytjobs.config import get_config_path
from ytjobs.logging.logger import get_logger
```

**What to know:**

- Framework packages are in PYTHONPATH
- Stage code is in PYTHONPATH
- Don't modify PYTHONPATH manually

### Sandbox Structure

**Code runs in sandbox:**

```text
sandbox/
├── source.tar.gz (extracted)
├── stages/
│   └── my_stage/
│       ├── config.yaml
│       └── src/
│           └── mapper.py
└── input.jsonl (for map operations)
```

**What to know:**

- Current directory is sandbox root
- Input files are in sandbox
- Output goes to stdout (for map operations)

## Programmatic API

Custom tooling or framework extensions can call the same helpers the pipeline uses:

- **Upload / archive**: `upload_all_code`, `build_code_locally`, `create_code_archive`, `upload_code_archive` in `yt_framework.operations.upload`.
- **Dependency file lists**: `build_stage_dependencies`, `build_ytjobs_dependencies`, `build_map_dependencies`, `add_checkpoint` in `yt_framework.operations.dependencies`.

Autodoc and signatures: [API Reference](../reference/api.md) (sections **Upload and local build**, **Stage dependency file lists**, and **Shared operation utilities**).

## Debugging Code Upload

### Check Upload Status

Code upload happens automatically. Check logs for:

```text
[Upload] Packaging code...
[Upload] Uploading code to //tmp/my_pipeline/build...
[Upload] Code uploaded successfully
```

### Common Issues

### Issue: Code not updating

- Code is uploaded once per pipeline run
- Changes require re-running pipeline
- Check `build_folder` is correct

### Issue: Import errors

- Verify `yt_framework` and `ytjobs` are installed
- Check PYTHONPATH is set correctly
- Review wrapper script logs

### Issue: Config not found

- Verify `config.yaml` exists in stage directory
- Check config path in logs
- Review wrapper script

### Issue: Requirements not installing

- Verify `requirements.txt` exists
- Check file format is correct
- Review installation logs

### Inspecting Uploaded Code

**In YT web UI:**

1. Navigate to build folder: `//tmp/my_pipeline/build`
2. Download `source.tar.gz`
3. Extract and inspect contents

**In dev mode:**

1. Check `.build/source/` directory
2. Inspect `source.tar.gz` archive
3. Review wrapper scripts

## Best Practices

1. **Keep code organized**: Use `src/` directory structure
2. **Pin dependencies**: Use `requirements.txt` with versions
3. **Test locally**: Use dev mode for faster iteration
4. **Monitor uploads**: Check logs for upload status
5. **Optimize size**: Keep code archive small
6. **Version control**: Track code changes

## Advanced Topics

### Code Upload Optimization

**Minimize upload size:**

- Only include necessary files
- Use `.ytignore` to exclude files
- Avoid large data files in code

**Faster uploads:**

- Use local build folder for testing
- Cache dependencies in Docker image
- Minimize code changes between runs

## Next Steps

- Learn about [Docker Support](docker.md) for custom environments
- Explore [Checkpoints](checkpoints.md) for model files
- Check out [Examples](https://github.com/GregoryKogan/yt-framework/tree/main/examples/) for code upload patterns
