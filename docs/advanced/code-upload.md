# Code Upload

Understanding how code upload works is essential for debugging and optimizing your pipelines. This guide explains the code upload mechanism and how to configure it.

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

```plaintext
stages/
└── my_stage/
    ├── stage.py
    ├── config.yaml
    └── src/              # Code upload needed
        └── mapper.py
```

### Upload Process

1. **Code Packaging**: Creates `code.tar.gz` archive containing:
   - `yt_framework/` package
   - `ytjobs/` package
   - `stages/` directory with all stage code
   - Stage config files (`config.yaml`)
   - `requirements.txt` files (if present)

2. **Upload to YT**: Uploads archive to `build_folder`:

   ```plaintext
   //tmp/my_pipeline/build/code.tar.gz
   ```

3. **Wrapper Scripts**: Generates wrapper scripts for each operation in build folder root:

   ```plaintext
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

```plaintext
//tmp/my_pipeline/build/
└── code.tar.gz                                    # Code archive (contains everything)
```

The `code.tar.gz` archive contains (when extracted):

```plaintext
code.tar.gz (extracted contents)
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

The `code.tar.gz` archive contains:

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

```plaintext
code.tar.gz
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
tar -xzf code.tar.gz

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

```txt
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

## Build Code Directory

By default, code is uploaded from the pipeline directory. You can specify a custom code directory:

```yaml
# configs/config.yaml
pipeline:
  mode: "prod"
  build_folder: "//tmp/my_pipeline/build"
  build_code_dir: "/path/to/custom/code"  # Optional
```

**Use cases:**

- Monorepo with shared code
- Code in different location
- Custom code structure

**Note:** If `build_code_dir` is relative, it's resolved relative to pipeline directory.

## Dev Mode Behavior

In dev mode, code upload is skipped:

- Code runs directly from local filesystem
- No archive creation
- No upload to YT
- Faster iteration

**Dev mode execution:**

```plaintext
.dev/sandbox_input->output/
├── input.jsonl
├── code.tar.gz (extracted)
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

```plaintext
sandbox/
├── code.tar.gz (extracted)
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

## Debugging Code Upload

### Check Upload Status

Code upload happens automatically. Check logs for:

```plaintext
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
2. Download `code.tar.gz`
3. Extract and inspect contents

**In dev mode:**

1. Check `.build/build/` directory
2. Inspect `code.tar.gz` archive
3. Review wrapper scripts

## Best Practices

1. **Keep code organized**: Use `src/` directory structure
2. **Pin dependencies**: Use `requirements.txt` with versions
3. **Test locally**: Use dev mode for faster iteration
4. **Monitor uploads**: Check logs for upload status
5. **Optimize size**: Keep code archive small
6. **Version control**: Track code changes

## Advanced Topics

### Custom Code Structure

For custom code structures, use `build_code_dir`:

```yaml
pipeline:
  build_code_dir: "/path/to/shared/code"
```

This allows sharing code across pipelines or using monorepo structures.

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
