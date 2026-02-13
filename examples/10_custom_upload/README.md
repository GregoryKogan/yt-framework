# Custom Upload Example

Demonstrates the `upload_paths` and `upload_modules` configuration for uploading custom local packages to YT alongside the implicit `ytjobs` package.

## What It Demonstrates

- **upload_paths**: Uploading a local directory (`lib/my_utils`) to the job sandbox
- **Custom packages**: Using a custom package in vanilla operation code
- **Import from custom module**: `from my_utils.helpers import greet` in stage code

## Features

- Local package in `lib/my_utils/` with `__init__.py` and `helpers.py`
- Pipeline config with `upload_paths: [{ source: "./lib/my_utils", target: "my_utils" }]`
- Vanilla stage that imports and uses the custom `greet()` function

## Configuration

```yaml
# configs/config.yaml
pipeline:
  upload_paths:
    - { source: "./lib/my_utils", target: "my_utils" }
```

The `source` path is relative to the pipeline directory. The `target` sets the directory name in the archive (and thus the import name). Omit `target` to use the last path component (e.g., `./lib/my_utils` defaults to `my_utils`).

## Running

```bash
python pipeline.py
```

Executes the vanilla operation which imports `my_utils.helpers.greet` and logs the result.

## Files

- `pipeline.py`: Pipeline entry point
- `lib/my_utils/`: Custom package to upload
- `stages/use_custom/`: Vanilla stage using the custom module
- `configs/config.yaml`: Pipeline config with `upload_paths`

## Key Concepts

- **upload_paths**: For local directories (relative to pipeline_dir)
- **upload_modules**: For installed Python packages (import by name)
- **ytjobs**: Always uploaded implicitly - no need to list it
- **.ytignore**: Applied to all upload sources

## Next Steps

- See [Code Upload](https://yt-framework.readthedocs.io/en/latest/advanced/code-upload.html) for full documentation
- Try `upload_modules: [some_package]` for installed packages
- Add `upload_paths` entries without `target` to use default naming
