# Custom upload paths

Pipeline-level `upload_paths` packs `lib/my_utils/` next to `ytjobs` so `vanilla.py` can `import my_utils.helpers`.

## Config

```yaml
pipeline:
  upload_paths:
    - { source: "./lib/my_utils", target: "my_utils" }
```

`source` is relative to the pipeline root. `target` is the directory name inside the archive (defaults to the final path segment if omitted).

## Run

```bash
python pipeline.py
```

## Files

- `lib/my_utils/` — sample package
- `stages/use_custom/` — vanilla importing `greet()`
- `.ytignore` still applies to uploaded trees

## See also

- [Code upload](https://yt-framework.readthedocs.io/en/latest/advanced/code-upload.html)
- `upload_modules` for installed distribution names instead of raw folders
