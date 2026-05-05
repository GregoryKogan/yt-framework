# Environment logging

Vanilla script prints structured host facts (GPU/CUDA when present, Python packages, disk, etc.) to help compare sandbox vs laptop.

## Run

```bash
python pipeline.py
```

Inspect stdout/stderr in dev under `.dev/` or in the YT operation UI in prod.

## Files

- `stages/logenv/src/vanilla.py` — logging script
- `stages/logenv/config.yaml` — resources

## When to use

Capture proof of image contents, driver versions, and config snippets (masked) when debugging “works on my machine” deltas.

## Next

- [05_vanilla_operation](../05_vanilla_operation/)
- [Troubleshooting](../../docs/troubleshooting/index.md)
