# Multiple config files

Same repo, different `configs/*.yaml` selected with `--config`. Handy for dev vs prod table sizes or resource blocks without editing the default file.

## Run

```bash
python pipeline.py
python pipeline.py --config configs/config_custom.yaml
python pipeline.py --config configs/config_large.yaml
```

## Files

- `configs/config.yaml` — default
- `configs/config_custom.yaml` / `configs/config_large.yaml` — alternates
- `stages/process_data/` — single stage exercised by each file

## See also

- [Advanced configuration](../../docs/configuration/advanced.md)
- [Dev vs prod](../../docs/dev-vs-prod.md)
