# Vanilla example

Single `run_vanilla` call executes `src/vanilla.py` once. No table stdin/stdout contract—use this pattern for bootstrap scripts, probes, or logging.

## Run

```bash
python pipeline.py
```

## Files

- `stages/run_vanilla/stage.py` — calls `run_vanilla`
- `stages/run_vanilla/src/vanilla.py` — entry script
- `stages/run_vanilla/config.yaml` — resources

## Notes

- Job code reads YAML via `ytjobs.config.get_config_path()`.
- Combine with map stages when part of the flow needs table I/O.

## Next

- [04_map_operation](../04_map_operation/)
- [environment_log](../environment_log/)
- [09_multiple_operations](../09_multiple_operations/)
