# Map example

`create_input` writes a small JSONL-backed table; `run_map` streams it through `src/mapper.py` (stdin/stdout JSON lines). Switch `pipeline.mode` to `prod` when you want real cluster jobs and upload.

## Run

```bash
python pipeline.py
```

## Files

- `stages/create_input/` — synthetic input table
- `stages/run_map/` — `run_map` + `src/mapper.py`
- `configs/config.yaml` — mode + `build_folder` for prod

## Mapper shape

```python
import json, sys
from omegaconf import OmegaConf
from ytjobs.config import get_config_path

def main():
    config = OmegaConf.load(get_config_path())
    for line in sys.stdin:
        row = json.loads(line)
        out = {"id": row["id"], "doubled": row.get("value", 0) * 2}
        print(json.dumps(out), flush=True)

if __name__ == "__main__":
    main()
```

Config inside the mapper comes from `get_config_path()` (same pattern as production jobs).

## Next

- [05_vanilla_operation](../05_vanilla_operation/)
- [video_gpu](../video_gpu/) for GPU images
