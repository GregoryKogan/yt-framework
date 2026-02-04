#!/usr/bin/env python3

import json
import sys
from omegaconf import OmegaConf
from ytjobs.config import get_config_path


def main():
    config = OmegaConf.load(get_config_path())

    multiplier = config.job.multiplier
    prefix = config.job.prefix

    for line in sys.stdin:
        row = json.loads(line)
        output_row = {
            "id": row["id"],
            "processed_value": row["value"] * multiplier,
            "processed_text": prefix + row.get("text", ""),
        }
        print(json.dumps(output_row), flush=True)


if __name__ == "__main__":
    main()
