#!/usr/bin/env python3
"""
Simple Mapper
=============

This mapper is executed for each input row.
It reads from stdin and writes to stdout in JSON format.

The mapper processes each row by:
1. Transforming the text (uppercase + prefix)
2. Multiplying the value
3. Adding processing metadata
"""

import sys
import json
from omegaconf import OmegaConf
from ytjobs.config import get_config_path


def main():
    # Load configuration from config.yaml (uploaded with the code)
    config = OmegaConf.load(get_config_path())

    multiplier = config.job.multiplier
    prefix = config.job.prefix

    for line in sys.stdin:
        row = json.loads(line)

        output_row = {
            "id": row["id"],
            "original_text": row["text"],
            "processed_text": prefix + row["text"].upper(),
            "original_value": row["value"],
            "processed_value": row["value"] * multiplier,
        }

        print(json.dumps(output_row), flush=True)


if __name__ == "__main__":
    main()
