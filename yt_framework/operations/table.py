"""
Table Operations
================

Operations for working with YT tables.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from yt_framework.yt.client_base import BaseYTClient


def get_row_count(
    yt_client: BaseYTClient,
    table_path: str,
    logger: logging.Logger,
) -> int:
    """
    Get number of rows in a YT table.

    Args:
        yt_client: YT client instance
        table_path: YT table path
        logger: Logger instance

    Returns:
        Number of rows in table
    """
    logger.info(f"Reading row count from {table_path}")
    count = yt_client.row_count(table_path)
    logger.info(f"Table has {count} rows")
    return count


def read_table(
    yt_client: BaseYTClient,
    table_path: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Read rows from a YT table.

    Args:
        yt_client: YT client instance
        table_path: YT table path
        logger: Logger instance

    Returns:
        List of rows as dictionaries
    """
    logger.info(f"Reading results from {table_path}")

    results = list(yt_client.read_table(table_path))

    logger.info(f"Read {len(results)} rows")
    return results


def download_table(
    yt_client: BaseYTClient,
    table_path: str,
    output_file: Path,
    logger: logging.Logger,
) -> None:
    """
    Download YT table to local JSONL file.

    Args:
        yt_client: YT client instance
        table_path: YT table path
        output_file: Local file path for output
        logger: Logger instance

    Returns:
        None
    """
    logger.info(f"Downloading table {table_path} to {output_file}")

    rows = yt_client.read_table(table_path)

    with open(output_file, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    row_count = sum(1 for _ in open(output_file))
    logger.info(f"✓ Downloaded {row_count} rows → {output_file}")
