"""Helpers to count rows, load rows into memory, and export tables to JSONL.

For low-level access, use ``BaseYTClient`` methods (``read_table``, ``row_count``, etc.)
directly on ``self.deps.yt_client``. These functions add logging and convenience.
"""

import json
import logging
from pathlib import Path
from typing import Any

from yt_framework.yt.client_base import BaseYTClient


def get_row_count(
    yt_client: BaseYTClient,
    table_path: str,
    logger: logging.Logger,
) -> int:
    """Get number of rows in a YT table.

    Args:
        yt_client: YT client instance
        table_path: YT table path
        logger: Logger instance

    Returns:
        Number of rows in table

    """
    logger.info("Reading row count from %s", table_path)
    count = yt_client.row_count(table_path)
    logger.info("Table has %s rows", count)
    return count


def read_table(
    yt_client: BaseYTClient,
    table_path: str,
    logger: logging.Logger,
) -> list[dict[str, Any]]:
    """Read rows from a YT table.

    Args:
        yt_client: YT client instance
        table_path: YT table path
        logger: Logger instance

    Returns:
        List of rows as dictionaries

    """
    logger.info("Reading results from %s", table_path)

    results = list(yt_client.read_table(table_path))

    logger.info("Read %s rows", len(results))
    return results


def download_table(
    yt_client: BaseYTClient,
    table_path: str,
    output_file: Path,
    logger: logging.Logger,
) -> None:
    """Download YT table to local JSONL file.

    Args:
        yt_client: YT client instance
        table_path: YT table path
        output_file: Local file path for output
        logger: Logger instance

    Returns:
        None

    """
    logger.info("Downloading table %s to %s", table_path, output_file)

    rows = yt_client.read_table(table_path)

    with open(output_file, "w") as f:
        f.writelines(json.dumps(row) + "\n" for row in rows)

    row_count = sum(1 for _ in open(output_file))
    logger.info("✓ Downloaded %s rows → %s", row_count, output_file)
