"""
S3 Operations
=============

Operations for working with S3 data in pipelines.
"""

import logging
from typing import List, Optional

from yt_framework.yt.client_base import BaseYTClient
from ytjobs.s3.client import S3Client


def list_s3_files(
    s3_client: S3Client,
    bucket: str,
    prefix: str,
    logger: logging.Logger,
    extension: Optional[str] = None,
    max_files: Optional[int] = None,
) -> List[str]:
    """
    List files from S3 bucket with optional filtering.

    Args:
        s3_client: S3 client instance
        bucket: S3 bucket name
        prefix: S3 prefix path
        logger: Logger instance
        extension: Optional file extension filter (e.g., 'mp4')
        max_files: Optional maximum number of files to return

    Returns:
        List of S3 file paths
    """
    logger.info(f"Listing files from S3: s3://{bucket}/{prefix}")

    paths = s3_client.list_files(
        bucket=bucket,
        prefix=prefix,
        extension=extension,
        max_files=max_files,
    )

    logger.info(f"Found {len(paths)} files")

    if paths:
        logger.debug("Sample paths:")
        for path in paths[:3]:
            logger.debug(f"  - {path}")
        if len(paths) > 3:
            logger.debug(f"  ... and {len(paths) - 3} more")

    return paths


def save_s3_paths_to_table(
    yt_client: BaseYTClient,
    bucket: str,
    paths: List[str],
    output_table: str,
    logger: logging.Logger,
) -> None:
    """
    Save S3 file paths to YT table as bucket and path columns.

    Args:
        yt_client: YT client instance
        bucket: S3 bucket name
        paths: List of S3 file paths
        output_table: YT table path
        logger: Logger instance
    """
    logger.info(f"Saving {len(paths)} paths to YT table: {output_table}")

    # Convert paths to table rows
    rows = [{"bucket": bucket, "path": path} for path in paths]

    yt_client.write_table(table_path=output_table, rows=rows, append=False)

    logger.info(f"✓ Saved {len(rows)} paths → {output_table}")
