"""Upload local job files to Cypress for cluster integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def upload_job_dep(
    yt_client: object,
    yt_case_prefix: str,
    local_file: Path,
    job_filename: str,
) -> tuple[str, str]:
    """Upload a local file; return ``(absolute_yt_path, sandbox_file_name)`` for ``FilePath``."""
    yt_path = f"{yt_case_prefix}/_job_deps/{job_filename}"
    yt_client.upload_file(local_file, yt_path, create_parent_dir=True)
    return (yt_path, job_filename)
