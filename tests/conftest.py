"""Pytest hooks shared across the test suite."""

from __future__ import annotations

import os
from pathlib import Path

_TESTS_DIR = Path(__file__).resolve().parent


def pytest_ignore_collect(
    collection_path: Path,
    path=None,  # legacy pytest arg; unused
    config=None,
) -> bool | None:
    """Skip real-cluster tests in CI or without YT credentials."""
    try:
        rel = collection_path.resolve().relative_to(_TESTS_DIR)
    except ValueError:
        return None
    parts = rel.parts
    if len(parts) >= 2 and parts[0] == "integration" and parts[1] == "yt_cluster":
        # GitHub Actions, GitLab CI, and others set CI=true; never hit the cell from CI.
        if os.environ.get("CI", "").lower() in ("true", "1"):
            return True
        from integration.yt_cluster._cluster_config import is_cluster_config_ready

        if not is_cluster_config_ready():
            return True
    return None
