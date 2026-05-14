"""Filesystem layout for installed packages (no reliance on root ``__init__`` exports)."""

from __future__ import annotations

from importlib import resources
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def distribution_site_root(package_name: str) -> Path:
    """Return the parent of the on-disk package directory for ``package_name``.

    That parent is typically on ``sys.path`` so subprocesses can import the
    package when it is appended to ``PYTHONPATH``.

    Args:
        package_name: Top-level distribution name (for example ``yt_framework``).

    """
    with resources.as_file(resources.files(package_name)) as pkg_dir:
        return pkg_dir.resolve().parent
