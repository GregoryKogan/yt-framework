"""YT Framework - YTsaurus pipeline framework."""

from importlib import metadata as importlib_metadata

try:
    # Keep version definition in a single source: the package metadata
    __version__ = importlib_metadata.version("yt_framework")
except importlib_metadata.PackageNotFoundError:  # pragma: no cover
    # Fallback for editable installs or when metadata isn't available
    __version__ = "0.0.0"
