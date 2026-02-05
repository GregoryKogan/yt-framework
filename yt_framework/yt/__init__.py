"""YT client implementation for YTsaurus operations.

This module provides client implementations for interacting with YTsaurus clusters.
It includes both production (real YT cluster) and development (local filesystem simulation)
clients that implement the same interface.

Example:
    >>> from yt_framework.yt import create_yt_client
    >>> client = create_yt_client(mode="dev", pipeline_dir=Path("."))
    >>> client.write_table("//tmp/test", [{"id": 1, "value": "hello"}])
"""

from .factory import create_yt_client
from .client_base import BaseYTClient

__all__ = ["create_yt_client", "BaseYTClient"]
