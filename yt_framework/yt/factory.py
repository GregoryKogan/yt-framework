"""
YTsaurus Operations
===================

Reusable YT client for any YT operations.
"""

import logging
from pathlib import Path
from typing import Dict, Literal, Optional, Union

from yt_framework.yt.client_base import BaseYTClient
from yt_framework.yt.client_prod import YTProdClient
from yt_framework.yt.client_dev import YTDevClient


def create_yt_client(
    logger: Optional[logging.Logger] = None,
    mode: Optional[Literal["prod", "dev"]] = "dev",
    pipeline_dir: Optional[Union[Path, str]] = None,
    secrets: Optional[Dict[str, str]] = None,
) -> BaseYTClient:
    """
    Factory function to create appropriate YT client based on mode.

    Args:
        logger: Logger instance (default: creates new logger)
        mode: "prod" for production YT client, "dev" for local development
        pipeline_dir: Pipeline directory (required for dev mode)
        secrets: Optional dictionary containing YT credentials. Required only for prod mode.
                Expected keys:
                - YT_PROXY
                - YT_TOKEN
    Returns:
        BaseYTClient instance (YTProdClient or YTDevClient)
    """
    _logger = logger or logging.getLogger(__name__)
    if mode == "prod":
        if secrets is None:
            raise ValueError("secrets are required for prod mode")
        return YTProdClient(logger=_logger, secrets=secrets)
    else:
        _pipeline_dir: Optional[Path] = None
        if pipeline_dir is not None:
            _pipeline_dir = Path(pipeline_dir) if isinstance(pipeline_dir, str) else pipeline_dir
        return YTDevClient(logger=_logger, pipeline_dir=_pipeline_dir)
