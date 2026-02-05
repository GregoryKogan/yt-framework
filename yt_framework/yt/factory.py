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
                - YT_PROXY: YTsaurus proxy URL
                - YT_TOKEN: YTsaurus authentication token
                
    Returns:
        BaseYTClient instance (YTProdClient or YTDevClient)
        
    Raises:
        ValueError: If secrets are required for prod mode but not provided.
        
    Example:
        >>> # Dev mode (local filesystem simulation)
        >>> client = create_yt_client(mode="dev", pipeline_dir=Path("."))
        >>> 
        >>> # Prod mode (real YT cluster)
        >>> secrets = {"YT_PROXY": "my-proxy", "YT_TOKEN": "my-token"}
        >>> client = create_yt_client(mode="prod", secrets=secrets)
    """
    _logger = logger or logging.getLogger(__name__)
    if mode == "prod":
        if secrets is None:
            raise ValueError("secrets are required for prod mode")
        return YTProdClient(logger=_logger, secrets=secrets)
    else:
        _pipeline_dir: Optional[Path] = None
        if pipeline_dir is not None:
            _pipeline_dir = (
                Path(pipeline_dir) if isinstance(pipeline_dir, str) else pipeline_dir
            )
        return YTDevClient(logger=_logger, pipeline_dir=_pipeline_dir)
