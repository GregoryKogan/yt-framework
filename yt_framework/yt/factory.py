"""Factory that returns `YTDevClient` or `YTProdClient` based on pipeline mode."""

import logging
from pathlib import Path
from typing import Any, Literal

from yt_framework.yt.client_base import BaseYTClient
from yt_framework.yt.client_dev import YTDevClient
from yt_framework.yt.client_prod import YTProdClient


def create_yt_client(
    logger: logging.Logger | None = None,
    mode: Literal["prod", "dev"] | None = "dev",
    pipeline_dir: Path | str | None = None,
    secrets: dict[str, str] | None = None,
    pickling: dict[str, Any] | None = None,
) -> BaseYTClient:
    """Factory function to create appropriate YT client based on mode.

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
            msg = "secrets are required for prod mode"
            raise ValueError(msg)
        return YTProdClient(logger=_logger, secrets=secrets, pickling=pickling or {})
    _pipeline_dir: Path | None = None
    if pipeline_dir is not None:
        _pipeline_dir = (
            Path(pipeline_dir) if isinstance(pipeline_dir, str) else pipeline_dir
        )
    return YTDevClient(logger=_logger, pipeline_dir=_pipeline_dir)
