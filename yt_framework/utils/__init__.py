"""General utilities for YT Framework.

This module provides utility functions for:
- Logging: Setup and formatting of loggers with colored output
- Environment: Loading environment variables and secrets from .env files
- Ignore patterns: Matching files against .ytignore patterns

Example:
    >>> from yt_framework.utils import setup_logging, load_secrets
    >>> logger = setup_logging()
    >>> secrets = load_secrets(Path("configs"))
"""

from .logging import setup_logging, log_config, log_header, log_operation, log_success
from .ignore import YTIgnoreMatcher, YTIgnorePattern
from .env import load_env_file, load_secrets

__all__ = [
    "setup_logging",
    "log_config",
    "log_header",
    "log_operation",
    "log_success",
    "YTIgnoreMatcher",
    "YTIgnorePattern",
    "load_env_file",
    "load_secrets",
]
