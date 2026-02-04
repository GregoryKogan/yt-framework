"""
Job Configuration Utilities
============================

Configuration loading for YT jobs - using OmegaConf.
"""

import os
from pathlib import Path


def get_config_path() -> Path:
    """
    Get the path to the job configuration file.

    Returns:
        Path object pointing to the config file

    Raises:
        ValueError: If JOB_CONFIG_PATH environment variable is not set
    """
    job_config_path = os.environ.get("JOB_CONFIG_PATH")
    if job_config_path:
        return Path(job_config_path)
    else:
        raise ValueError("JOB_CONFIG_PATH environment variable is not set")


__all__ = ["get_config_path"]
