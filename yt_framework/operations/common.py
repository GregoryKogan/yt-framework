"""
Common Operations Utilities
============================

Shared utility functions used by multiple operation types.
"""

import logging
from typing import Dict, Optional, Any

from omegaconf import DictConfig
from yt_framework.utils.env import load_secrets


def _construct_command(script_path: str) -> str:
    """Construct command from script path."""
    if script_path.endswith('.sh'):
        return f"bash {script_path}"
    else:
        return f"python3 {script_path}"


def _get_config_value_with_default(
    config: DictConfig,
    key: str,
    default: Any,
    logger: logging.Logger,
) -> Any:
    """
    Get config value with default, logging when default is used.
    
    Args:
        config: OmegaConf DictConfig object
        key: Config key to access (supports dot notation like "client.pool")
        default: Default value to use if key is missing or None
        logger: Logger instance for logging defaults
        
    Returns:
        Config value if present and not None, otherwise default
    """
    try:
        # Check if key exists in config
        if key not in config:
            logger.info(f"  Using default {key}={default} (not specified in config)")
            return default
        
        value = config.get(key)
        # If value is None, use default and log
        if value is None:
            logger.info(f"  Using default {key}={default} (value is None in config)")
            return default
        
        return value
    except Exception:
        # Key doesn't exist or access failed, use default
        logger.info(f"  Using default {key}={default} (not specified in config)")
        return default


def build_environment(
    configs_dir,
    logger: logging.Logger,
) -> Dict[str, str]:
    """
    Build environment variables for map operations.

    Jobs read configuration from config.yaml, so only secrets are passed
    via environment variables.

    Args:
        configs_dir: Directory containing secrets.env file
        logger: Logger instance

    Returns:
        Dictionary of secret environment variables
    """
    # Get all secrets loaded from secrets.env file
    logger.debug("Building environment with secrets...")
    env = load_secrets(configs_dir)

    # Log secret keys (mask values)
    for key, value in env.items():
        logger.debug(f"  {key}: {'*' * min(len(value), 10)}")

    logger.debug(f"Environment ready with {len(env)} secrets")
    return env


def prepare_docker_auth(
    docker_image: Optional[str],
    docker_username: Optional[str],
    docker_password: Optional[str],
) -> Optional[Dict[str, str]]:
    """
    Prepare Docker authentication dictionary.

    Args:
        docker_image: Optional Docker image name
        docker_username: Optional Docker registry username
        docker_password: Optional Docker registry password

    Returns:
        Docker authentication dict if all credentials provided, None otherwise
    """
    if docker_image and docker_username and docker_password:
        return {"username": docker_username, "password": docker_password}
    return None
