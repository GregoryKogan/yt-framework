"""Shared helpers for map/vanilla/map-reduce (resources, secrets, tokenizer wiring)."""

import logging
from pathlib import Path
from typing import Dict, Optional, Any, Set, TYPE_CHECKING

from omegaconf import DictConfig

from yt_framework.yt.client_base import OperationResources
from yt_framework.utils.env import load_secrets
from .tokenizer_artifact import (
    init_tokenizer_artifact_directory,
    resolve_tokenizer_artifact_name,
    resolve_tokenizer_archive_name,
)

if TYPE_CHECKING:
    from yt_framework.core.stage import StageContext


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
    configs_dir: Path,
    logger: logging.Logger,
) -> Dict[str, str]:
    """
    Build environment variables for map and vanilla operations.

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


def extract_operation_resources(
    operation_config: DictConfig,
    logger: logging.Logger,
) -> OperationResources:
    """Extract OperationResources from operation config with fallback defaults."""
    resources_config = operation_config.get("resources") or operation_config
    pool = _get_config_value_with_default(resources_config, "pool", "default", logger)
    pool_tree = _get_config_value_with_default(
        resources_config, "pool_tree", None, logger
    )
    docker_image = _get_config_value_with_default(
        resources_config, "docker_image", None, logger
    )
    memory_gb = _get_config_value_with_default(
        resources_config, "memory_limit_gb", 4, logger
    )
    cpu_limit = _get_config_value_with_default(resources_config, "cpu_limit", 2, logger)
    gpu_limit = _get_config_value_with_default(resources_config, "gpu_limit", 0, logger)
    job_count = _get_config_value_with_default(resources_config, "job_count", 1, logger)
    user_slots = _get_config_value_with_default(
        resources_config, "user_slots", None, logger
    )
    return OperationResources(
        pool=pool,
        pool_tree=pool_tree,
        docker_image=docker_image,
        memory_gb=memory_gb,
        cpu_limit=cpu_limit,
        gpu_limit=gpu_limit,
        job_count=job_count,
        user_slots=user_slots,
    )


def extract_secure_env_client_kwargs(operation_config: DictConfig) -> Dict[str, Any]:
    """Options for ``YTProdClient`` secure vault / public env partitioning."""
    out: Dict[str, Any] = {}
    epk = operation_config.get("environment_public_keys")
    if epk is not None:
        out["environment_public_keys"] = [str(x) for x in list(epk)]
    if operation_config.get("use_plain_environment_for_secrets"):
        out["use_plain_environment_for_secrets"] = True
    return out


def collect_passthrough_kwargs(
    operation_config: DictConfig,
    reserved_keys: Set[str],
) -> Dict[str, Any]:
    """
    Collect top-level config values to forward to YT client.

    OmegaConf dict nodes are resolved to plain Python containers.
    """
    from omegaconf import DictConfig as OmegaDictConfig, OmegaConf

    out: Dict[str, Any] = {}
    for k in operation_config.keys():
        if k in reserved_keys:
            continue
        v = operation_config.get(k)
        if v is None:
            continue
        if isinstance(v, OmegaDictConfig):
            out[str(k)] = OmegaConf.to_container(v, resolve=True)
        else:
            out[str(k)] = v
    return out


def build_operation_environment(
    context: "StageContext",
    operation_config: DictConfig,
    logger: logging.Logger,
    include_stage_name: bool = True,
    include_tokenizer_artifact: bool = True,
) -> Dict[str, str]:
    """
    Build operation environment from secrets + explicit env config + optional helpers.
    """
    env = build_environment(configs_dir=context.deps.configs_dir, logger=logger)
    for k, v in (operation_config.get("env") or {}).items():
        if v is not None:
            env[str(k)] = str(v)

    if include_tokenizer_artifact:
        tokenizer_cfg = operation_config.get("tokenizer_artifact")
        if tokenizer_cfg:
            init_tokenizer_artifact_directory(
                context=context,
                tokenizer_artifact_config=tokenizer_cfg,
            )
            if tokenizer_cfg.get("artifact_base"):
                artifact_name = resolve_tokenizer_artifact_name(
                    stage_config=context.config,
                    tokenizer_artifact_config=tokenizer_cfg,
                )
                if artifact_name:
                    archive_name = resolve_tokenizer_archive_name(artifact_name)
                    env.setdefault("TOKENIZER_ARTIFACT_FILE", archive_name)
                    env.setdefault(
                        "TOKENIZER_ARTIFACT_DIR", f"tokenizer_artifacts/{artifact_name}"
                    )
                    env.setdefault("TOKENIZER_ARTIFACT_NAME", artifact_name)

    if include_stage_name:
        env.setdefault("YT_STAGE_NAME", context.name)

    return env


def extract_docker_auth_from_operation_config(
    operation_config: DictConfig,
    env: Dict[str, str],
) -> Optional[Dict[str, str]]:
    """Resolve docker image from config and return auth payload if credentials exist."""
    docker_image = (operation_config.get("resources") or {}).get(
        "docker_image"
    ) or operation_config.get("docker_image")
    return prepare_docker_auth(
        docker_image=docker_image,
        docker_username=env.get("DOCKER_AUTH_USERNAME"),
        docker_password=env.get("DOCKER_AUTH_PASSWORD"),
    )


def extract_max_failed_jobs(
    operation_config: DictConfig,
    logger: logging.Logger,
) -> int:
    """Extract max_failed_job_count with default."""
    return _get_config_value_with_default(
        operation_config, "max_failed_job_count", 1, logger
    )
