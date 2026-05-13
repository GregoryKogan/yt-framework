"""Shared helpers for map/vanilla/map-reduce (resources, secrets, tokenizer wiring)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from omegaconf import DictConfig, ListConfig, OmegaConf

from yt_framework.utils.env import load_secrets
from yt_framework.yt.clients.client_base import OperationResources

from ._internal.tokenizer_artifact import (
    init_tokenizer_artifact_directory,
    resolve_tokenizer_archive_name,
    resolve_tokenizer_artifact_name,
)

if TYPE_CHECKING:
    import logging
    from pathlib import Path

    from yt_framework.operations.stage_contracts import StageContext


def _dict_config_or(node: object, *, fallback: DictConfig) -> DictConfig:
    return node if isinstance(node, DictConfig) else fallback


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _int_from_config_value(value: object) -> int:
    return int(cast("Any", value))


def _get_config_value_with_default(
    config: DictConfig,
    key: str,
    default: object,
    logger: logging.Logger,
) -> object:
    """Get config value with default, logging when default is used.

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
            logger.info("  Using default %s=%s (not specified in config)", key, default)
            return default

        value = config.get(key)
        # If value is None, use default and log
        if value is None:
            logger.info("  Using default %s=%s (value is None in config)", key, default)
            return default
    except (AttributeError, KeyError, RuntimeError, TypeError):
        # Key doesn't exist or access failed, use default
        logger.info("  Using default %s=%s (not specified in config)", key, default)
        return default
    else:
        return value


def build_environment(
    configs_dir: Path,
    logger: logging.Logger,
) -> dict[str, str]:
    """Build environment variables for map and vanilla operations.

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
        logger.debug("  %s: %s", key, "*" * min(len(value), 10))

    logger.debug("Environment ready with %s secrets", len(env))
    return env


def prepare_docker_auth(
    docker_image: str | None,
    docker_username: str | None,
    docker_password: str | None,
) -> dict[str, str] | None:
    """Prepare Docker authentication dictionary.

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
    resources_config = _dict_config_or(
        operation_config.get("resources"),
        fallback=operation_config,
    )
    pool = str(
        _get_config_value_with_default(resources_config, "pool", "default", logger),
    )
    pool_tree = _optional_str(
        _get_config_value_with_default(
            resources_config,
            "pool_tree",
            None,
            logger,
        ),
    )
    docker_image = _optional_str(
        _get_config_value_with_default(
            resources_config,
            "docker_image",
            None,
            logger,
        ),
    )
    memory_gb = _int_from_config_value(
        _get_config_value_with_default(
            resources_config,
            "memory_limit_gb",
            4,
            logger,
        ),
    )
    cpu_limit = _int_from_config_value(
        _get_config_value_with_default(resources_config, "cpu_limit", 2, logger),
    )
    gpu_limit = _int_from_config_value(
        _get_config_value_with_default(resources_config, "gpu_limit", 0, logger),
    )
    job_count = _int_from_config_value(
        _get_config_value_with_default(resources_config, "job_count", 1, logger),
    )
    user_slots_raw = _get_config_value_with_default(
        resources_config,
        "user_slots",
        None,
        logger,
    )
    user_slots = (
        _int_from_config_value(user_slots_raw) if user_slots_raw is not None else None
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


def extract_secure_env_client_kwargs(operation_config: DictConfig) -> dict[str, Any]:
    """Options for ``YTProdClient`` secure vault / public env partitioning."""
    out: dict[str, Any] = {}
    epk = operation_config.get("environment_public_keys")
    if epk is not None:
        if isinstance(epk, (list, tuple, ListConfig)):
            out["environment_public_keys"] = [str(x) for x in epk]
        else:
            out["environment_public_keys"] = [str(epk)]
    if operation_config.get("use_plain_environment_for_secrets"):
        out["use_plain_environment_for_secrets"] = True
    return out


def collect_passthrough_kwargs(
    operation_config: DictConfig,
    reserved_keys: set[str],
) -> dict[str, Any]:
    """Collect top-level config values to forward to YT client.

    OmegaConf dict nodes are resolved to plain Python containers.
    """
    out: dict[str, Any] = {}
    for k in operation_config:
        if k in reserved_keys:
            continue
        v = operation_config.get(k)
        if v is None:
            continue
        if isinstance(v, DictConfig):
            out[str(k)] = OmegaConf.to_container(v, resolve=True)
        else:
            out[str(k)] = v
    return out


def _merge_tokenizer_keys_into_env(
    env: dict[str, str],
    context: StageContext,
    tokenizer_cfg_raw: DictConfig,
) -> None:
    init_tokenizer_artifact_directory(
        context=context,
        tokenizer_artifact_config=tokenizer_cfg_raw,
    )
    if not tokenizer_cfg_raw.get("artifact_base"):
        return
    artifact_name = resolve_tokenizer_artifact_name(
        stage_config=context.config,
        tokenizer_artifact_config=tokenizer_cfg_raw,
    )
    if not artifact_name:
        return
    archive_name = resolve_tokenizer_archive_name(artifact_name)
    env.setdefault("TOKENIZER_ARTIFACT_FILE", archive_name)
    env.setdefault(
        "TOKENIZER_ARTIFACT_DIR",
        f"tokenizer_artifacts/{artifact_name}",
    )
    env.setdefault("TOKENIZER_ARTIFACT_NAME", artifact_name)


def _merge_operation_env_block(
    env: dict[str, str],
    operation_config: DictConfig,
) -> None:
    env_block = operation_config.get("env")
    env_pairs: Mapping[str, Any] = env_block if isinstance(env_block, Mapping) else {}
    for k, v in env_pairs.items():
        if v is not None:
            env[str(k)] = str(v)


def build_operation_environment(
    context: StageContext,
    operation_config: DictConfig,
    logger: logging.Logger,
    include_stage_name: bool = True,  # noqa: FBT001,FBT002
    include_tokenizer_artifact: bool = True,  # noqa: FBT001,FBT002
) -> dict[str, str]:
    """Build operation environment from secrets + explicit env config + optional helpers."""
    env = build_environment(configs_dir=context.deps.configs_dir, logger=logger)
    _merge_operation_env_block(env, operation_config)

    if include_tokenizer_artifact:
        tokenizer_cfg_raw = operation_config.get("tokenizer_artifact")
        if isinstance(tokenizer_cfg_raw, DictConfig):
            _merge_tokenizer_keys_into_env(env, context, tokenizer_cfg_raw)

    if include_stage_name:
        env.setdefault("YT_STAGE_NAME", context.name)

    return env


def docker_auth_from_op_config(
    operation_config: DictConfig,
    env: dict[str, str],
) -> dict[str, str] | None:
    """Resolve docker image from config and return auth payload if credentials exist."""
    res_raw = operation_config.get("resources")
    res_map: Mapping[str, Any] = res_raw if isinstance(res_raw, Mapping) else {}
    docker_image = _optional_str(
        res_map.get("docker_image") or operation_config.get("docker_image"),
    )
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
    return _int_from_config_value(
        _get_config_value_with_default(
            operation_config,
            "max_failed_job_count",
            1,
            logger,
        ),
    )
