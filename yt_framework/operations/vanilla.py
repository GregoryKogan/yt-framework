"""
High-level orchestration for YT vanilla operations.

This module provides functions for running vanilla operations on YTsaurus clusters.
"""

import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional

from omegaconf import DictConfig

from yt_framework.utils.logging import log_header, log_success
from yt_framework.core.stage import StageContext
from yt_framework.yt.client_base import OperationResources
from .common import (
    build_environment,
    prepare_docker_auth,
    _get_config_value_with_default,
)
from .dependency_strategy import TarArchiveDependencyBuilder


@dataclass
class VanillaOperationData:
    """Data container for vanilla operation configuration.
    
    Attributes:
        script_path: Path to vanilla.py script in YT (or placeholder if tar mode).
        dependencies: List of (yt_path, local_path) tuples for files to upload.
        environment: Environment variables dictionary (secrets only).
        docker_auth: Optional Docker authentication dictionary for private registries.
        command: Optional command to execute (used in tar archive mode).
    """
    script_path: str
    dependencies: List[Tuple[str, str]]
    environment: Dict[str, str]
    docker_auth: Optional[Dict[str, str]]
    command: Optional[str] = None


def _prepare_vanilla_operation(
    pipeline_config: DictConfig,
    operation_config: DictConfig,
    stage_config: DictConfig,
    stage_dir: Path,
    configs_dir: Path,
    logger: logging.Logger,
) -> VanillaOperationData:
    """
    Prepare everything needed for a vanilla operation (private function).

    Automatically handles:
    - Secrets-only environment building
    - Dependency file list preparation
    - Docker authentication preparation

    Args:
        pipeline_config: Pipeline-level config (for secrets)
        operation_config: Operation-specific config (from client.operations.vanilla)
        stage_config: Full stage config (for accessing job section)
        stage_dir: Path to stage directory
        configs_dir: Directory containing secrets.env
        logger: Logger instance

    Returns:
        VanillaOperationData instance containing:
        - script_path: Path to vanilla.py in YT (or placeholder if tar mode)
        - dependencies: List of (yt_path, local_path) tuples
        - environment: Environment variables (secrets only)
        - docker_auth: Docker auth dict or None
        - command: Optional command to execute (for tar mode)
    """

    environment = build_environment(configs_dir=configs_dir, logger=logger)

    # Use strategy pattern to build dependencies
    # Pass both operation_config (for checkpoint) and stage_config (for job.model_name)
    builder = TarArchiveDependencyBuilder()
    script_path, dependencies, command = builder.build_dependencies(
        operation_type="vanilla",
        stage_dir=stage_dir,
        build_folder=pipeline_config.pipeline.build_folder,
        operation_config=operation_config,
        stage_config=stage_config,
        logger=logger,
    )

    # Get Docker auth credentials from loaded secrets
    # Support both resources.docker_image and direct docker_image for flexibility
    docker_image = None
    if "resources" in operation_config and operation_config.resources.get(
        "docker_image"
    ):
        docker_image = operation_config.resources.docker_image
    elif operation_config.get("docker_image"):
        docker_image = operation_config.docker_image

    docker_auth = prepare_docker_auth(
        docker_image=docker_image,
        docker_username=environment.get("DOCKER_AUTH_USERNAME"),
        docker_password=environment.get("DOCKER_AUTH_PASSWORD"),
    )

    return VanillaOperationData(
        script_path=script_path,
        dependencies=dependencies,
        environment=environment,
        docker_auth=docker_auth,
        command=command,
    )


def run_vanilla(
    context: StageContext,
    operation_config: DictConfig,
) -> bool:
    """
    Run YT vanilla operation and wait for completion.

    All job parameters (pool, memory, CPU, Docker image, etc.) are automatically
    extracted from operation_config. Operation config should be passed from
    stage.config.operations.vanilla. The task name is automatically set to
    the stage name.

    Args:
        context: Stage context (provides deps, logger, stage_dir, name)
        operation_config: Operation-specific config (from client.operations.vanilla)

    Returns:
        True if successful, False otherwise
    """
    logger = context.logger
    # Use stage name as task name
    task_name = context.name

    # Prepare operation data automatically
    vanilla_operation_data = _prepare_vanilla_operation(
        pipeline_config=context.deps.pipeline_config,
        operation_config=operation_config,
        stage_config=context.config,
        stage_dir=context.stage_dir,
        configs_dir=context.deps.configs_dir,
        logger=logger,
    )

    log_header(
        logger,
        "Vanilla Operation",
        f"Task: {task_name} | Script: {vanilla_operation_data.script_path}",
    )
    logger.debug(f"Dependencies: {len(vanilla_operation_data.dependencies)} files")

    # Command is always provided by the dependency builder (tar archive mode)
    if not vanilla_operation_data.command:
        raise ValueError("Command not provided by dependency builder")

    command = vanilla_operation_data.command

    # Extract job parameters from operation_config.resources (or top-level as fallback)
    # Use defaults when values are not specified in config, logging when defaults are used
    resources_config = operation_config.get("resources", {})
    if not resources_config:
        # Fallback to top-level config if resources section doesn't exist
        resources_config = operation_config

    logger.debug("Extracting operation resources from config")

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
    max_failed_jobs = _get_config_value_with_default(
        operation_config, "max_failed_job_count", 1, logger
    )

    resources = OperationResources(
        pool=pool,
        pool_tree=pool_tree,
        docker_image=docker_image,
        memory_gb=memory_gb,
        cpu_limit=cpu_limit,
        gpu_limit=gpu_limit,
        job_count=job_count,
        user_slots=user_slots,
    )

    operation = context.deps.yt_client.run_vanilla(
        command=command,
        files=vanilla_operation_data.dependencies,
        env=vanilla_operation_data.environment,
        task_name=task_name,
        resources=resources,
        docker_auth=vanilla_operation_data.docker_auth,
        max_failed_jobs=max_failed_jobs,
    )

    if operation is None:
        logger.error("Failed to submit vanilla operation: returned None")
        return False

    logger.debug(f"Operation submitted: {operation.id}")

    # Wait for completion
    success = context.deps.yt_client.wait_for_operation(operation)

    if success:
        log_success(logger, "Vanilla operation completed successfully")
    else:
        logger.error("Vanilla operation failed")

    return success
