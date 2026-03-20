"""
High-level orchestration for YT vanilla operations.

This module provides functions for running vanilla operations on YTsaurus clusters.
"""

import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional, Any, TYPE_CHECKING

from omegaconf import DictConfig, OmegaConf

from yt_framework.utils.logging import log_header, log_success
from .common import (
    build_environment,
    extract_operation_resources,
    build_operation_environment,
    extract_docker_auth_from_operation_config,
    extract_max_failed_jobs,
    collect_passthrough_kwargs,
)
from .dependency_strategy import TarArchiveDependencyBuilder

if TYPE_CHECKING:
    from yt_framework.core.stage import StageContext


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
    dep = builder.build_dependencies(
        operation_type="vanilla",
        stage_dir=stage_dir,
        archive_name="source.tar.gz",
        build_folder=pipeline_config.pipeline.build_folder,
        operation_config=operation_config,
        stage_config=stage_config,
        logger=logger,
    )
    script_path = dep.script_path
    dependencies = dep.dependencies
    command = dep.command

    docker_auth = extract_docker_auth_from_operation_config(operation_config, environment)

    return VanillaOperationData(
        script_path=script_path,
        dependencies=dependencies,
        environment=environment,
        docker_auth=docker_auth,
        command=command,
    )


def run_vanilla(
    context: "StageContext",
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
    env = build_operation_environment(
        context=context,
        operation_config=operation_config,
        logger=logger,
        include_stage_name=True,
        include_tokenizer_artifact=False,
    )

    # Prepare operation data automatically
    vanilla_operation_data = _prepare_vanilla_operation(
        pipeline_config=context.deps.pipeline_config,
        operation_config=operation_config,
        stage_config=context.config,
        stage_dir=context.stage_dir,
        configs_dir=context.deps.configs_dir,
        logger=logger,
    )
    vanilla_operation_data.environment = env
    vanilla_operation_data.docker_auth = extract_docker_auth_from_operation_config(
        operation_config, env
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

    logger.debug("Extracting operation resources from config")
    resources = extract_operation_resources(operation_config, logger)
    max_failed_jobs = extract_max_failed_jobs(operation_config, logger)

    vanilla_kwargs: dict[str, Any] = {}
    od = operation_config.get("operation_description")
    if od:
        if isinstance(od, str):
            logger.info(f"Operation label: {od}")
        else:
            vanilla_kwargs["operation_description"] = OmegaConf.to_container(
                od, resolve=True
            )

    vanilla_kwargs.update(
        collect_passthrough_kwargs(
            operation_config,
            reserved_keys={
                "resources",
                "env",
                "max_failed_job_count",
                "file_paths",
                "checkpoint",
                "tokenizer_artifact",
                "tar_command_bootstrap",
                "operation_description",
            },
        )
    )

    operation = context.deps.yt_client.run_vanilla(
        command=command,
        files=vanilla_operation_data.dependencies,
        env=vanilla_operation_data.environment,
        task_name=task_name,
        resources=resources,
        docker_auth=vanilla_operation_data.docker_auth,
        max_failed_jobs=max_failed_jobs,
        **vanilla_kwargs,
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
