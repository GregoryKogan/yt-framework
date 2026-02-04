"""
Map Operations
==============

High-level orchestration for YT map operations.
"""

import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional

from yt.wrapper.schema import TableSchema  # pyright: ignore[reportMissingImports]
from omegaconf import DictConfig

from yt_framework.utils.logging import log_header, log_success
from yt_framework.core.stage import StageContext
from yt_framework.yt.client_base import OperationResources
from .dependency_strategy import TarArchiveDependencyBuilder
from .common import build_environment, prepare_docker_auth, _get_config_value_with_default


@dataclass
class MapOperationData:
    mapper_path: str
    dependencies: List[Tuple[str, str]]
    environment: Dict[str, str]
    docker_auth: Optional[Dict[str, str]]
    command: Optional[str] = None


def _prepare_map_operation(
    pipeline_config: DictConfig,
    operation_config: DictConfig,
    stage_config: DictConfig,
    stage_dir: Path,
    configs_dir: Path,
    logger: logging.Logger,
) -> MapOperationData:
    """
    Prepare everything needed for a map operation (private function).

    Automatically handles:
    - Secrets-only environment building
    - Dependency file list preparation
    - Docker authentication preparation

    Args:
        pipeline_config: Pipeline-level config (for secrets)
        operation_config: Operation-specific config (from client.operations.map)
        stage_config: Full stage config (for accessing job section)
        stage_dir: Path to stage directory
        configs_dir: Directory containing secrets.env
        logger: Logger instance

    Returns:
        MapOperationData instance containing:
        - mapper_path: Path to mapper.py in YT (or bash wrapper if tar mode)
        - dependencies: List of (yt_path, local_path) tuples
        - environment: Environment variables (secrets only)
        - docker_auth: Docker auth dict or None
        - command: Optional command to execute (for tar mode)
    """

    environment = build_environment(configs_dir=configs_dir, logger=logger)

    # Use strategy pattern to build dependencies
    # Pass both operation_config (for checkpoint) and stage_config (for job.model_name)
    builder = TarArchiveDependencyBuilder()
    mapper_path, dependencies, command = builder.build_dependencies(
        operation_type="map",
        stage_dir=stage_dir,
        build_folder=pipeline_config.pipeline.build_folder,
        operation_config=operation_config,
        stage_config=stage_config,
        logger=logger,
    )

    # Get Docker auth credentials from loaded secrets
    # Support both resources.docker_image and direct docker_image for flexibility
    docker_image = None
    if "resources" in operation_config and operation_config.resources.get("docker_image"):
        docker_image = operation_config.resources.docker_image
    elif operation_config.get("docker_image"):
        docker_image = operation_config.docker_image
    
    docker_auth = prepare_docker_auth(
        docker_image=docker_image,
        docker_username=environment.get("DOCKER_AUTH_USERNAME"),
        docker_password=environment.get("DOCKER_AUTH_PASSWORD"),
    )

    return MapOperationData(
        mapper_path=mapper_path,
        dependencies=dependencies,
        environment=environment,
        docker_auth=docker_auth,
        command=command,
    )


def run_map(
    context: StageContext,
    operation_config: DictConfig,
    output_schema: Optional[TableSchema] = None,
) -> bool:
    """
    Run YT map operation and wait for completion.

    All job parameters (pool, memory, CPU, Docker image, etc.) are automatically
    extracted from operation_config. Operation config should be passed from
    stage.config.operations.map.

    Args:
        context: Stage context (provides deps, logger, stage_dir)
        operation_config: Operation-specific config (from client.operations.map)
        output_schema: Optional YT TableSchema for typed output table

    Returns:
        True if successful, False otherwise
    """
    logger = context.logger
    log_header(logger, "Map Operation", f"Input: {operation_config.input_table} | Output: {operation_config.output_table}")

    if not operation_config.get("input_table"):
        raise ValueError("No input_table configured in operation config")
    if not operation_config.get("output_table"):
        raise ValueError("No output_table configured in operation config")

    # Prepare operation data automatically
    map_operation_data = _prepare_map_operation(
        pipeline_config=context.deps.pipeline_config,
        operation_config=operation_config,
        stage_config=context.config,
        stage_dir=context.stage_dir,
        configs_dir=context.deps.configs_dir,
        logger=logger,
    )

    logger.debug(f"Dependencies: {len(map_operation_data.dependencies)} files")

    # Command is always provided by the dependency builder (tar archive mode)
    if not map_operation_data.command:
        raise ValueError("Command not provided by dependency builder")
    
    command = map_operation_data.command

    # Extract job parameters from operation_config.resources (or top-level as fallback)
    # Use defaults when values are not specified in config, logging when defaults are used
    resources_config = operation_config.get("resources", {})
    if not resources_config:
        # Fallback to top-level config if resources section doesn't exist
        resources_config = operation_config
    
    logger.debug("Extracting operation resources from config")
    
    pool = _get_config_value_with_default(resources_config, "pool", "default", logger)
    pool_tree = _get_config_value_with_default(resources_config, "pool_tree", None, logger)
    docker_image = _get_config_value_with_default(resources_config, "docker_image", None, logger)
    memory_gb = _get_config_value_with_default(resources_config, "memory_limit_gb", 4, logger)
    cpu_limit = _get_config_value_with_default(resources_config, "cpu_limit", 2, logger)
    gpu_limit = _get_config_value_with_default(resources_config, "gpu_limit", 0, logger)
    job_count = _get_config_value_with_default(resources_config, "job_count", 1, logger)
    user_slots = _get_config_value_with_default(resources_config, "user_slots", None, logger)
    max_failed_jobs = _get_config_value_with_default(operation_config, "max_failed_job_count", 1, logger)
    
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

    operation = context.deps.yt_client.run_map(
        command=command,
        input_table=operation_config.input_table,
        output_table=operation_config.output_table,
        files=map_operation_data.dependencies,
        resources=resources,
        env=map_operation_data.environment,
        output_schema=output_schema,
        max_failed_jobs=max_failed_jobs,
        docker_auth=map_operation_data.docker_auth,
    )

    # Wait for completion
    success = context.deps.yt_client.wait_for_operation(operation)
    
    if success:
        log_success(logger, "Map operation completed successfully")
    else:
        logger.error("Map operation failed")
    
    return success
