"""
High-level orchestration for YT map operations.

This module provides functions for running map operations on YTsaurus clusters.
"""

import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional, TYPE_CHECKING, Any

from yt.wrapper.schema import TableSchema  # pyright: ignore[reportMissingImports]
from omegaconf import DictConfig

from yt_framework.utils.logging import log_header, log_success
from yt_framework.yt.client_base import OperationResources
from .dependency_strategy import TarArchiveDependencyBuilder
from .common import (
    extract_operation_resources,
    build_operation_environment,
    extract_docker_auth_from_operation_config,
    extract_max_failed_jobs,
    collect_passthrough_kwargs,
)

if TYPE_CHECKING:
    from yt_framework.core.stage import StageContext


@dataclass
class MapOperationData:
    """Data container for map operation configuration.

    Attributes:
        mapper_path: Path to mapper.py script in YT (or bash wrapper if tar mode).
        dependencies: List of (yt_path, local_path) tuples for files to upload.
        environment: Environment variables dictionary (secrets only).
        docker_auth: Optional Docker authentication dictionary for private registries.
        command: Optional command to execute (used in tar archive mode).
    """

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
    logger: logging.Logger,
) -> MapOperationData:
    """
    Build tar-archive dependencies for a map operation.

    Environment and docker_auth are intentionally left empty here; the caller
    builds them via ``build_operation_environment`` and sets them on the returned
    object after construction.

    Args:
        pipeline_config: Pipeline-level config (build_folder, etc.)
        operation_config: Operation-specific config (from client.operations.map)
        stage_config: Full stage config (for accessing job section)
        stage_dir: Path to stage directory
        logger: Logger instance

    Returns:
        MapOperationData with dependencies and command populated.
    """
    builder = TarArchiveDependencyBuilder()
    dep = builder.build_dependencies(
        operation_type="map",
        stage_dir=stage_dir,
        archive_name="source.tar.gz",
        build_folder=pipeline_config.pipeline.build_folder,
        operation_config=operation_config,
        stage_config=stage_config,
        logger=logger,
    )

    return MapOperationData(
        mapper_path=dep.script_path,
        dependencies=dep.dependencies,
        environment={},
        docker_auth=None,
        command=dep.command,
    )


def run_map(
    context: "StageContext",
    operation_config: DictConfig,
    output_schema: Optional[TableSchema] = None,
    mapper: Optional[Any] = None,
    job: Optional[Any] = None,
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
        mapper: Optional mapper leg (legacy name). When omitted, framework uses command wrapper.
            Can be a TypedJob instance or command string.
        job: Preferred mapper leg alias. Can be a TypedJob instance or command string.

    Returns:
        True if successful, False otherwise
    """
    logger = context.logger
    if not operation_config.get("input_table"):
        raise ValueError(
            "No input_table in operation_config; "
            "expected at client.operations.map.input_table"
        )
    if not operation_config.get("output_table"):
        raise ValueError(
            "No output_table in operation_config; "
            "expected at client.operations.map.output_table"
        )

    log_header(
        logger,
        "Map Operation",
        f"Input: {operation_config.input_table} | Output: {operation_config.output_table}",
    )

    env = build_operation_environment(
        context=context,
        operation_config=operation_config,
        logger=logger,
        include_stage_name=True,
        include_tokenizer_artifact=True,
    )

    map_operation_data = _prepare_map_operation(
        pipeline_config=context.deps.pipeline_config,
        operation_config=operation_config,
        stage_config=context.config,
        stage_dir=context.stage_dir,
        logger=logger,
    )
    map_operation_data.environment = env
    map_operation_data.docker_auth = extract_docker_auth_from_operation_config(
        operation_config, env
    )

    logger.debug(f"Dependencies: {len(map_operation_data.dependencies)} files")

    if mapper is not None and job is not None and mapper != job:
        raise ValueError(
            "Both 'mapper' and 'job' are set with different values; use only one"
        )
    mapper_leg = job if job is not None else mapper
    if mapper_leg is None:
        mapper_leg = map_operation_data.command
    if mapper_leg is None:
        raise ValueError("Command not provided by dependency builder")

    logger.debug("Extracting operation resources from config")
    resources: OperationResources = extract_operation_resources(
        operation_config, logger
    )
    max_failed_jobs = extract_max_failed_jobs(operation_config, logger)

    map_kwargs: dict = {}
    od = operation_config.get("operation_description")
    if od:
        if isinstance(od, str):
            logger.info(f"Operation label: {od}")
            map_kwargs["title"] = od
        else:
            from omegaconf import OmegaConf as _OmegaConf

            map_kwargs["operation_description"] = _OmegaConf.to_container(
                od, resolve=True
            )

    reserved_keys = {
        "input_table",
        "output_table",
        "resources",
        "env",
        "max_failed_job_count",
        "file_paths",
        "checkpoint",
        "tokenizer_artifact",
        "tar_command_bootstrap",
        "operation_description",
    }
    map_kwargs.update(collect_passthrough_kwargs(operation_config, reserved_keys))

    operation = context.deps.yt_client.run_map(
        command=mapper_leg,
        input_table=operation_config.input_table,
        output_table=operation_config.output_table,
        files=map_operation_data.dependencies,
        resources=resources,
        env=map_operation_data.environment,
        output_schema=output_schema,
        max_failed_jobs=max_failed_jobs,
        docker_auth=map_operation_data.docker_auth,
        **map_kwargs,
    )

    # Wait for completion
    success = context.deps.yt_client.wait_for_operation(operation)

    if success:
        log_success(logger, "Map operation completed successfully")
    else:
        logger.error("Map operation failed")

    return success
