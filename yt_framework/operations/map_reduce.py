"""
Map-reduce and reduce-only operations for YT Framework.

Stages use these to run YT map-reduce or reduce operations with TypedJob
mapper/reducer instances. Dependencies (archive + optional file_paths) are
built by the framework; credentials come from configs/secrets.env.
"""

import logging
from pathlib import Path
from typing import Any, List, Optional

from omegaconf import DictConfig, OmegaConf

from yt_framework.utils.logging import log_header, log_success
from yt_framework.core.stage import StageContext
from yt_framework.yt.client_base import OperationResources
from .dependency_strategy import TarArchiveDependencyBuilder
from .job_command import require_consistent_map_reduce_legs
from .common import (
    build_environment,
    prepare_docker_auth,
    _get_config_value_with_default,
)


def _resources_from_config(operation_config: DictConfig, logger: logging.Logger) -> OperationResources:
    """Extract OperationResources from operation_config.resources."""
    resources_config = operation_config.get("resources") or operation_config
    pool = _get_config_value_with_default(resources_config, "pool", "default", logger)
    pool_tree = _get_config_value_with_default(resources_config, "pool_tree", None, logger)
    docker_image = _get_config_value_with_default(resources_config, "docker_image", None, logger)
    memory_gb = _get_config_value_with_default(resources_config, "memory_limit_gb", 4, logger)
    cpu_limit = _get_config_value_with_default(resources_config, "cpu_limit", 2, logger)
    gpu_limit = _get_config_value_with_default(resources_config, "gpu_limit", 0, logger)
    job_count = _get_config_value_with_default(resources_config, "job_count", 1, logger)
    user_slots = _get_config_value_with_default(resources_config, "user_slots", None, logger)
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


def run_map_reduce(
    context: StageContext,
    operation_config: DictConfig,
    mapper: Any,
    reducer: Any,
    output_schema: Optional[Any] = None,
) -> bool:
    """
    Run a YT map-reduce operation and wait for completion.

    Pass mapper and reducer either both as ``TypedJob`` instances or both as
    command strings (JSON stdin/stdout). Mixing kinds raises ``ValueError``.

    Set ``operation_config.tar_command_bootstrap: true`` to wrap string legs with
    the same ``tar -xzf source.tar.gz`` + wrapper pattern as map operations
    (requires matching wrappers in the uploaded tarball; see docs).

    Args:
        context: Stage context (deps, logger, stage_dir, config).
        operation_config: client.operations.map_reduce config (input_table,
            output_table, reduce_by, sort_by, resources, file_paths, etc.).
        mapper: Mapper TypedJob instance.
        reducer: Reducer TypedJob instance.
        output_schema: Optional YT TableSchema for output table.

    Returns:
        True if the operation completed successfully.
    """
    logger = context.logger
    log_header(
        logger,
        "Map-Reduce Operation",
        f"Input: {operation_config.get('input_table')} -> Output: {operation_config.get('output_table')}",
    )

    input_table = operation_config.get("input_table")
    output_table = operation_config.get("output_table")
    reduce_by = list(operation_config.get("reduce_by") or [])
    if not input_table or not output_table or not reduce_by:
        raise ValueError("operation_config must set input_table, output_table, and reduce_by")

    env = build_environment(configs_dir=context.deps.configs_dir, logger=logger)
    for k, v in (operation_config.get("env") or {}).items():
        if v is not None:
            env[str(k)] = str(v)
    resources = _resources_from_config(operation_config, logger)

    require_consistent_map_reduce_legs(mapper, reducer)

    builder = TarArchiveDependencyBuilder()
    dep = builder.build_dependencies(
        operation_type="map_reduce",
        stage_dir=context.stage_dir,
        archive_name="source.tar.gz",
        build_folder=context.deps.pipeline_config.pipeline.build_folder,
        operation_config=operation_config,
        stage_config=context.config,
        logger=logger,
        mapper=mapper,
        reducer=reducer,
    )
    dependencies = dep.dependencies
    if dep.mapper_command is not None and dep.reducer_command is not None:
        mapper = dep.mapper_command
        reducer = dep.reducer_command
        logger.info("Using tar bootstrap commands for map-reduce mapper and reducer legs")
    elif dep.mapper_command is not None or dep.reducer_command is not None:
        raise RuntimeError(
            "Internal error: partial map-reduce tar bootstrap (only one leg set); "
            "expected both or neither."
        )

    docker_image = (operation_config.get("resources") or {}).get("docker_image") or operation_config.get("docker_image")
    docker_auth = prepare_docker_auth(
        docker_image=docker_image,
        docker_username=env.get("DOCKER_AUTH_USERNAME"),
        docker_password=env.get("DOCKER_AUTH_PASSWORD"),
    )

    sort_by = list(operation_config.get("sort_by") or [])
    max_failed_jobs = _get_config_value_with_default(operation_config, "max_failed_job_count", 1, logger)

    spec_kwargs: dict = {}
    if operation_config.get("map_job_count") is not None:
        spec_kwargs["map_job_count"] = operation_config.map_job_count

    od = operation_config.get("operation_description")
    if od:
        if isinstance(od, str):
            logger.info(f"Operation label: {od}")
        else:
            spec_kwargs["operation_description"] = OmegaConf.to_container(od, resolve=True)
    if operation_config.get("typed_reduce_row_iterator_io"):
        spec_kwargs["typed_reduce_row_iterator_io"] = True
    rji = operation_config.get("reduce_job_io")
    if rji:
        spec_kwargs["reduce_job_io"] = OmegaConf.to_container(rji, resolve=True)
    mji = operation_config.get("map_job_io")
    if mji:
        spec_kwargs["map_job_io"] = OmegaConf.to_container(mji, resolve=True)

    operation = context.deps.yt_client.run_map_reduce(
        mapper=mapper,
        reducer=reducer,
        input_table=input_table,
        output_table=output_table,
        reduce_by=reduce_by,
        files=dependencies,
        resources=resources,
        env=env,
        sort_by=sort_by if sort_by else None,
        output_schema=output_schema,
        max_failed_jobs=max_failed_jobs,
        docker_auth=docker_auth,
        **spec_kwargs,
    )

    success = context.deps.yt_client.wait_for_operation(operation)
    if success:
        log_success(logger, "Map-reduce operation completed successfully")
    else:
        logger.error("Map-reduce operation failed")
    return success


def run_reduce(
    context: StageContext,
    operation_config: DictConfig,
    reducer: Any,
    output_schema: Optional[Any] = None,
) -> bool:
    """
    Run a YT reduce-only operation and wait for completion.

    Pass ``reducer`` as a ``TypedJob`` or a command string. With
    ``operation_config.tar_command_bootstrap: true``, string reducers get the same
    tar extract + wrapper bootstrap as map (see docs).

    Args:
        context: Stage context.
        operation_config: client.operations.reduce config.
        reducer: Reducer TypedJob instance.
        output_schema: Optional output table schema.

    Returns:
        True if the operation completed successfully.
    """
    logger = context.logger
    log_header(
        logger,
        "Reduce Operation",
        f"Input: {operation_config.get('input_table')} -> Output: {operation_config.get('output_table')}",
    )

    input_table = operation_config.get("input_table")
    output_table = operation_config.get("output_table")
    reduce_by = list(operation_config.get("reduce_by") or [])
    if not input_table or not output_table or not reduce_by:
        raise ValueError("operation_config must set input_table, output_table, and reduce_by")

    env = build_environment(configs_dir=context.deps.configs_dir, logger=logger)
    for k, v in (operation_config.get("env") or {}).items():
        if v is not None:
            env[str(k)] = str(v)
    resources = _resources_from_config(operation_config, logger)

    builder = TarArchiveDependencyBuilder()
    dep = builder.build_dependencies(
        operation_type="reduce",
        stage_dir=context.stage_dir,
        archive_name="source.tar.gz",
        build_folder=context.deps.pipeline_config.pipeline.build_folder,
        operation_config=operation_config,
        stage_config=context.config,
        logger=logger,
        reducer=reducer,
    )
    dependencies = dep.dependencies
    if dep.reducer_command is not None:
        reducer = dep.reducer_command
        logger.info("Using tar bootstrap command for reduce leg")

    docker_image = (operation_config.get("resources") or {}).get("docker_image") or operation_config.get("docker_image")
    docker_auth = prepare_docker_auth(
        docker_image=docker_image,
        docker_username=env.get("DOCKER_AUTH_USERNAME"),
        docker_password=env.get("DOCKER_AUTH_PASSWORD"),
    )

    max_failed_jobs = _get_config_value_with_default(operation_config, "max_failed_job_count", 1, logger)

    reduce_kw: dict = {}
    rod = operation_config.get("operation_description")
    if rod:
        if isinstance(rod, str):
            logger.info(f"Operation label: {rod}")
        else:
            reduce_kw["operation_description"] = OmegaConf.to_container(rod, resolve=True)
    jio = operation_config.get("job_io")
    if jio:
        reduce_kw["job_io"] = OmegaConf.to_container(jio, resolve=True)

    operation = context.deps.yt_client.run_reduce(
        reducer=reducer,
        input_table=input_table,
        output_table=output_table,
        reduce_by=reduce_by,
        files=dependencies,
        resources=resources,
        env=env,
        output_schema=output_schema,
        max_failed_jobs=max_failed_jobs,
        docker_auth=docker_auth,
        **reduce_kw,
    )

    success = context.deps.yt_client.wait_for_operation(operation)
    if success:
        log_success(logger, "Reduce operation completed successfully")
    else:
        logger.error("Reduce operation failed")
    return success
