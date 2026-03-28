"""
Map-reduce and reduce-only operations for YT Framework.

Stages use these to run YT map-reduce or reduce operations with TypedJob
mapper/reducer instances. Dependencies (archive + optional file_paths) are
built by the framework; credentials come from configs/secrets.env.
"""

import warnings
from typing import Any, Optional, TYPE_CHECKING

from omegaconf import DictConfig, OmegaConf

from yt_framework.utils.logging import log_header, log_success
from .dependency_strategy import TarArchiveDependencyBuilder
from .job_command import require_consistent_map_reduce_legs
from .common import (
    extract_operation_resources,
    build_operation_environment,
    extract_docker_auth_from_operation_config,
    extract_max_failed_jobs,
    collect_passthrough_kwargs,
)

if TYPE_CHECKING:
    from yt_framework.core.stage import StageContext


def run_map_reduce(
    context: "StageContext",
    operation_config: DictConfig,
    mapper: Any = None,
    reducer: Any = None,
    output_schema: Optional[Any] = None,
    map_job: Any = None,
    reduce_job: Any = None,
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
        mapper: *Deprecated* — use ``map_job`` instead.
        reducer: *Deprecated* — use ``reduce_job`` instead.
        output_schema: Optional YT TableSchema for output table.
        map_job: Mapper leg (``TypedJob`` instance or command string).
        reduce_job: Reducer leg (``TypedJob`` instance or command string).

    Returns:
        True if the operation completed successfully.
    """
    if mapper is not None and map_job is None:
        warnings.warn(
            "'mapper=' is deprecated; use 'map_job=' instead",
            DeprecationWarning,
            stacklevel=2,
        )
    if reducer is not None and reduce_job is None:
        warnings.warn(
            "'reducer=' is deprecated; use 'reduce_job=' instead",
            DeprecationWarning,
            stacklevel=2,
        )
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
        raise ValueError(
            "operation_config must set input_table, output_table, and reduce_by; "
            "expected at client.operations.map_reduce.{input_table,output_table,reduce_by}"
        )

    env = build_operation_environment(
        context=context,
        operation_config=operation_config,
        logger=logger,
        include_stage_name=True,
        include_tokenizer_artifact=True,
    )
    resources = extract_operation_resources(operation_config, logger)

    if mapper is not None and map_job is not None and mapper != map_job:
        raise ValueError(
            "Both 'mapper' and 'map_job' are set with different values; use only one"
        )
    if reducer is not None and reduce_job is not None and reducer != reduce_job:
        raise ValueError(
            "Both 'reducer' and 'reduce_job' are set with different values; use only one"
        )
    mapper = map_job if map_job is not None else mapper
    reducer = reduce_job if reduce_job is not None else reducer

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
        logger.info(
            "Using tar bootstrap commands for map-reduce mapper and reducer legs"
        )
    elif dep.mapper_command is not None or dep.reducer_command is not None:
        raise RuntimeError(
            "Internal error: partial map-reduce tar bootstrap (only one leg set); "
            "expected both or neither."
        )

    docker_auth = extract_docker_auth_from_operation_config(operation_config, env)

    sort_by = list(operation_config.get("sort_by") or [])
    max_failed_jobs = extract_max_failed_jobs(operation_config, logger)

    spec_kwargs: dict = {}
    if operation_config.get("map_job_count") is not None:
        spec_kwargs["map_job_count"] = operation_config.map_job_count

    od = operation_config.get("operation_description")
    if od:
        if isinstance(od, str):
            logger.info(f"Operation label: {od}")
            spec_kwargs["title"] = od
        else:
            spec_kwargs["operation_description"] = OmegaConf.to_container(
                od, resolve=True
            )

    passthrough = collect_passthrough_kwargs(
        operation_config,
        reserved_keys={
            "input_table",
            "output_table",
            "reduce_by",
            "sort_by",
            "resources",
            "env",
            "max_failed_job_count",
            "file_paths",
            "checkpoint",
            "tokenizer_artifact",
            "tar_command_bootstrap",
            "map_job_count",
            "operation_description",
            # Legacy custom IO options are intentionally no longer consumed here.
            "typed_reduce_row_iterator_io",
            "reduce_job_io",
            "map_job_io",
        },
    )
    spec_kwargs.update(passthrough)

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
    context: "StageContext",
    operation_config: DictConfig,
    reducer: Any = None,
    output_schema: Optional[Any] = None,
    job: Any = None,
) -> bool:
    """
    Run a YT reduce-only operation and wait for completion.

    Pass ``reducer`` as a ``TypedJob`` or a command string. With
    ``operation_config.tar_command_bootstrap: true``, string reducers get the same
    tar extract + wrapper bootstrap as map (see docs).

    Args:
        context: Stage context.
        operation_config: client.operations.reduce config.
        reducer: Reducer leg (legacy name).
        output_schema: Optional output table schema.
        job: Preferred reducer leg alias.

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
        raise ValueError(
            "operation_config must set input_table, output_table, and reduce_by; "
            "expected at client.operations.reduce.{input_table,output_table,reduce_by}"
        )

    env = build_operation_environment(
        context=context,
        operation_config=operation_config,
        logger=logger,
        include_stage_name=True,
        include_tokenizer_artifact=True,
    )
    resources = extract_operation_resources(operation_config, logger)

    if reducer is not None and job is not None and reducer != job:
        raise ValueError(
            "Both 'reducer' and 'job' are set with different values; use only one"
        )
    reducer = job if job is not None else reducer

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

    docker_auth = extract_docker_auth_from_operation_config(operation_config, env)

    max_failed_jobs = extract_max_failed_jobs(operation_config, logger)

    reduce_kw: dict = {}
    rod = operation_config.get("operation_description")
    if rod:
        if isinstance(rod, str):
            logger.info(f"Operation label: {rod}")
            reduce_kw["title"] = rod
        else:
            reduce_kw["operation_description"] = OmegaConf.to_container(
                rod, resolve=True
            )

    passthrough = collect_passthrough_kwargs(
        operation_config,
        reserved_keys={
            "input_table",
            "output_table",
            "reduce_by",
            "resources",
            "env",
            "max_failed_job_count",
            "file_paths",
            "checkpoint",
            "tokenizer_artifact",
            "tar_command_bootstrap",
            "operation_description",
            # Legacy custom IO option is intentionally no longer consumed here.
            "job_io",
        },
    )
    reduce_kw.update(passthrough)

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
