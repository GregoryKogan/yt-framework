"""Submit map-reduce and reduce-only YT operations (TypedJob or command strings).

Builds archives plus optional file dependencies; cluster credentials still come
from `configs/secrets.env` like other operations.
"""

import logging
import warnings
from typing import TYPE_CHECKING, Any

from omegaconf import DictConfig, ListConfig, OmegaConf
from yt.wrapper import Operation

from yt_framework.job_command import require_consistent_map_reduce_legs
from yt_framework.operations._internal.dependency_strategy import (
    TarArchiveDependencyBuilder,
)
from yt_framework.operations.common import (
    build_operation_environment,
    collect_passthrough_kwargs,
    docker_auth_from_op_config,
    extract_max_failed_jobs,
    extract_operation_resources,
    extract_secure_env_client_kwargs,
)
from yt_framework.utils.logging import log_header, log_success

if TYPE_CHECKING:
    from yt.wrapper.schema import TableSchema

    from yt_framework.core.stage import StageContext


def _wait_operation_with_log(
    context: "StageContext",
    operation: Operation,
    logger: logging.Logger,
    *,
    success_msg: str,
    failure_msg: str,
) -> bool:
    success = context.deps.yt_client.wait_for_operation(operation)
    if success:
        log_success(logger, success_msg)
    else:
        logger.error(failure_msg)
    return success


def _str_list_from_config(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, ListConfig)):
        return [str(x) for x in value]
    return [str(value)]


def _map_reduce_tables_ready(
    input_table: str,
    output_table: str,
    reduce_by: list[str],
) -> bool:
    return bool(input_table and output_table and reduce_by)


def _validate_map_reduce_inputs(
    operation_config: DictConfig,
) -> tuple[str, str, list[str]]:
    input_table = str(operation_config.get("input_table") or "")
    output_table = str(operation_config.get("output_table") or "")
    reduce_by = _str_list_from_config(operation_config.get("reduce_by"))
    if _map_reduce_tables_ready(input_table, output_table, reduce_by):
        return input_table, output_table, reduce_by
    msg = (
        "operation_config must set input_table, output_table, and reduce_by; "
        "expected at client.operations.map_reduce.{input_table,output_table,reduce_by}"
    )
    raise ValueError(msg)


def _assert_exclusive_map_job_aliases(
    mapper: object,
    map_job: object,
) -> None:
    if mapper is not None and map_job is not None and mapper != map_job:
        msg = "Both 'mapper' and 'map_job' are set with different values; use only one"
        raise ValueError(msg)


def _assert_exclusive_reduce_job_aliases(
    reducer: object,
    reduce_job: object,
) -> None:
    if reducer is not None and reduce_job is not None and reducer != reduce_job:
        msg = "Both 'reducer' and 'reduce_job' are set with different values; use only one"
        raise ValueError(msg)


def _resolve_map_reduce_legs(
    mapper: object,
    reducer: object,
    map_job: object,
    reduce_job: object,
) -> tuple[object, object]:
    _assert_exclusive_map_job_aliases(mapper, map_job)
    _assert_exclusive_reduce_job_aliases(reducer, reduce_job)
    resolved_mapper = map_job if map_job is not None else mapper
    resolved_reducer = reduce_job if reduce_job is not None else reducer
    require_consistent_map_reduce_legs(resolved_mapper, resolved_reducer)
    return resolved_mapper, resolved_reducer


def _warn_deprecated_map_reduce_aliases(
    mapper: object,
    map_job: object,
    reducer: object,
    reduce_job: object,
) -> None:
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


def _prepare_map_reduce_dependencies(
    context: "StageContext",
    operation_config: DictConfig,
    mapper: object,
    reducer: object,
) -> tuple[list[tuple[str, str]], object, object]:
    builder = TarArchiveDependencyBuilder()
    dep = builder.build_dependencies(
        operation_type="map_reduce",
        stage_dir=context.stage_dir,
        archive_name="source.tar.gz",
        build_folder=context.deps.pipeline_config.pipeline.build_folder,
        operation_config=operation_config,
        stage_config=context.config,
        logger=context.logger,
        mapper=mapper,
        reducer=reducer,
    )
    if dep.mapper_command is not None and dep.reducer_command is not None:
        context.logger.info(
            "Using tar bootstrap commands for map-reduce mapper and reducer legs",
        )
        return dep.dependencies, dep.mapper_command, dep.reducer_command
    if dep.mapper_command is None and dep.reducer_command is None:
        return dep.dependencies, mapper, reducer
    msg = (
        "Internal error: partial map-reduce tar bootstrap (only one leg set); "
        "expected both or neither."
    )
    raise RuntimeError(msg)


def _build_map_reduce_spec_kwargs(
    operation_config: DictConfig,
    logger: logging.Logger,
) -> dict[str, Any]:
    spec_kwargs: dict[str, Any] = {}
    if operation_config.get("map_job_count") is not None:
        spec_kwargs["map_job_count"] = operation_config.map_job_count

    od = operation_config.get("operation_description")
    if od:
        if isinstance(od, str):
            logger.info("Operation label: %s", od)
            spec_kwargs["title"] = od
        else:
            spec_kwargs["operation_description"] = OmegaConf.to_container(
                od,
                resolve=True,
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
            "environment_public_keys",
            "use_plain_environment_for_secrets",
        },
    )
    spec_kwargs.update(passthrough)
    return spec_kwargs


def _parse_reduce_io(operation_config: DictConfig) -> tuple[str, str, list[str]]:
    input_table = str(operation_config.get("input_table") or "")
    output_table = str(operation_config.get("output_table") or "")
    reduce_by = _str_list_from_config(operation_config.get("reduce_by"))
    return input_table, output_table, reduce_by


def _require_reduce_tables(
    input_table: str,
    output_table: str,
    reduce_by: list[str],
) -> None:
    if input_table and output_table and reduce_by:
        return
    msg = (
        "operation_config must set input_table, output_table, and reduce_by; "
        "expected at client.operations.reduce.{input_table,output_table,reduce_by}"
    )
    raise ValueError(msg)


def _reduce_description_kwargs(
    operation_config: DictConfig,
    logger: logging.Logger,
) -> dict[str, Any]:
    reduce_kw: dict[str, Any] = {}
    rod = operation_config.get("operation_description")
    if not rod:
        return reduce_kw
    if isinstance(rod, str):
        logger.info("Operation label: %s", rod)
        reduce_kw["title"] = rod
        return reduce_kw
    reduce_kw["operation_description"] = OmegaConf.to_container(rod, resolve=True)
    return reduce_kw


def _resolve_reduce_leg(reducer: object, job: object) -> object:
    if reducer is not None and job is not None and reducer != job:
        msg = "Both 'reducer' and 'job' are set with different values; use only one"
        raise ValueError(msg)
    return job if job is not None else reducer


def _tar_reduce_dependencies(
    context: "StageContext",
    operation_config: DictConfig,
    reducer: object,
    logger: logging.Logger,
) -> tuple[list[tuple[str, str]], object]:
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
        logger.info("Using tar bootstrap command for reduce leg")
        reducer = dep.reducer_command
    return dependencies, reducer


def run_map_reduce(
    context: "StageContext",
    operation_config: DictConfig,
    mapper: object = None,
    reducer: object = None,
    output_schema: "TableSchema | None" = None,
    map_job: object = None,
    reduce_job: object = None,
) -> bool:
    """Run a YT map-reduce operation and wait for completion.

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
    _warn_deprecated_map_reduce_aliases(mapper, map_job, reducer, reduce_job)
    logger = context.logger
    log_header(
        logger,
        "Map-Reduce Operation",
        f"Input: {operation_config.get('input_table')} -> Output: {operation_config.get('output_table')}",
    )

    input_table, output_table, reduce_by = _validate_map_reduce_inputs(operation_config)

    env = build_operation_environment(
        context=context,
        operation_config=operation_config,
        logger=logger,
        include_stage_name=True,
        include_tokenizer_artifact=True,
    )
    resources = extract_operation_resources(operation_config, logger)

    mapper, reducer = _resolve_map_reduce_legs(mapper, reducer, map_job, reduce_job)
    dependencies, mapper, reducer = _prepare_map_reduce_dependencies(
        context=context,
        operation_config=operation_config,
        mapper=mapper,
        reducer=reducer,
    )

    docker_auth = docker_auth_from_op_config(operation_config, env)

    sort_by = _str_list_from_config(operation_config.get("sort_by"))
    max_failed_jobs = extract_max_failed_jobs(operation_config, logger)

    spec_kwargs = _build_map_reduce_spec_kwargs(operation_config, logger)

    operation = context.deps.yt_client.run_map_reduce(
        mapper=mapper,
        reducer=reducer,
        input_table=input_table,
        output_table=output_table,
        reduce_by=reduce_by,
        files=dependencies,
        resources=resources,
        env=env,
        sort_by=sort_by or None,
        output_schema=output_schema,
        max_failed_jobs=max_failed_jobs,
        docker_auth=docker_auth,
        **extract_secure_env_client_kwargs(operation_config),
        **spec_kwargs,
    )

    return _wait_operation_with_log(
        context,
        operation,
        logger,
        success_msg="Map-reduce operation completed successfully",
        failure_msg="Map-reduce operation failed",
    )


def run_reduce(
    context: "StageContext",
    operation_config: DictConfig,
    reducer: object = None,
    output_schema: "TableSchema | None" = None,
    job: object = None,
) -> bool:
    """Run a YT reduce-only operation and wait for completion.

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

    input_table, output_table, reduce_by = _parse_reduce_io(operation_config)
    _require_reduce_tables(input_table, output_table, reduce_by)

    env = build_operation_environment(
        context=context,
        operation_config=operation_config,
        logger=logger,
        include_stage_name=True,
        include_tokenizer_artifact=True,
    )
    resources = extract_operation_resources(operation_config, logger)

    reducer = _resolve_reduce_leg(reducer, job)
    dependencies, reducer = _tar_reduce_dependencies(
        context,
        operation_config,
        reducer,
        logger,
    )

    docker_auth = docker_auth_from_op_config(operation_config, env)

    max_failed_jobs = extract_max_failed_jobs(operation_config, logger)

    reduce_kw = _reduce_description_kwargs(operation_config, logger)

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
            "environment_public_keys",
            "use_plain_environment_for_secrets",
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
        **extract_secure_env_client_kwargs(operation_config),
        **reduce_kw,
    )

    return _wait_operation_with_log(
        context,
        operation,
        logger,
        success_msg="Reduce operation completed successfully",
        failure_msg="Reduce operation failed",
    )
