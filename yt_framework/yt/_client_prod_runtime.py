"""Internal helpers for ``YTProdClient`` (cyclomatic-complexity isolation)."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal, NoReturn, cast

from yt.wrapper import FilePath, MapSpecBuilder, TablePath, TypedJob, VanillaSpecBuilder
from yt.wrapper import format as yt_format
from yt.wrapper.spec_builders import MapReduceSpecBuilder, ReduceSpecBuilder

from yt_framework.yt.clients.client_base import OperationResources
from yt_framework.yt.max_row_weight import parse_max_row_weight_bytes

# YtClient.run_operation() only accepts these keyword args; everything else must be
# applied via SpecBuilder chain methods (weight, title, description, …).
_RUN_OPERATION_KWARGS = frozenset(
    {"enable_optimizations", "run_operation_mutation_id", "sync"},
)


def _raise_runtime_error(message: str) -> NoReturn:
    """Raise RuntimeError from a nested function (TRY301)."""
    raise RuntimeError(message)


def _optional_str_kw(raw: object) -> str | None:
    if raw is None:
        return None
    return str(raw)


def _spec_builder_secure_vault(
    spec_builder: object, vault: Mapping[str, Any]
) -> object:
    if not vault:
        return spec_builder
    sec = cast("Any", getattr(spec_builder, "secure_vault", None))
    if callable(sec):
        return sec(dict(vault))
    return spec_builder


def _pop_run_operation_kwargs(
    kwargs: dict[str, Any], allowed: frozenset[str]
) -> tuple[dict[str, Any], dict[str, Any]]:
    kw = dict(kwargs)
    run_op: dict[str, Any] = {}
    for key in list(kw.keys()):
        if key in allowed:
            run_op[key] = kw.pop(key)
    return kw, run_op


def apply_one_spec_kw_builder(
    spec_builder: object,
    kw: dict[str, Any],
    key: str,
    val: Any,
    *,
    allowed_run_op: frozenset[str],
) -> object:
    if key == "max_row_weight":
        kw.pop(key, None)
        return spec_builder
    meth = getattr(spec_builder, key, None)
    if meth is not None and callable(meth):
        kw.pop(key)
        return cast("Any", meth)(val)
    msg = (
        f"Unknown YT operation option {key!r}: not a SpecBuilder method on "
        f"{type(spec_builder).__name__} and not one of {sorted(allowed_run_op)}."
    )
    raise ValueError(msg)


def apply_spec_opts_run_kwargs(
    spec_builder: object,
    kwargs: dict[str, Any],
) -> tuple[Any, dict[str, Any]]:
    """Apply SpecBuilder kwargs and split out ``run_operation`` options."""
    kw, run_op = _pop_run_operation_kwargs(kwargs, _RUN_OPERATION_KWARGS)
    for key, val in list(kw.items()):
        spec_builder = apply_one_spec_kw_builder(
            spec_builder,
            kw,
            key,
            val,
            allowed_run_op=_RUN_OPERATION_KWARGS,
        )
    return spec_builder, run_op


def _apply_command_leg_format(
    leg_builder: object,
    leg: object,
) -> object:
    """Configure wire format for command legs only."""
    if isinstance(leg, TypedJob):
        return leg_builder
    return cast("Any", leg_builder).format(yt_format.JsonFormat(encode_utf8=False))


def apply_max_row_weight_builder(
    spec_builder: object,
    max_row_weight: str | None,
) -> object:
    """Apply max row weight to spec builder when supported."""
    if max_row_weight is None:
        return spec_builder
    max_row_weight_bytes = parse_max_row_weight_bytes(max_row_weight)
    sb = cast("Any", spec_builder)
    table_writer = getattr(sb, "table_writer", None)
    if callable(table_writer):
        return table_writer({"max_row_weight": max_row_weight_bytes})
    job_io = getattr(sb, "job_io", None)
    if callable(job_io):
        return job_io({"table_writer": {"max_row_weight": max_row_weight_bytes}})
    return spec_builder


def read_required_yt_secret(
    secrets: Mapping[str, str], *, key: str, missing_message: str
) -> str:
    val = secrets.get(key)
    if not val:
        raise ValueError(missing_message)
    return str(val)


def disable_yt_proxy_discovery(
    client: Any, logger: logging.Logger, yt_proxy: str
) -> None:
    try:
        if "proxy" in client.config:
            client.config["proxy"]["enable_proxy_discovery"] = False
            logger.debug(
                "YT Client initialized with proxy: %s (proxy discovery disabled)",
                yt_proxy,
            )
        else:
            logger.debug("YT Client initialized with proxy: %s", yt_proxy)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Could not disable proxy discovery: %s. Continuing with default settings.",
            e,
        )
        logger.debug("YT Client initialized with proxy: %s", yt_proxy)


def prod_create_table_parent(
    *,
    make_parents: bool,
    table_path: str,
    create_path: Callable[
        [str, Literal["table", "file", "map_node", "list_node", "document"]],
        None,
    ],
    logger: logging.Logger,
) -> None:
    if not make_parents or "/" not in table_path:
        return
    parent_dir = "/".join(table_path.rstrip("/").split("/")[:-1])
    if not parent_dir:
        return
    logger.debug("Ensuring parent directory exists: %s", parent_dir)
    create_path(parent_dir, "map_node")


def prod_write_table_replace_create(
    client: Any,
    *,
    append: bool,
    table_path: str,
    replication_factor: int,
) -> None:
    if append:
        return
    if client.exists(table_path):
        client.remove(table_path, force=True)
    client.create(
        "table",
        table_path,
        attributes={"replication_factor": replication_factor},
        ignore_existing=True,
    )


def _prod_upload_directory_step(
    local_file: Path,
    local_dir: Path,
    yt_dir: str,
    ignore_matcher: Any,
    *,
    create_path: Callable[
        [str, Literal["table", "file", "map_node", "list_node", "document"]],
        None,
    ],
    upload_file: Callable[[Path, str], None],
) -> tuple[str, str | None]:
    """Return ``(skipped|ignored|uploaded, yt_path_or_none)``."""
    if not local_file.is_file():
        return ("skipped", None)
    if ignore_matcher.should_ignore(local_file):
        return ("ignored", None)
    rel_path = local_file.relative_to(local_dir)
    yt_path = f"{yt_dir}/{rel_path}".replace("\\", "/")
    parent = "/".join(yt_path.split("/")[:-1])
    if parent:
        create_path(parent, "map_node")
    upload_file(local_file, yt_path)
    return ("uploaded", yt_path)


def _prod_process_upload_directory_file(
    local_file: Path,
    uploaded: list[str],
    local_dir: Path,
    yt_dir: str,
    ignore_matcher: Any,
    *,
    create_path: Callable[
        [str, Literal["table", "file", "map_node", "list_node", "document"]],
        None,
    ],
    upload_file: Callable[[Path, str], None],
    logger: logging.Logger,
) -> int:
    """Upload one candidate; return 1 if ignored, else 0."""
    kind, path = _prod_upload_directory_step(
        local_file,
        local_dir,
        yt_dir,
        ignore_matcher,
        create_path=create_path,
        upload_file=upload_file,
    )
    if kind == "ignored":
        logger.debug("Ignoring file (matched .ytignore): %s", local_file)
        return 1
    if kind == "uploaded":
        if path is None:
            msg = "internal: map upload step returned uploaded without path"
            raise RuntimeError(msg)
        uploaded.append(path)
    return 0


def prod_upload_directory_files(
    *,
    local_dir: Path,
    yt_dir: str,
    pattern: str,
    ignore_matcher: Any,
    create_path: Callable[
        [str, Literal["table", "file", "map_node", "list_node", "document"]],
        None,
    ],
    upload_file: Callable[[Path, str], None],
    logger: logging.Logger,
) -> list[str]:
    create_path(yt_dir, "map_node")
    uploaded: list[str] = []
    ignored_count = 0
    for local_file in local_dir.rglob(pattern):
        ignored_count += _prod_process_upload_directory_file(
            local_file,
            uploaded,
            local_dir,
            yt_dir,
            ignore_matcher,
            create_path=create_path,
            upload_file=upload_file,
            logger=logger,
        )
    logger.info("Uploaded %s files", len(uploaded))
    if ignored_count > 0:
        logger.info("Ignored %s files (matched .ytignore patterns)", ignored_count)
    return uploaded


def prod_merge_sort_spec_kwargs(
    kwargs: dict[str, Any],
    *,
    pool: str | None,
    pool_tree: str | None,
) -> dict[str, Any]:
    out = dict(kwargs)
    raw_spec = out.pop("spec", None)
    spec: dict[str, Any] = dict(raw_spec) if isinstance(raw_spec, dict) else {}
    if pool:
        spec["pool"] = pool
    if pool_tree:
        spec["pool_tree"] = pool_tree
    if spec:
        out["spec"] = spec
    return out


def prod_map_spec_with_vault(
    *,
    input_table: str,
    output_path: TablePath,
    resources: OperationResources,
    max_failed_jobs: int,
    mapper_job: object,
    file_paths: list[FilePath],
    public_env: dict[str, str],
    merged_vault: Mapping[str, Any],
    logger: logging.Logger,
) -> Any:
    msb: Any = MapSpecBuilder()
    spec_builder = (
        msb.pool(resources.pool)
        .resource_limits({"user_slots": resources.user_slots})
        .max_failed_job_count(max_failed_jobs)
        .job_count(resources.job_count)
        .input_table_paths([input_table])
        .output_table_paths([output_path])
    )
    if resources.pool_tree:
        spec_builder = spec_builder.pool_trees([resources.pool_tree])
        logger.debug("Set pool tree to %s", resources.pool_tree)
    mapper_builder: Any = (
        spec_builder.begin_mapper()
        .command(mapper_job)
        .file_paths(file_paths)
        .environment(public_env)
        .memory_limit(resources.memory_gb * 1024**3)
        .cpu_limit(resources.cpu_limit)
        .gpu_limit(resources.gpu_limit)
    )
    mapper_builder = _apply_command_leg_format(mapper_builder, mapper_job)
    if resources.docker_image:
        mapper_builder = mapper_builder.docker_image(resources.docker_image)
    mapper_builder.end_mapper()
    return _spec_builder_secure_vault(spec_builder, merged_vault)


def prod_vanilla_spec_with_vault(
    *,
    resources: OperationResources,
    max_failed_jobs: int,
    task_name: str,
    vanilla_job: object,
    file_paths: list[FilePath],
    public_env: dict[str, str],
    merged_vault: Mapping[str, Any],
    logger: logging.Logger,
    operation_description: object | None,
) -> Any:
    vsb: Any = VanillaSpecBuilder()
    spec_builder = (
        vsb.pool(resources.pool)
        .resource_limits({"user_slots": resources.user_slots})
        .max_failed_job_count(max_failed_jobs)
    )
    if isinstance(operation_description, dict):
        spec_builder = spec_builder.description(operation_description)
    if resources.pool_tree:
        spec_builder = spec_builder.pool_trees([resources.pool_tree])
        logger.debug("Set pool tree to %s", resources.pool_tree)
    task_builder = (
        spec_builder.begin_task(task_name)
        .command(vanilla_job)
        .file_paths(file_paths)
        .environment(public_env)
        .memory_limit(resources.memory_gb * 1024**3)
        .cpu_limit(resources.cpu_limit)
        .gpu_limit(resources.gpu_limit)
        .job_count(resources.job_count)
    )
    if resources.docker_image:
        task_builder = task_builder.docker_image(resources.docker_image)
    task_builder.end_task()
    return _spec_builder_secure_vault(spec_builder, merged_vault)


def prod_mr_open_spec_builder(
    *,
    source_table: TablePath,
    dest_table: TablePath,
    resources: OperationResources,
    max_failed_jobs: int,
    kwargs: dict[str, Any],
) -> Any:
    mrsb: Any = MapReduceSpecBuilder()
    spec_builder = (
        mrsb.input_table_paths([source_table])
        .output_table_paths([dest_table])
        .pool(resources.pool)
        .max_failed_job_count(max_failed_jobs)
    )
    if resources.pool_tree:
        spec_builder = spec_builder.pool_trees([resources.pool_tree])
    if resources.user_slots:
        spec_builder = spec_builder.resource_limits(
            {"user_slots": resources.user_slots},
        )
    od = kwargs.pop("operation_description", None)
    if isinstance(od, dict):
        spec_builder = spec_builder.description(od)
    return spec_builder


def prod_map_reduce_after_legs(
    spec_builder: Any,
    merged_vault: Mapping[str, Any],
    reduce_by: list[str],
    sort_by: list[str] | None,
    kwargs: dict[str, Any],
) -> Any:
    spec_builder = cast("Any", _spec_builder_secure_vault(spec_builder, merged_vault))
    spec_builder = spec_builder.reduce_by(reduce_by)
    if sort_by:
        spec_builder = spec_builder.sort_by(sort_by)
    map_job_count = kwargs.pop("map_job_count", None)
    if map_job_count is not None:
        spec_builder = spec_builder.map_job_count(map_job_count)
    return spec_builder


def prod_reduce_open_spec_builder(
    *,
    source_table: TablePath,
    dest_table: TablePath,
    resources: OperationResources,
    max_failed_jobs: int,
    kwargs: dict[str, Any],
) -> Any:
    rsb: Any = ReduceSpecBuilder()
    spec_builder = (
        rsb.input_table_paths([source_table])
        .output_table_paths([dest_table])
        .pool(resources.pool)
        .max_failed_job_count(max_failed_jobs)
    )
    if resources.pool_tree:
        spec_builder = spec_builder.pool_trees([resources.pool_tree])
    if resources.user_slots:
        spec_builder = spec_builder.resource_limits(
            {"user_slots": resources.user_slots},
        )
    rod = kwargs.pop("operation_description", None)
    if isinstance(rod, dict):
        spec_builder = spec_builder.description(rod)
    return spec_builder


def prod_reduce_finish_reducer_leg(
    spec_builder: Any,
    *,
    reducer_leg: object,
    file_paths: list[FilePath],
    public_env: dict[str, str],
    resources: OperationResources,
) -> Any:
    reducer_builder: Any = (
        spec_builder.begin_reducer()
        .command(reducer_leg)
        .file_paths(file_paths)
        .environment(public_env)
        .memory_limit(resources.memory_gb * 1024**3)
        .cpu_limit(resources.cpu_limit)
        .gpu_limit(resources.gpu_limit)
    )
    reducer_builder = _apply_command_leg_format(reducer_builder, reducer_leg)
    if resources.docker_image:
        reducer_builder = reducer_builder.docker_image(resources.docker_image)
    reducer_builder.end_reducer()
    return spec_builder


def prod_submit_operation_with_kwargs(
    client: Any,
    logger: logging.Logger,
    spec_builder: Any,
    kwargs: dict[str, Any],
    *,
    none_message: str,
    log_message: str,
) -> Any:
    spec_builder = apply_max_row_weight_builder(
        spec_builder,
        _optional_str_kw(kwargs.get("max_row_weight")),
    )
    spec_builder, run_op = apply_spec_opts_run_kwargs(
        spec_builder,
        kwargs,
    )
    run_op.setdefault("sync", False)
    operation = client.run_operation(spec_builder, **run_op)
    if operation is None:
        _raise_runtime_error(none_message)
    logger.info(log_message, operation.id)
    return operation


__all__ = [
    "_apply_command_leg_format",
    "_optional_str_kw",
    "_raise_runtime_error",
    "_spec_builder_secure_vault",
    "apply_max_row_weight_builder",
    "apply_spec_opts_run_kwargs",
    "disable_yt_proxy_discovery",
    "prod_create_table_parent",
    "prod_map_reduce_after_legs",
    "prod_map_spec_with_vault",
    "prod_merge_sort_spec_kwargs",
    "prod_mr_open_spec_builder",
    "prod_reduce_finish_reducer_leg",
    "prod_reduce_open_spec_builder",
    "prod_submit_operation_with_kwargs",
    "prod_upload_directory_files",
    "prod_vanilla_spec_with_vault",
    "prod_write_table_replace_create",
    "read_required_yt_secret",
]
