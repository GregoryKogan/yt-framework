"""Internal helpers for :mod:`yt_framework.operations.command_ops.map_reduce`."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from omegaconf import DictConfig, ListConfig

from yt_framework.job_command import require_consistent_map_reduce_legs
from yt_framework.utils.logging import log_success

if TYPE_CHECKING:
    import logging

    from yt.wrapper import Operation

    from yt_framework.operations.stage_contracts import StageContext


def wait_operation_with_log(
    context: StageContext,
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


def str_list_from_config(value: object) -> list[str]:
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


def validate_map_reduce_inputs(
    operation_config: DictConfig,
) -> tuple[str, str, list[str]]:
    input_table = str(operation_config.get("input_table") or "")
    output_table = str(operation_config.get("output_table") or "")
    reduce_by = str_list_from_config(operation_config.get("reduce_by"))
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


def resolve_map_reduce_legs(
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


def warn_deprecated_map_reduce_aliases(
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
