"""
Sort operation for YT Framework.

Stages use this to run a YT sort operation with pool/resources drawn from config,
following the same (context, operation_config) pattern as run_map_reduce/run_reduce.
"""

import logging
from typing import TYPE_CHECKING

from omegaconf import DictConfig

from yt_framework.utils.logging import log_header, log_success
from .common import extract_operation_resources

if TYPE_CHECKING:
    from yt_framework.core.stage import StageContext


def run_sort(
    context: "StageContext",
    operation_config: DictConfig,
) -> bool:
    """
    Run a YT sort operation and wait for completion.

    Args:
        context: Stage context (deps, logger, stage_dir, config).
        operation_config: Sort-specific config.  Required keys:

            * ``input_table`` — table to sort in-place.
            * ``sort_by`` — list of column names.

            Optional keys mirror ``run_map_reduce``:

            * ``resources.pool`` / ``resources.pool_tree`` — scheduler pool.
            * ``resources.memory_limit_gb``, ``resources.cpu_limit`` — resource hints.

    Returns:
        True if the sort completed successfully.

    Example config (``client.operations.sort`` in stage ``config.yaml``)::

        sort:
          sort_by: [shard_order, mock_field]
          resources:
            pool: my_pool

    Then in the stage::

        from yt_framework.operations.sort import run_sort
        sort_cfg = OmegaConf.merge(
            self.config.client.operations.sort,
            {"input_table": intermediate_table},
        )
        run_sort(context=self.context, operation_config=sort_cfg)
    """
    logger = context.logger
    table_path = operation_config.get("input_table")
    sort_by = list(operation_config.get("sort_by") or [])

    if not table_path:
        raise ValueError(
            "operation_config must set input_table; "
            "expected at client.operations.sort.input_table"
        )
    if not sort_by:
        raise ValueError(
            "operation_config must set sort_by; "
            "expected at client.operations.sort.sort_by"
        )

    resources = extract_operation_resources(operation_config, logger)

    log_header(logger, "Sort Operation", f"Sorting {table_path} by {sort_by}")

    context.deps.yt_client.run_sort(
        table_path=table_path,
        sort_by=sort_by,
        pool=resources.pool or None,
        pool_tree=resources.pool_tree or None,
    )

    log_success(logger, "Sort completed")
    return True
