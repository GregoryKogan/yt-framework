"""Tests for yt_framework.yt.client_base (OperationResources + BaseYTClient helpers)."""

import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from yt_framework.yt.client_base import BaseYTClient, OperationResources


def _null_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


def test_operation_resources_rejects_non_positive_memory_gb() -> None:
    with pytest.raises(ValueError, match="memory_gb must be set to a positive integer"):
        OperationResources(memory_gb=0)


def test_operation_resources_rejects_non_positive_cpu_limit() -> None:
    with pytest.raises(ValueError, match="cpu_limit must be set to a positive integer"):
        OperationResources(cpu_limit=0)


def test_operation_resources_rejects_negative_gpu_limit() -> None:
    with pytest.raises(
        ValueError, match="gpu_limit must be set to a non-negative integer"
    ):
        OperationResources(gpu_limit=-1)


def test_operation_resources_rejects_non_positive_job_count() -> None:
    with pytest.raises(ValueError, match="job_count must be set to a positive integer"):
        OperationResources(job_count=0)


class _StubBaseClient(BaseYTClient):
    """Concrete client with configurable ``read_table`` for ``_get_table_columns`` tests."""

    def __init__(
        self,
        logger: logging.Logger,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(logger)
        self._rows: list[dict[str, Any]] = (
            rows if rows is not None else [{"visible": 1}]
        )

    def create_path(
        self,
        path: str,
        node_type: Any = "map_node",
    ) -> None:
        del path, node_type

    def exists(self, path: str) -> bool:
        del path
        return False

    def write_table(
        self,
        table_path: str,
        rows: list[dict[str, Any]],
        append: bool = False,
        replication_factor: int = 1,
    ) -> None:
        del table_path, rows, append, replication_factor

    def read_table(self, table_path: str) -> list[dict[str, Any]]:
        del table_path
        return list(self._rows)

    def row_count(self, table_path: str) -> int:
        del table_path
        return len(self._rows)

    def run_yql(
        self,
        query: str,
        pool: str = "default",
        max_row_weight: str | None = None,
    ) -> None:
        del query, pool, max_row_weight

    def join_tables(
        self,
        left_table: str,
        right_table: str,
        output_table: str,
        on: Any,
        how: Any = "left",
        select_columns: list[str] | None = None,
        dry_run: bool = False,
    ) -> str | None:
        del left_table, right_table, output_table, on, how, select_columns, dry_run
        return None

    def filter_table(
        self,
        input_table: str,
        output_table: str,
        condition: str,
        dry_run: bool = False,
    ) -> str | None:
        del input_table, output_table, condition, dry_run
        return None

    def select_columns(
        self,
        input_table: str,
        output_table: str,
        columns: list[str],
        dry_run: bool = False,
    ) -> str | None:
        del input_table, output_table, columns, dry_run
        return None

    def group_by_aggregate(
        self,
        input_table: str,
        output_table: str,
        group_by: Any,
        aggregations: dict[str, Any],
        dry_run: bool = False,
    ) -> str | None:
        del input_table, output_table, group_by, aggregations, dry_run
        return None

    def union_tables(
        self,
        tables: list[str],
        output_table: str,
        dry_run: bool = False,
    ) -> str | None:
        del tables, output_table, dry_run
        return None

    def distinct(
        self,
        input_table: str,
        output_table: str,
        columns: list[str] | None = None,
        dry_run: bool = False,
    ) -> str | None:
        del input_table, output_table, columns, dry_run
        return None

    def sort_table(
        self,
        input_table: str,
        output_table: str,
        order_by: Any,
        ascending: bool = True,
        dry_run: bool = False,
    ) -> str | None:
        del input_table, output_table, order_by, ascending, dry_run
        return None

    def limit_table(
        self,
        input_table: str,
        output_table: str,
        limit: int,
        dry_run: bool = False,
    ) -> str | None:
        del input_table, output_table, limit, dry_run
        return None

    def upload_file(
        self,
        local_path: Path,
        yt_path: str,
        create_parent_dir: bool = False,
    ) -> None:
        del local_path, yt_path, create_parent_dir

    def upload_directory(
        self,
        local_dir: Path,
        yt_dir: str,
        pattern: str = "*",
    ) -> list[str]:
        del local_dir, yt_dir, pattern
        return []

    def run_map(
        self,
        command: Any,
        input_table: str,
        output_table: str,
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: Any = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: Any = None,
        append: bool = False,
        **kwargs: Any,
    ) -> Any:
        del (
            command,
            input_table,
            output_table,
            files,
            resources,
            env,
            output_schema,
            max_failed_jobs,
            docker_auth,
            job,
            append,
            kwargs,
        )
        return MagicMock()

    def run_map_reduce(
        self,
        mapper: Any,
        reducer: Any,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        sort_by: list[str] | None = None,
        output_schema: Any = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        map_job: Any = None,
        reduce_job: Any = None,
        **kwargs: Any,
    ) -> Any:
        del (
            mapper,
            reducer,
            input_table,
            output_table,
            reduce_by,
            files,
            resources,
            env,
            sort_by,
            output_schema,
            max_failed_jobs,
            docker_auth,
            map_job,
            reduce_job,
            kwargs,
        )
        return MagicMock()

    def run_reduce(
        self,
        reducer: Any,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: Any = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: Any = None,
        **kwargs: Any,
    ) -> Any:
        del (
            reducer,
            input_table,
            output_table,
            reduce_by,
            files,
            resources,
            env,
            output_schema,
            max_failed_jobs,
            docker_auth,
            job,
            kwargs,
        )
        return MagicMock()

    def run_sort(
        self,
        table_path: str,
        sort_by: list[str],
        pool: str | None = None,
        pool_tree: str | None = None,
        **kwargs: Any,
    ) -> None:
        del table_path, sort_by, pool, pool_tree, kwargs

    def run_vanilla(
        self,
        command: Any,
        files: list[tuple[str, str]],
        env: dict[str, str],
        task_name: str,
        job: Any = None,
        **kwargs: Any,
    ) -> Any:
        del command, files, env, task_name, job, kwargs
        return MagicMock()


def test_base_yt_client_get_table_columns_strips_leading_underscore_columns() -> None:
    c = _StubBaseClient(
        _null_logger("tests.client_base.cols"),
        rows=[{"keep": 1, "_yql_x": 2}],
    )
    assert c._get_table_columns("//tmp/t") == ["keep"]


def test_base_yt_client_get_table_columns_keeps_all_keys_when_only_internal_like_names() -> (
    None
):
    c = _StubBaseClient(
        _null_logger("tests.client_base.cols_fb"),
        rows=[{"_only": 1}],
    )
    assert c._get_table_columns("//tmp/t") == ["_only"]


def test_base_yt_client_get_table_columns_raises_when_table_has_no_rows() -> None:
    c = _StubBaseClient(_null_logger("tests.client_base.empty"), rows=[])
    with pytest.raises(ValueError, match="empty, cannot determine columns"):
        c._get_table_columns("//tmp/empty")


def test_base_yt_client_wait_for_operation_returns_true_when_completed() -> None:
    op = MagicMock()
    op.get_state.return_value = "completed"
    c = _StubBaseClient(_null_logger("tests.client_base.wait_ok"))
    assert c.wait_for_operation(op) is True
    op.wait.assert_called_once()


def test_base_yt_client_wait_for_operation_returns_false_when_state_not_completed() -> (
    None
):
    op = MagicMock()
    op.get_state.return_value = "failed"
    op.get_error.return_value = "e"
    c = _StubBaseClient(_null_logger("tests.client_base.wait_bad"))
    assert c.wait_for_operation(op) is False


def test_base_yt_client_wait_for_operation_returns_false_when_wait_raises() -> None:
    op = MagicMock()
    op.wait.side_effect = RuntimeError("cluster hiccup")
    c = _StubBaseClient(_null_logger("tests.client_base.wait_exc"))
    assert c.wait_for_operation(op) is False


def test_stub_base_client_run_yql_accepts_max_row_weight_override() -> None:
    c = _StubBaseClient(_null_logger("tests.client_base.yql_mrw"))
    assert c.run_yql("SELECT 1", max_row_weight="64M") is None
