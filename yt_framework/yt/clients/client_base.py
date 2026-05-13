"""Abstract base class for YT client implementations.

Concrete dev and prod clients inherit from ``BaseYTClient``.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional

from yt.wrapper import Operation

from .client_wait import ClientOperationWaitMixin
from .operation_resources import OperationResources
from .operation_specs import (
    MapReduceSubmitSpec,
    MapSubmitSpec,
    ReduceSubmitSpec,
    VanillaSubmitSpec,
    docker_auth_tuple,
    env_pairs_tuple,
    extras_tuple,
    file_pairs_tuple,
)
from .yql.yql_ops_abc import YqlOpsABC

if TYPE_CHECKING:
    from yt.wrapper.schema import TableSchema


class BaseYTClient(ClientOperationWaitMixin, YqlOpsABC, ABC):
    """Abstract base class for YT client implementations.

    Defines the interface that both production and development clients must implement.
    """

    def __init__(
        self,
        logger: logging.Logger,
        pipeline_dir: Path | None = None,
    ) -> None:
        """Initialize base YT client.

        Args:
            logger: Logger instance
            pipeline_dir: Optional pipeline directory (used in dev mode)

        """
        self.logger = logger
        self.pipeline_dir = pipeline_dir

    @abstractmethod
    def create_path(
        self,
        path: str,
        node_type: Literal[
            "table",
            "file",
            "map_node",
            "list_node",
            "document",
        ] = "map_node",
    ) -> None:
        """Create a path in YT.

        Args:
            path: YT path to create
            node_type: Type of node to create (default: "map_node")

        """

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a path exists in YT.

        Args:
            path: YT path to check

        Returns:
            True if path exists, False otherwise

        """

    @abstractmethod
    def write_table(
        self,
        table_path: str,
        rows: list[dict[str, Any]],
        *,
        append: bool = False,
        replication_factor: int = 1,
    ) -> None:
        """Write rows to a YT table.

        Args:
            table_path: YT table path
            rows: List of dictionaries representing table rows
            append: If True, append to existing table (default: False)
            replication_factor: Replication factor for the table (default: 1)

        Note:
            Subclasses may accept additional parameters (e.g., make_parents in YTProdClient).

        """

    @abstractmethod
    def read_table(self, table_path: str) -> list[dict[str, Any]]:
        """Read rows from a YT table.

        Args:
            table_path: YT table path

        Returns:
            List of dictionaries representing table rows

        """

    @abstractmethod
    def row_count(self, table_path: str) -> int:
        """Get number of rows in a YT table.

        Args:
            table_path: YT table path

        Returns:
            Number of rows in the table

        """

    def _get_table_columns(self, table_path: str) -> list[str]:
        """Get column names from a table by reading one row.

        This is a helper method used internally by convenience methods.
        Subclasses should implement this method.

        Args:
            table_path: Path to YT table

        Returns:
            List of column names

        Raises:
            ValueError: If table is empty or cannot be read

        """
        # Default implementation - subclasses should override
        rows = self.read_table(table_path)
        if not rows:
            msg = f"Table {table_path} is empty, cannot determine columns"
            raise ValueError(msg)
        # Get column names from first row
        columns = list(rows[0].keys())
        # Filter out internal YQL columns like _other, _yql_column_*
        columns = [col for col in columns if not col.startswith("_")]
        if not columns:
            # If all columns were filtered out, use all keys (fallback)
            columns = list(rows[0].keys())
        return columns

    @abstractmethod
    def run_yql(
        self,
        query: str,
        pool: str = "default",
        max_row_weight: str | None = None,
    ) -> None:
        """Execute a YQL query on YT cluster.

        Args:
            query: YQL query string to execute
            pool: YT pool name (default: 'default')
            max_row_weight: Optional max row weight override (default uses project default)

        Raises:
            Exception: If query execution fails

        """

    @abstractmethod
    def upload_file(
        self,
        local_path: Path,
        yt_path: str,
        *,
        create_parent_dir: bool = False,
    ) -> None:
        """Upload a file to YT.

        Args:
            local_path: Local file path to upload
            yt_path: YT destination path
            create_parent_dir: If True, create parent directory if it doesn't exist (default: False)

        """

    @abstractmethod
    def upload_directory(
        self,
        local_dir: Path,
        yt_dir: str,
        pattern: str = "*",
    ) -> list[str]:
        """Upload a directory to YT.

        Recursively uploads all files from a local directory to a YT directory,
        respecting .ytignore patterns if present.

        Args:
            local_dir: Local directory path to upload
            yt_dir: YT destination directory path
            pattern: File pattern to match (default: "*" for all files)

        Returns:
            List of uploaded YT file paths

        """

    @abstractmethod
    def run_map_submit(self, spec: MapSubmitSpec) -> Operation:
        """Submit a map operation using a :class:`MapSubmitSpec`."""

    def run_map(
        self,
        command: object,
        input_table: str,
        output_table: str,
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: Optional["TableSchema"] = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: object = None,
        *,
        append: bool = False,
        **kwargs: object,
    ) -> Operation:
        """Run a map operation on YT (delegates to :meth:`run_map_submit`)."""
        return self.run_map_submit(
            MapSubmitSpec(
                command=command,
                input_table=input_table,
                output_table=output_table,
                files=file_pairs_tuple(files),
                resources=resources,
                env=env_pairs_tuple(env),
                output_schema=output_schema,
                max_failed_jobs=max_failed_jobs,
                docker_auth=docker_auth_tuple(docker_auth),
                job=job,
                append=append,
                extras=extras_tuple(dict(kwargs)),
            ),
        )

    @abstractmethod
    def run_map_reduce_submit(self, spec: MapReduceSubmitSpec) -> Operation:
        """Submit a map-reduce operation using a :class:`MapReduceSubmitSpec`."""

    def run_map_reduce(
        self,
        mapper: object,
        reducer: object,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        sort_by: list[str] | None = None,
        output_schema: Optional["TableSchema"] = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        map_job: object = None,
        reduce_job: object = None,
        **kwargs: object,
    ) -> Operation:
        """Run a map-reduce operation (delegates to :meth:`run_map_reduce_submit`)."""
        return self.run_map_reduce_submit(
            MapReduceSubmitSpec(
                mapper=mapper,
                reducer=reducer,
                input_table=input_table,
                output_table=output_table,
                reduce_by=tuple(reduce_by),
                files=file_pairs_tuple(files),
                resources=resources,
                env=env_pairs_tuple(env),
                sort_by=None if sort_by is None else tuple(sort_by),
                output_schema=output_schema,
                max_failed_jobs=max_failed_jobs,
                docker_auth=docker_auth_tuple(docker_auth),
                map_job=map_job,
                reduce_job=reduce_job,
                extras=extras_tuple(dict(kwargs)),
            ),
        )

    @abstractmethod
    def run_reduce_submit(self, spec: ReduceSubmitSpec) -> Operation:
        """Submit a reduce-only operation using a :class:`ReduceSubmitSpec`."""

    def run_reduce(
        self,
        reducer: object,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: Optional["TableSchema"] = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: object = None,
        **kwargs: object,
    ) -> Operation:
        """Run a reduce-only operation (delegates to :meth:`run_reduce_submit`)."""
        return self.run_reduce_submit(
            ReduceSubmitSpec(
                reducer=reducer,
                input_table=input_table,
                output_table=output_table,
                reduce_by=tuple(reduce_by),
                files=file_pairs_tuple(files),
                resources=resources,
                env=env_pairs_tuple(env),
                output_schema=output_schema,
                max_failed_jobs=max_failed_jobs,
                docker_auth=docker_auth_tuple(docker_auth),
                job=job,
                extras=extras_tuple(dict(kwargs)),
            ),
        )

    @abstractmethod
    def run_sort(
        self,
        table_path: str,
        sort_by: list[str],
        pool: str | None = None,
        pool_tree: str | None = None,
        **kwargs: object,
    ) -> None:
        """Sort a table in place by the given columns.

        Args:
            table_path: Table to sort.
            sort_by: List of column names (or SortColumn objects) to sort by.
            pool: Scheduler pool to run in.
            pool_tree: Pool tree to run in.
            **kwargs: Extra options forwarded to the underlying sort call.

        """

    @abstractmethod
    def run_vanilla_submit(self, spec: VanillaSubmitSpec) -> Operation | None:
        """Submit a vanilla operation using a :class:`VanillaSubmitSpec`."""

    def run_vanilla(
        self,
        command: object,
        files: list[tuple[str, str]],
        env: dict[str, str],
        task_name: str,
        job: object = None,
        **kwargs: object,
    ) -> Operation | None:
        """Run a vanilla operation (delegates to :meth:`run_vanilla_submit`)."""
        kw = dict(kwargs)
        resources_obj = kw.pop("resources", None)
        if not isinstance(resources_obj, OperationResources):
            msg = "run_vanilla requires resources=OperationResources in kwargs"
            raise TypeError(msg)
        docker_auth = kw.pop("docker_auth", None)
        _mfj = kw.pop("max_failed_jobs", 1)
        max_failed_jobs = _mfj if isinstance(_mfj, int) else 1
        docker_map = docker_auth if isinstance(docker_auth, dict) else None
        return self.run_vanilla_submit(
            VanillaSubmitSpec(
                command=command,
                files=file_pairs_tuple(files),
                env=env_pairs_tuple(env),
                task_name=task_name,
                resources=resources_obj,
                job=job,
                max_failed_jobs=max_failed_jobs,
                docker_auth=docker_auth_tuple(docker_map),
                extras=extras_tuple(kw),
            ),
        )
