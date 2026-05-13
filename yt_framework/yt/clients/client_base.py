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
from .yql_ops_abc import YqlOpsABC

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
        """Run a map operation on YT.

        Args:
            command: Legacy mapper job argument (TypedJob instance or command string).
            input_table: Input table path
            output_table: Output table path
            files: List of (yt_path, local_path) tuples for files to upload
            resources: Operation resources (pool, memory, CPU, GPU, etc.)
            env: Environment variables dictionary
            output_schema: Optional YT TableSchema for typed output table
            max_failed_jobs: Maximum number of failed jobs before operation fails
            docker_auth: Optional Docker authentication dictionary
            job: Preferred mapper job argument (TypedJob instance or command string).
            append: If True, append mapper output to an existing output table.
            **kwargs: Extra options forwarded to the underlying YT client.

        """

    @abstractmethod
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
        """Run a map-reduce operation on YT.

        Args:
            mapper: Mapper job (TypedJob instance or command string).
            reducer: Reducer job (TypedJob instance or command string).
            input_table: Input table path.
            output_table: Output table path.
            reduce_by: List of columns to reduce by.
            files: List of (yt_path, local_path) tuples for dependencies.
            resources: Operation resources.
            env: Environment variables.
            sort_by: Optional sort columns before reduce.
            output_schema: Optional output table schema.
            max_failed_jobs: Maximum failed jobs allowed.
            docker_auth: Optional Docker auth.
            map_job: Preferred mapper job alias (TypedJob instance or command string).
            reduce_job: Preferred reducer job alias (TypedJob instance or command string).
            **kwargs: Extra options applied to the spec builder where supported.

        """

    @abstractmethod
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
        """Run a reduce-only operation on YT.

        Args:
            reducer: Reducer job (TypedJob instance or command string).
            input_table: Input table path.
            output_table: Output table path.
            reduce_by: List of columns to reduce by.
            files: List of (yt_path, local_path) tuples for dependencies.
            resources: Operation resources.
            env: Environment variables.
            output_schema: Optional output table schema.
            max_failed_jobs: Maximum failed jobs allowed.
            docker_auth: Optional Docker auth.
            job: Preferred reducer job alias (TypedJob instance or command string).
            **kwargs: Extra options applied to the spec builder where supported.

        """

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
    def run_vanilla(
        self,
        command: object,
        files: list[tuple[str, str]],
        env: dict[str, str],
        task_name: str,
        job: object = None,
        **kwargs: object,
    ) -> Operation | None:
        """Run a vanilla operation on YT.

        Args:
            command: Legacy vanilla command argument (e.g., "python3 vanilla.py")
            files: List of (yt_path, local_path) tuples for files to upload
            env: Environment variables dictionary
            task_name: Task name for the operation
            job: Preferred vanilla command alias.
            *args: Additional positional arguments (implementation-specific)
            **kwargs: Additional keyword arguments. Common kwargs include:
                - resources: OperationResources instance (pool, memory, CPU, GPU, etc.)
                - docker_auth: Optional Docker authentication dictionary
                - max_failed_jobs: Maximum number of failed jobs before operation fails

        Returns:
            Operation object or None when submission fails.

        """
