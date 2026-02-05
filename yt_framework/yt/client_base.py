"""
Base YT Client
==============

Abstract base class for YT client implementations.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union, Tuple, Literal, TYPE_CHECKING
from dataclasses import dataclass
from pathlib import Path
import logging

from yt.wrapper import Operation  # pyright: ignore[reportMissingImports]

if TYPE_CHECKING:
    from yt.wrapper.schema import TableSchema  # pyright: ignore[reportMissingImports]


@dataclass
class OperationResources:
    """Resource configuration for YT operations.
    
    This dataclass defines the computational resources allocated to YT operations
    like map and vanilla jobs. Note that in configuration files, use `memory_limit_gb`
    (not `memory_gb`) - the framework automatically maps this field.
    
    Attributes:
        pool: YT pool name for resource allocation (default: "default").
        pool_tree: Optional pool tree name (default: None).
        docker_image: Optional Docker image name for containerized execution (default: None).
        memory_gb: Memory allocation in GB (default: 4). In config files, use `memory_limit_gb`.
        cpu_limit: CPU cores allocated (default: 2).
        gpu_limit: Number of GPUs allocated (default: 0).
        job_count: Number of parallel jobs (default: 1).
        user_slots: Optional user slots limit (default: None).
        
    Raises:
        ValueError: If memory_gb, cpu_limit, or job_count are not positive integers,
                   or if gpu_limit is negative.
    """
    pool: str = "default"
    pool_tree: Optional[str] = None
    docker_image: Optional[str] = None
    memory_gb: int = 4
    cpu_limit: int = 2
    gpu_limit: int = 0
    job_count: int = 1
    user_slots: Optional[int] = None

    def __post_init__(self):
        if self.memory_gb is None or self.memory_gb <= 0:
            raise ValueError(
                f"memory_gb must be set to a positive integer, got {self.memory_gb}"
            )
        if self.cpu_limit is None or self.cpu_limit <= 0:
            raise ValueError(
                f"cpu_limit must be set to a positive integer, got {self.cpu_limit}"
            )
        if self.gpu_limit is None or self.gpu_limit < 0:
            raise ValueError(
                f"gpu_limit must be set to a non-negative integer, got {self.gpu_limit}"
            )
        if self.job_count is None or self.job_count <= 0:
            raise ValueError(
                f"job_count must be set to a positive integer, got {self.job_count}"
            )


class BaseYTClient(ABC):
    """
    Abstract base class for YT client implementations.

    Defines the interface that both production and development clients must implement.
    """

    def __init__(
        self,
        logger: logging.Logger,
        pipeline_dir: Optional[Path] = None,
    ) -> None:
        """
        Initialize base YT client.

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
            "table", "file", "map_node", "list_node", "document"
        ] = "map_node",
    ) -> None:
        """
        Create a path in YT.

        Args:
            path: YT path to create
            node_type: Type of node to create (default: "map_node")
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if a path exists in YT.

        Args:
            path: YT path to check

        Returns:
            True if path exists, False otherwise
        """
        pass

    @abstractmethod
    def write_table(
        self,
        table_path: str,
        rows: List[Dict[str, Any]],
        append: bool = False,
        replication_factor: int = 1,
    ) -> None:
        """
        Write rows to a YT table.

        Args:
            table_path: YT table path
            rows: List of dictionaries representing table rows
            append: If True, append to existing table (default: False)
            replication_factor: Replication factor for the table (default: 1)
            
        Note:
            Subclasses may accept additional parameters (e.g., make_parents in YTProdClient).
        """
        pass

    @abstractmethod
    def read_table(self, table_path: str) -> List[Dict[str, Any]]:
        """
        Read rows from a YT table.

        Args:
            table_path: YT table path

        Returns:
            List of dictionaries representing table rows
        """
        pass

    @abstractmethod
    def row_count(self, table_path: str) -> int:
        """
        Get number of rows in a YT table.

        Args:
            table_path: YT table path

        Returns:
            Number of rows in the table
        """
        pass

    def _get_table_columns(self, table_path: str) -> List[str]:
        """
        Get column names from a table by reading one row.

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
            raise ValueError(f"Table {table_path} is empty, cannot determine columns")
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
    ) -> None:
        """
        Execute a YQL query on YT cluster.

        Args:
            query: YQL query string to execute
            pool: YT pool name (default: 'default')

        Raises:
            Exception: If query execution fails
        """
        pass

    # Convenience methods for common YQL operations

    @abstractmethod
    def join_tables(
        self,
        left_table: str,
        right_table: str,
        output_table: str,
        on: Union[str, List[str], Dict[str, str]],
        how: Literal["inner", "left", "right", "full"] = "left",
        select_columns: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Join two tables using YQL.

        Args:
            left_table: Path to left table
            right_table: Path to right table
            output_table: Path to output table
            on: Join key(s) - column name(s) to join on
                - str: Same column name on both sides (e.g., "user_id")
                - List[str]: Multiple columns with same names (e.g., ["user_id", "region"])
                - Dict[str, str]: Different column names (e.g., {"left": "input_s3_path", "right": "path"})
            how: Join type - "inner", "left", "right", or "full"
            select_columns: Optional list of columns to select (with table aliases)
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise
        """
        pass

    @abstractmethod
    def filter_table(
        self,
        input_table: str,
        output_table: str,
        condition: str,
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Filter table rows using WHERE condition.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            condition: WHERE condition (e.g., "status = 'active' AND total > 100")
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise
        """
        pass

    @abstractmethod
    def select_columns(
        self,
        input_table: str,
        output_table: str,
        columns: List[str],
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Select specific columns from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            columns: List of column names to select
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise
        """
        pass

    @abstractmethod
    def group_by_aggregate(
        self,
        input_table: str,
        output_table: str,
        group_by: Union[str, List[str]],
        aggregations: Dict[str, Union[str, Tuple[str, str]]],
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Group by columns and compute aggregations.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            group_by: Column(s) to group by
            aggregations: Dict mapping output column names to aggregation functions
                         e.g., {"order_count": "count", "total_amount": "sum"}
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise
        """
        pass

    @abstractmethod
    def union_tables(
        self,
        tables: List[str],
        output_table: str,
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Union multiple tables.

        Args:
            tables: List of table paths to union
            output_table: Path to output table
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise
        """
        pass

    @abstractmethod
    def distinct(
        self,
        input_table: str,
        output_table: str,
        columns: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Get distinct rows from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            columns: Optional list of columns to select (if None, selects all)
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise
        """
        pass

    @abstractmethod
    def sort_table(
        self,
        input_table: str,
        output_table: str,
        order_by: Union[str, List[str]],
        ascending: bool = True,
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Sort table by columns.

        WARNING: Sorting large tables can be expensive. Use with caution.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            order_by: Column(s) to sort by
            ascending: Sort direction (True for ASC, False for DESC)
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise
        """
        pass

    @abstractmethod
    def limit_table(
        self,
        input_table: str,
        output_table: str,
        limit: int,
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Limit number of rows from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            limit: Maximum number of rows to return
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise
        """
        pass

    @abstractmethod
    def upload_file(
        self, local_path: Path, yt_path: str, create_parent_dir: bool = False
    ) -> None:
        """
        Upload a file to YT.

        Args:
            local_path: Local file path to upload
            yt_path: YT destination path
            create_parent_dir: If True, create parent directory if it doesn't exist (default: False)
        """
        pass

    @abstractmethod
    def upload_directory(
        self, local_dir: Path, yt_dir: str, pattern: str = "*"
    ) -> List[str]:
        """
        Upload a directory to YT.

        Recursively uploads all files from a local directory to a YT directory,
        respecting .ytignore patterns if present.

        Args:
            local_dir: Local directory path to upload
            yt_dir: YT destination directory path
            pattern: File pattern to match (default: "*" for all files)

        Returns:
            List of uploaded YT file paths
        """
        pass

    @abstractmethod
    def run_map(
        self,
        command: str,
        input_table: str,
        output_table: str,
        files: List[Tuple[str, str]],
        resources: OperationResources,
        env: Dict[str, str],
        output_schema: Optional["TableSchema"] = None,
        max_failed_jobs: int = 1,
        docker_auth: Optional[Dict[str, str]] = None,
    ) -> Operation:
        """
        Run a map operation on YT.

        Args:
            command: Command to execute (e.g., "python3 mapper.py")
            input_table: Input table path
            output_table: Output table path
            files: List of (yt_path, local_path) tuples for files to upload
            resources: Operation resources (pool, memory, CPU, GPU, etc.)
            env: Environment variables dictionary
            output_schema: Optional YT TableSchema for typed output table
            max_failed_jobs: Maximum number of failed jobs before operation fails
            docker_auth: Optional Docker authentication dictionary
        """
        pass

    @abstractmethod
    def run_vanilla(
        self,
        command: str,
        files: List[Tuple[str, str]],
        env: Dict[str, str],
        task_name: str,
        *args,
        **kwargs,
    ) -> Operation:
        """
        Run a vanilla operation on YT.

        Args:
            command: Command to execute (e.g., "python3 vanilla.py")
            files: List of (yt_path, local_path) tuples for files to upload
            env: Environment variables dictionary
            task_name: Task name for the operation
            *args: Additional positional arguments (implementation-specific)
            **kwargs: Additional keyword arguments. Common kwargs include:
                - resources: OperationResources instance (pool, memory, CPU, GPU, etc.)
                - docker_auth: Optional Docker authentication dictionary
                - max_failed_jobs: Maximum number of failed jobs before operation fails

        Returns:
            Operation object
        """
        pass

    def wait_for_operation(self, operation: Operation) -> bool:
        """
        Wait for operation to complete.

        Args:
            operation: Operation to wait for

        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Waiting for operation to complete...")

        try:
            operation.wait()
            state = operation.get_state()

            if state == "completed":
                self.logger.info("Operation completed successfully")
                return True
            else:
                self.logger.error(f"Operation {state}")
                self._log_operation_error(operation)
                return False

        except Exception as e:
            self.logger.error("Operation failed")
            self._log_error_from_exception(e)
            return False

    def _log_operation_error(self, operation: Operation) -> None:
        """Log operation error details."""
        try:
            error = operation.get_error()
            if error:
                self.logger.error(f"Error: {error}")
        except Exception:
            pass

    def _log_error_from_exception(self, exception: Exception) -> None:
        """Extract and log error from exception."""
        try:
            if (
                hasattr(exception, "attributes")
                and "stderrs"
                in exception.attributes  # pyright: ignore[reportAttributeAccessIssue]
            ):
                stderrs = (
                    exception.attributes[  # pyright: ignore[reportAttributeAccessIssue]
                        "stderrs"
                    ]
                )
                if stderrs and len(stderrs) > 0:
                    stderr = (
                        stderrs[0]
                        .get("error", {})
                        .get("attributes", {})
                        .get("stderr", "")
                    )
                    if stderr:
                        self.logger.error("Job stderr:")
                        for line in stderr.replace("\\n", "\n").split("\n"):
                            if line.strip():
                                self.logger.error(f"  {line}")
        except Exception:
            self.logger.error(f"Error: {str(exception)}")
