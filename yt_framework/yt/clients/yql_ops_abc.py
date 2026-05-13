"""Abstract YQL convenience operations shared by YT clients."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal


class YqlOpsABC(ABC):
    """Abstract YQL helpers (join, filter, …) mixed into ``BaseYTClient``."""

    # Convenience methods for common YQL operations

    @abstractmethod
    def join_tables(
        self,
        left_table: str,
        right_table: str,
        output_table: str,
        on: str | list[str] | dict[str, str],
        how: Literal["inner", "left", "right", "full"] = "left",
        select_columns: list[str] | None = None,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Join two tables using YQL.

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
            max_row_weight: Optional max row weight override

        Returns:
            YQL query string if dry_run=True, None otherwise

        """

    @abstractmethod
    def filter_table(
        self,
        input_table: str,
        output_table: str,
        condition: str,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Filter table rows using WHERE condition.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            condition: WHERE condition (e.g., "status = 'active' AND total > 100")
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight override

        Returns:
            YQL query string if dry_run=True, None otherwise

        """

    @abstractmethod
    def select_columns(
        self,
        input_table: str,
        output_table: str,
        columns: list[str],
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Select specific columns from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            columns: List of column names to select
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight override

        Returns:
            YQL query string if dry_run=True, None otherwise

        """

    @abstractmethod
    def group_by_aggregate(
        self,
        input_table: str,
        output_table: str,
        group_by: str | list[str],
        aggregations: dict[str, str | tuple[str, str]],
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Group by columns and compute aggregations.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            group_by: Column(s) to group by
            aggregations: Dict mapping output column names to aggregation functions
                         e.g., {"order_count": "count", "total_amount": "sum"}
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight override

        Returns:
            YQL query string if dry_run=True, None otherwise

        """

    @abstractmethod
    def union_tables(
        self,
        tables: list[str],
        output_table: str,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Union multiple tables.

        Args:
            tables: List of table paths to union
            output_table: Path to output table
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight override

        Returns:
            YQL query string if dry_run=True, None otherwise

        """

    @abstractmethod
    def distinct(
        self,
        input_table: str,
        output_table: str,
        columns: list[str] | None = None,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Get distinct rows from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            columns: Optional list of columns to select (if None, selects all)
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight override

        Returns:
            YQL query string if dry_run=True, None otherwise

        """

    @abstractmethod
    def sort_table(
        self,
        input_table: str,
        output_table: str,
        order_by: str | list[str],
        *,
        ascending: bool = True,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Sort table by columns.

        WARNING: Sorting large tables can be expensive. Use with caution.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            order_by: Column(s) to sort by
            ascending: Sort direction (True for ASC, False for DESC)
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight override

        Returns:
            YQL query string if dry_run=True, None otherwise

        """

    @abstractmethod
    def limit_table(
        self,
        input_table: str,
        output_table: str,
        limit: int,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Limit number of rows from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            limit: Maximum number of rows to return
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight override

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
