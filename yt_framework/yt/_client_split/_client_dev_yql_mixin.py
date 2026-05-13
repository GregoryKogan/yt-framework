"""YQL convenience methods for dev YT client (DuckDB simulation)."""

# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

from typing import Literal

from yt_framework.yt.yql_builder import (
    build_distinct_query,
    build_filter_query,
    build_group_by_query,
    build_join_query,
    build_limit_query,
    build_select_query,
    build_sort_query,
    build_union_query,
)


class ClientDevYqlMixin:
    """Mixin providing high-level YQL table helpers in dev mode."""

    # Convenience methods for common YQL operations

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
        """Join two tables using YQL (executed locally with DuckDB in dev mode)."""
        query = build_join_query(
            left_table=left_table,
            right_table=right_table,
            output_table=output_table,
            on=on,
            how=how,
            select_columns=select_columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def filter_table(
        self,
        input_table: str,
        output_table: str,
        condition: str,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Filter table rows using WHERE condition (executed locally with DuckDB in dev mode)."""
        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_filter_query(
            input_table=input_table,
            output_table=output_table,
            condition=condition,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def select_columns(
        self,
        input_table: str,
        output_table: str,
        columns: list[str],
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Select specific columns from a table (executed locally with DuckDB in dev mode)."""
        query = build_select_query(
            input_table=input_table,
            output_table=output_table,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

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
        """Group by columns and compute aggregations (executed locally with DuckDB in dev mode)."""
        query = build_group_by_query(
            input_table=input_table,
            output_table=output_table,
            group_by=group_by,
            aggregations=aggregations,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def union_tables(
        self,
        tables: list[str],
        output_table: str,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Union multiple tables (executed locally with DuckDB in dev mode)."""
        # Get columns from first table to avoid _other column issues
        # All tables in union should have the same columns
        columns = self._get_table_columns(tables[0])

        query = build_union_query(
            tables=tables,
            output_table=output_table,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def distinct(
        self,
        input_table: str,
        output_table: str,
        columns: list[str] | None = None,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Get distinct rows from a table (executed locally with DuckDB in dev mode)."""
        query = build_distinct_query(
            input_table=input_table,
            output_table=output_table,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

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
        """Sort table by columns (executed locally with DuckDB in dev mode)."""
        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_sort_query(
            input_table=input_table,
            output_table=output_table,
            order_by=order_by,
            columns=columns,
            ascending=ascending,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def limit_table(
        self,
        input_table: str,
        output_table: str,
        limit: int,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Limit number of rows from a table (executed locally with DuckDB in dev mode)."""
        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_limit_query(
            input_table=input_table,
            output_table=output_table,
            limit=limit,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None
