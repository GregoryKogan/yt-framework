"""YQL convenience methods for dev YT client (DuckDB simulation)."""

# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from yt_framework.yt.clients.yql_requests import (
        DistinctRequest,
        FilterTableRequest,
        GroupByAggregateRequest,
        JoinTablesRequest,
        LimitTableRequest,
        SelectColumnsRequest,
        SortTableRequest,
        UnionTablesRequest,
    )
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

    def join_tables_request(self, req: JoinTablesRequest) -> str | None:
        """Join two tables using YQL (executed locally with DuckDB in dev mode)."""
        query = build_join_query(req)
        if req.dry_run:
            return query
        self.run_yql(query, max_row_weight=req.max_row_weight)
        return None

    def filter_table_request(self, req: FilterTableRequest) -> str | None:
        """Filter table rows using WHERE condition (dev: DuckDB)."""
        cols = (
            req.columns
            if req.columns is not None
            else self._get_table_columns(req.input_table)
        )
        query = build_filter_query(replace(req, columns=cols))
        if req.dry_run:
            return query
        self.run_yql(query, max_row_weight=req.max_row_weight)
        return None

    def select_columns_request(self, req: SelectColumnsRequest) -> str | None:
        """Select specific columns from a table (dev: DuckDB)."""
        query = build_select_query(req)
        if req.dry_run:
            return query
        self.run_yql(query, max_row_weight=req.max_row_weight)
        return None

    def group_by_aggregate_request(self, req: GroupByAggregateRequest) -> str | None:
        """Group by columns and compute aggregations (dev: DuckDB)."""
        query = build_group_by_query(req)
        if req.dry_run:
            return query
        self.run_yql(query, max_row_weight=req.max_row_weight)
        return None

    def union_tables_request(self, req: UnionTablesRequest) -> str | None:
        """Union multiple tables (dev: DuckDB)."""
        cols = (
            req.columns
            if req.columns is not None
            else self._get_table_columns(req.tables[0])
        )
        query = build_union_query(replace(req, columns=cols))
        if req.dry_run:
            return query
        self.run_yql(query, max_row_weight=req.max_row_weight)
        return None

    def distinct_request(self, req: DistinctRequest) -> str | None:
        """Get distinct rows from a table (dev: DuckDB)."""
        query = build_distinct_query(req)
        if req.dry_run:
            return query
        self.run_yql(query, max_row_weight=req.max_row_weight)
        return None

    def sort_table_request(self, req: SortTableRequest) -> str | None:
        """Sort table by columns (dev: DuckDB)."""
        cols = (
            req.columns
            if req.columns is not None
            else self._get_table_columns(req.input_table)
        )
        query = build_sort_query(replace(req, columns=cols))
        if req.dry_run:
            return query
        self.run_yql(query, max_row_weight=req.max_row_weight)
        return None

    def limit_table_request(self, req: LimitTableRequest) -> str | None:
        """Limit number of rows from a table (dev: DuckDB)."""
        cols = (
            req.columns
            if req.columns is not None
            else self._get_table_columns(req.input_table)
        )
        query = build_limit_query(replace(req, columns=cols))
        if req.dry_run:
            return query
        self.run_yql(query, max_row_weight=req.max_row_weight)
        return None
