"""Abstract YQL convenience operations shared by YT clients."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from yt_framework.yt.clients.yql.yql_requests import (
        DistinctRequest,
        FilterTableRequest,
        GroupByAggregateRequest,
        JoinTablesRequest,
        LimitTableRequest,
        SelectColumnsRequest,
        SortTableRequest,
        UnionTablesRequest,
    )


class YqlOpsABC(ABC):
    """Abstract YQL helpers (join, filter, …) mixed into ``BaseYTClient``."""

    @abstractmethod
    def join_tables_request(self, req: JoinTablesRequest) -> str | None:
        """Join two tables using YQL (see :class:`JoinTablesRequest`)."""

    @abstractmethod
    def filter_table_request(self, req: FilterTableRequest) -> str | None:
        """Filter table rows using WHERE (see :class:`FilterTableRequest`)."""

    @abstractmethod
    def select_columns_request(self, req: SelectColumnsRequest) -> str | None:
        """Select specific columns (see :class:`SelectColumnsRequest`)."""

    @abstractmethod
    def group_by_aggregate_request(self, req: GroupByAggregateRequest) -> str | None:
        """Group by columns and compute aggregations."""

    @abstractmethod
    def union_tables_request(self, req: UnionTablesRequest) -> str | None:
        """Union multiple tables."""

    @abstractmethod
    def distinct_request(self, req: DistinctRequest) -> str | None:
        """Get distinct rows from a table."""

    @abstractmethod
    def sort_table_request(self, req: SortTableRequest) -> str | None:
        """Sort table by columns (expensive on large tables)."""

    @abstractmethod
    def limit_table_request(self, req: LimitTableRequest) -> str | None:
        """Limit number of rows from a table."""
