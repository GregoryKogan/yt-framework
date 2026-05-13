"""Frozen request objects for YQL helper entry points."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class JoinTablesRequest:
    """Parameters for ``join_tables_request`` / ``build_join_query``."""

    left_table: str
    right_table: str
    output_table: str
    on: str | list[str] | dict[str, str] | tuple[str, ...]
    how: Literal["inner", "left", "right", "full"] = "left"
    select_columns: list[str] | None = None
    dry_run: bool = False
    max_row_weight: str | None = None


@dataclass(frozen=True)
class FilterTableRequest:
    """Parameters for ``filter_table_request`` / ``build_filter_query``."""

    input_table: str
    output_table: str
    condition: str
    columns: list[str] | None = None
    dry_run: bool = False
    max_row_weight: str | None = None


@dataclass(frozen=True)
class SelectColumnsRequest:
    """Parameters for ``select_columns_request`` / ``build_select_query``."""

    input_table: str
    output_table: str
    columns: list[str]
    dry_run: bool = False
    max_row_weight: str | None = None


@dataclass(frozen=True)
class GroupByAggregateRequest:
    """Parameters for ``group_by_aggregate_request`` / ``build_group_by_query``."""

    input_table: str
    output_table: str
    group_by: str | list[str]
    aggregations: dict[str, str | tuple[str, str]]
    dry_run: bool = False
    max_row_weight: str | None = None


@dataclass(frozen=True)
class UnionTablesRequest:
    """Parameters for ``union_tables_request`` / ``build_union_query``."""

    tables: tuple[str, ...]
    output_table: str
    columns: list[str] | None = None
    dry_run: bool = False
    max_row_weight: str | None = None


@dataclass(frozen=True)
class DistinctRequest:
    """Parameters for ``distinct_request`` / ``build_distinct_query``."""

    input_table: str
    output_table: str
    columns: list[str] | None = None
    dry_run: bool = False
    max_row_weight: str | None = None


@dataclass(frozen=True)
class SortTableRequest:
    """Parameters for ``sort_table_request`` / ``build_sort_query``."""

    input_table: str
    output_table: str
    order_by: str | list[str]
    columns: list[str] | None = None
    ascending: bool = True
    dry_run: bool = False
    max_row_weight: str | None = None


@dataclass(frozen=True)
class LimitTableRequest:
    """Parameters for ``limit_table_request`` / ``build_limit_query``."""

    input_table: str
    output_table: str
    limit: int
    columns: list[str] | None = None
    dry_run: bool = False
    max_row_weight: str | None = None
