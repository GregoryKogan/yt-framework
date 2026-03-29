"""Tests for yt_framework.yt.yql_builder pure formatting helpers."""

import pytest

from yt_framework.yt.yql_builder import (
    _escape_table_name,
    _format_aggregations,
    _format_column_list,
    _format_group_by_list,
    _format_join_conditions,
    _format_order_by_list,
    build_distinct_query,
    build_filter_query,
    build_group_by_query,
    build_join_query,
    build_limit_query,
    build_select_query,
    build_sort_query,
    build_union_query,
)


def test_escape_table_name_wraps_in_backticks() -> None:
    assert _escape_table_name("//tmp/t") == "`//tmp/t`"


def test_format_column_list_joins_with_comma_newline_indent() -> None:
    assert _format_column_list(["a.x", "b.y"]) == "a.x,\n    b.y"


def test_format_join_conditions_single_string_column() -> None:
    assert _format_join_conditions("id") == "a.id = b.id"


def test_format_join_conditions_list_uses_and_between_pairs() -> None:
    assert _format_join_conditions(["x", "y"]) == "a.x = b.x AND a.y = b.y"


def test_format_join_conditions_dict_single_pair() -> None:
    out = _format_join_conditions({"left": "uid", "right": "id"})
    assert out == "a.uid = b.id"


def test_format_join_conditions_dict_list_columns_zipped() -> None:
    out = _format_join_conditions(
        {"left": ["user_id", "region"], "right": ["id", "region_code"]}
    )
    assert out == "a.user_id = b.id AND a.region = b.region_code"


def test_format_group_by_list_accepts_string() -> None:
    assert _format_group_by_list("region") == "region"


def test_format_group_by_list_joins_list_columns() -> None:
    assert _format_group_by_list(["a", "b"]) == "a, b"


def test_format_order_by_list_string_uses_direction() -> None:
    assert _format_order_by_list("id", ascending=True) == "id ASC"


def test_format_order_by_list_list_uses_direction_per_column() -> None:
    assert _format_order_by_list(["a", "b"], ascending=False) == "a DESC, b DESC"


def test_format_aggregations_tuple_sum_includes_group_by() -> None:
    sel = _format_aggregations({"total_x": ("sum", "amount")}, group_by="gid")
    assert sel == "gid,\n    SUM(amount) AS total_x"


def test_format_aggregations_string_count_uses_star() -> None:
    sel = _format_aggregations({"n": "count"}, group_by="gid")
    assert sel == "gid,\n    COUNT(*) AS n"


def test_format_aggregations_tuple_count_uses_star() -> None:
    sel = _format_aggregations({"n": ("count", "ignored")}, group_by="gid")
    assert sel == "gid,\n    COUNT(*) AS n"


def test_format_aggregations_tuple_avg_uses_explicit_column_ref() -> None:
    sel = _format_aggregations({"avg_x": ("avg", "amount")}, group_by="gid")
    assert sel == "gid,\n    AVG(amount) AS avg_x"


def test_format_aggregations_string_sum_strips_total_prefix_from_key() -> None:
    sel = _format_aggregations({"total_amount": "sum"}, group_by="region")
    assert "SUM(amount) AS total_amount" in sel


def test_build_join_query_inner_with_using_when_select_columns_provided() -> None:
    q = build_join_query(
        "//l",
        "//r",
        "//out",
        on="id",
        how="inner",
        select_columns=["a.id", "b.name"],
    )
    assert "INSERT INTO `//out`" in q and "USING (id)" in q and "INNER JOIN" in q


def test_build_join_query_left_uses_on_when_dict_on_without_select_columns() -> None:
    q = build_join_query(
        "//left",
        "//right",
        "//out",
        on={"left": "uid", "right": "id"},
        how="left",
    )
    assert (
        "LEFT JOIN" in q
        and "ON a.uid = b.id" in q
        and "a.*, b.*" in q
        and "USING" not in q
    )


def test_build_join_query_string_on_uses_on_clause_when_select_columns_omitted() -> (
    None
):
    q = build_join_query("//l", "//r", "//out", on="id", how="inner")
    assert (
        "INNER JOIN" in q
        and "ON a.id = b.id" in q
        and "a.*, b.*" in q
        and "USING" not in q
    )


def test_build_join_query_list_on_uses_on_clause_when_select_columns_omitted() -> None:
    q = build_join_query("//l", "//r", "//out", on=["uid", "region"], how="right")
    assert (
        "RIGHT JOIN" in q
        and "ON a.uid = b.uid AND a.region = b.region" in q
        and "a.*, b.*" in q
        and "USING" not in q
    )


def test_build_join_query_full_maps_to_full_outer_join() -> None:
    q = build_join_query(
        "//a",
        "//b",
        "//out",
        on="k",
        how="full",
        select_columns=["a.k", "b.k"],
    )
    assert "FULL OUTER JOIN" in q and "USING (k)" in q


def test_build_join_query_formats_multi_column_using_when_on_is_list_with_select_columns() -> (
    None
):
    q = build_join_query(
        "//l",
        "//r",
        "//out",
        on=["uid", "region"],
        how="inner",
        select_columns=["a.uid", "b.uid", "a.region", "b.region"],
    )
    assert "USING (uid, region)" in q and "INNER JOIN" in q


def test_build_join_query_uses_on_when_on_is_tuple_even_with_select_columns() -> None:
    """Tuple keys are not ``list`` — exercises ``build_join_query`` else / fallback path."""
    q = build_join_query(
        "//l",
        "//r",
        "//out",
        on=("uid", "region"),
        how="left",
        select_columns=["a.uid", "a.region", "b.name"],
    )
    assert (
        "LEFT JOIN" in q
        and "ON a.uid = b.uid AND a.region = b.region" in q
        and "USING" not in q
    )


def test_build_union_query_raises_when_fewer_than_two_tables() -> None:
    with pytest.raises(ValueError, match="at least 2 tables"):
        build_union_query(["//only"], "//out", ["id"])


def test_build_union_query_joins_two_tables_with_union_all() -> None:
    q = build_union_query(["//a", "//b"], "//out", ["id", "name"])
    assert (
        "UNION ALL" in q
        and "FROM `//a`" in q
        and "FROM `//b`" in q
        and "INSERT INTO `//out`" in q
    )


def test_build_filter_query_includes_where_clause() -> None:
    q = build_filter_query(
        "//in",
        "//out",
        "status = 'active'",
        ["id", "status"],
    )
    assert "FROM `//in`" in q and "WHERE status = 'active'" in q


def test_build_select_query_lists_explicit_columns() -> None:
    q = build_select_query("//src", "//dst", ["a", "b"])
    assert "SELECT\n    a,\n    b\n" in q and "FROM `//src`" in q


def test_build_group_by_query_omits_group_by_when_group_by_empty_list() -> None:
    q = build_group_by_query(
        "//in",
        "//out",
        [],
        {"n": "count"},
    )
    assert "GROUP BY" not in q and "COUNT(*) AS n" in q


def test_build_group_by_query_includes_group_by_when_group_by_is_string() -> None:
    q = build_group_by_query(
        "//in",
        "//out",
        "region",
        {"n": "count"},
    )
    assert "GROUP BY region" in q and "COUNT(*) AS n" in q


def test_build_distinct_query_selects_star_when_columns_omitted() -> None:
    q = build_distinct_query("//in", "//out", columns=None)
    assert "SELECT DISTINCT\n    *\n" in q


def test_build_distinct_query_lists_columns_when_provided() -> None:
    q = build_distinct_query("//in", "//out", columns=["id", "name"])
    assert "SELECT DISTINCT\n    id,\n    name\n" in q


def test_build_sort_query_wraps_order_by_in_subquery() -> None:
    q = build_sort_query(
        "//in",
        "//out",
        order_by="id",
        columns=["id", "name"],
        ascending=False,
    )
    assert "ORDER BY id DESC" in q and "FROM (\n    SELECT *" in q


def test_build_limit_query_appends_limit() -> None:
    q = build_limit_query("//in", "//out", limit=10, columns=["id"])
    assert q.rstrip().endswith("LIMIT 10;") and "FROM `//in`" in q
