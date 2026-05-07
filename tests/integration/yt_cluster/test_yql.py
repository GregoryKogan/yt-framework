"""YQL execution and table convenience helpers (real cluster)."""

from __future__ import annotations

import pytest


@pytest.mark.yt_cluster
def test_run_yql_inserts_projection(
    yt_case_prefix: str, yt_client, op_resources
) -> None:
    tin = f"{yt_case_prefix}/yql_raw_in"
    tout = f"{yt_case_prefix}/yql_raw_out"
    yt_client.write_table(tin, [{"k": 1}, {"k": 2}])
    yt_client.run_yql(
        f"PRAGMA yt.InferSchema = '1';\n"
        f"INSERT INTO `{tout}` WITH TRUNCATE SELECT k, k + 10 AS k10 FROM `{tin}`",
        pool=op_resources.pool,
    )
    rows = sorted(yt_client.read_table(tout), key=lambda r: r["k"])
    assert rows == [{"k": 1, "k10": 11}, {"k": 2, "k10": 12}]


@pytest.mark.yt_cluster
def test_filter_table(yt_case_prefix: str, yt_client) -> None:
    tin, tout = f"{yt_case_prefix}/f_in", f"{yt_case_prefix}/f_out"
    yt_client.write_table(
        tin, [{"a": 1, "b": "u"}, {"a": 5, "b": "v"}, {"a": 3, "b": "w"}]
    )
    yt_client.filter_table(tin, tout, "a > 2")
    rows = sorted(yt_client.read_table(tout), key=lambda r: r["a"])
    assert rows == [{"a": 3, "b": "w"}, {"a": 5, "b": "v"}]


@pytest.mark.yt_cluster
def test_select_columns(yt_case_prefix: str, yt_client) -> None:
    tin, tout = f"{yt_case_prefix}/s_in", f"{yt_case_prefix}/s_out"
    yt_client.write_table(tin, [{"x": 1, "y": 2}, {"x": 3, "y": 4}])
    yt_client.select_columns(tin, tout, ["y"])
    rows = sorted(yt_client.read_table(tout), key=lambda r: r["y"])
    assert rows == [{"y": 2}, {"y": 4}]


@pytest.mark.yt_cluster
def test_group_by_aggregate(yt_case_prefix: str, yt_client) -> None:
    tin, tout = f"{yt_case_prefix}/g_in", f"{yt_case_prefix}/g_out"
    yt_client.write_table(
        tin,
        [
            {"region": "east", "v": 10},
            {"region": "east", "v": 20},
            {"region": "west", "v": 7},
        ],
    )
    yt_client.group_by_aggregate(
        tin,
        tout,
        group_by="region",
        aggregations={"total_v": "sum"},
    )
    rows = {r["region"]: r["total_v"] for r in yt_client.read_table(tout)}
    assert rows == {"east": 30, "west": 7}


@pytest.mark.yt_cluster
def test_union_tables(yt_case_prefix: str, yt_client) -> None:
    t1, t2, tout = (
        f"{yt_case_prefix}/u1",
        f"{yt_case_prefix}/u2",
        f"{yt_case_prefix}/u_out",
    )
    yt_client.write_table(t1, [{"n": 1}, {"n": 2}])
    yt_client.write_table(t2, [{"n": 3}])
    yt_client.union_tables([t1, t2], tout)
    nums = sorted(r["n"] for r in yt_client.read_table(tout))
    assert nums == [1, 2, 3]


@pytest.mark.yt_cluster
def test_distinct_table(yt_case_prefix: str, yt_client) -> None:
    tin, tout = f"{yt_case_prefix}/d_in", f"{yt_case_prefix}/d_out"
    yt_client.write_table(tin, [{"k": 1}, {"k": 1}, {"k": 2}])
    yt_client.distinct(tin, tout)
    nums = sorted(r["k"] for r in yt_client.read_table(tout))
    assert nums == [1, 2]


@pytest.mark.yt_cluster
def test_sort_table_yql(yt_case_prefix: str, yt_client) -> None:
    tin, tout = f"{yt_case_prefix}/sort_in", f"{yt_case_prefix}/sort_yql_out"
    yt_client.write_table(tin, [{"v": 3}, {"v": 1}, {"v": 2}])
    yt_client.sort_table(tin, tout, order_by="v")
    # Static Cypress reads do not guarantee row order; compare sorted values.
    rows = yt_client.read_table(tout)
    assert sorted(r["v"] for r in rows) == [1, 2, 3]


@pytest.mark.yt_cluster
def test_limit_table(yt_case_prefix: str, yt_client) -> None:
    tin, tout = f"{yt_case_prefix}/l_in", f"{yt_case_prefix}/l_out"
    yt_client.write_table(tin, [{"i": 10}, {"i": 20}, {"i": 30}])
    yt_client.limit_table(tin, tout, 2)
    vals = sorted(r["i"] for r in yt_client.read_table(tout))
    assert vals == [10, 20]


@pytest.mark.yt_cluster
def test_join_tables(yt_case_prefix: str, yt_client) -> None:
    left, right, out = (
        f"{yt_case_prefix}/j_left",
        f"{yt_case_prefix}/j_right",
        f"{yt_case_prefix}/j_out",
    )
    yt_client.write_table(left, [{"uid": 1, "name": "a"}, {"uid": 2, "name": "b"}])
    yt_client.write_table(right, [{"uid": 1, "score": 100}, {"uid": 3, "score": 200}])
    yt_client.join_tables(
        left,
        right,
        out,
        on="uid",
        how="inner",
        select_columns=[
            "a.uid AS uid",
            "a.name AS name",
            "b.score AS score",
        ],
    )
    rows = yt_client.read_table(out)
    assert len(rows) == 1
    row = {k: v for k, v in rows[0].items() if not str(k).startswith("_")}
    assert row == {"uid": 1, "name": "a", "score": 100}
