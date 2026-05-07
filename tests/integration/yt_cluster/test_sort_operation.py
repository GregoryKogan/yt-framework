"""Native YT sort operation (distinct from YQL sort_table)."""

from __future__ import annotations

import pytest


@pytest.mark.yt_cluster
def test_run_sort_orders_rows_in_place(
    yt_case_prefix: str, yt_client, op_resources
) -> None:
    t = f"{yt_case_prefix}/sort_native"
    yt_client.write_table(t, [{"k": "c"}, {"k": "a"}, {"k": "b"}])
    yt_client.run_sort(
        t,
        sort_by=["k"],
        pool=op_resources.pool,
        pool_tree=op_resources.pool_tree,
    )
    assert [r["k"] for r in yt_client.read_table(t)] == ["a", "b", "c"]
