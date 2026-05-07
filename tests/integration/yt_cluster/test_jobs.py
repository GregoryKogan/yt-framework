"""Map, map-reduce, reduce, and vanilla operations (cluster default Docker image)."""

from __future__ import annotations

from pathlib import Path

import pytest

from integration.yt_cluster._job_uploads import upload_job_dep

MAPPER_ID = """#!/usr/bin/env python3
import json
import sys

for line in sys.stdin:
    row = json.loads(line)
    print(json.dumps({"k": row["k"], "v": int(row["v"])}), flush=True)
"""

REDUCER_SUM = """#!/usr/bin/env python3
import json
import sys

cur_k = None
s = 0


def flush() -> None:
    global cur_k, s
    if cur_k is not None:
        print(json.dumps({"k": cur_k, "sum_v": s}), flush=True)


for line in sys.stdin:
    row = json.loads(line)
    k, v = row["k"], int(row["v"])
    if cur_k is None:
        cur_k, s = k, v
    elif k == cur_k:
        s += v
    else:
        flush()
        cur_k, s = k, v
flush()
"""


@pytest.mark.yt_cluster
def test_run_map_python_mapper(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "mapper.py").write_text(MAPPER_ID, encoding="utf-8")
    tin = f"{yt_case_prefix}/map_in"
    tout = f"{yt_case_prefix}/map_out"
    yt_client.write_table(tin, [{"k": "x", "v": 1}, {"k": "y", "v": 2}])
    mapper_local = local_case_dir / "mapper.py"
    deps = [upload_job_dep(yt_client, yt_case_prefix, mapper_local, "mapper.py")]
    op = yt_client.run_map(
        "python3 mapper.py",
        tin,
        tout,
        deps,
        op_resources,
        {},
    )
    assert yt_client.wait_for_operation(op), "map operation must finish successfully"
    rows = sorted(yt_client.read_table(tout), key=lambda r: r["k"])
    assert rows == [{"k": "x", "v": 1}, {"k": "y", "v": 2}]


@pytest.mark.yt_cluster
def test_run_map_reduce_sums_by_key(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    for name, body in (("mapper.py", MAPPER_ID), ("reducer.py", REDUCER_SUM)):
        (local_case_dir / name).write_text(body, encoding="utf-8")
    tin = f"{yt_case_prefix}/mr_in"
    tout = f"{yt_case_prefix}/mr_out"
    yt_client.write_table(
        tin,
        [
            {"k": "a", "v": 1},
            {"k": "a", "v": 2},
            {"k": "b", "v": 3},
        ],
    )
    deps = [
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "mapper.py", "mapper.py"
        ),
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "reducer.py", "reducer.py"
        ),
    ]
    op = yt_client.run_map_reduce(
        "python3 mapper.py",
        "python3 reducer.py",
        tin,
        tout,
        reduce_by=["k"],
        files=deps,
        resources=op_resources,
        env={},
    )
    assert yt_client.wait_for_operation(op), "map-reduce must finish successfully"
    got = {r["k"]: r["sum_v"] for r in yt_client.read_table(tout)}
    assert got == {"a": 3, "b": 3}


@pytest.mark.yt_cluster
def test_run_reduce_only_after_sort(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "reducer.py").write_text(REDUCER_SUM, encoding="utf-8")
    tin = f"{yt_case_prefix}/red_in"
    tout = f"{yt_case_prefix}/red_out"
    yt_client.write_table(
        tin,
        [
            {"k": "b", "v": 1},
            {"k": "a", "v": 2},
            {"k": "a", "v": 1},
        ],
    )
    yt_client.run_sort(
        tin,
        sort_by=["k"],
        pool=op_resources.pool,
        pool_tree=op_resources.pool_tree,
    )
    op = yt_client.run_reduce(
        "python3 reducer.py",
        tin,
        tout,
        reduce_by=["k"],
        files=[
            upload_job_dep(
                yt_client, yt_case_prefix, local_case_dir / "reducer.py", "reducer.py"
            )
        ],
        resources=op_resources,
        env={},
    )
    assert yt_client.wait_for_operation(op), "reduce must finish successfully"
    got = {r["k"]: r["sum_v"] for r in yt_client.read_table(tout)}
    assert got == {"a": 3, "b": 1}


@pytest.mark.yt_cluster
def test_run_vanilla_true(yt_case_prefix: str, yt_client, op_resources) -> None:
    op = yt_client.run_vanilla(
        "true",
        [],
        {},
        "yt_cluster_vanilla_smoke",
        op_resources,
    )
    assert yt_client.wait_for_operation(op), "vanilla no-op must succeed"
