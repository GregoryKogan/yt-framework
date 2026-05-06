"""Extra ``YTProdClient.run_map`` scenarios on a real cluster (happy paths)."""

from __future__ import annotations

import pytest
from yt.wrapper.operation_commands import (
    get_operation,
)  # pyright: ignore[reportMissingImports]

from integration.yt_cluster._job_uploads import upload_job_dep

_ENV_KEY = "YT_FRAMEWORK_CLUSTER_IT"
_ENV_VALUE = "cluster-it-env-ok"

MAPPER_WITH_ENV = f"""#!/usr/bin/env python3
import json
import os
import sys

for line in sys.stdin:
    row = json.loads(line)
    out = {{
        "k": row["k"],
        "v": int(row["v"]),
        "echo": os.environ.get("{_ENV_KEY}", ""),
    }}
    print(json.dumps(out), flush=True)
"""

MAPPER_WITH_SIDECAR = """#!/usr/bin/env python3
import json
import sys

tag = open("sidecar.txt", encoding="utf-8").read().strip()

for line in sys.stdin:
    row = json.loads(line)
    print(
        json.dumps({"k": row["k"], "v": int(row["v"]), "tag": tag}),
        flush=True,
    )
"""

MAPPER_IDENTITY_KV = """#!/usr/bin/env python3
import json
import sys

for line in sys.stdin:
    row = json.loads(line)
    print(json.dumps({"k": row["k"], "v": int(row["v"])}), flush=True)
"""


@pytest.mark.yt_cluster
def test_run_map_injects_env_into_mapper_output(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "mapper.py").write_text(MAPPER_WITH_ENV, encoding="utf-8")
    tin = f"{yt_case_prefix}/map_env_in"
    tout = f"{yt_case_prefix}/map_env_out"
    yt_client.write_table(tin, [{"k": "e", "v": 1}])
    deps = [
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "mapper.py", "mapper.py"
        )
    ]
    op = yt_client.run_map(
        "python3 mapper.py",
        tin,
        tout,
        deps,
        op_resources,
        {_ENV_KEY: _ENV_VALUE},
    )
    assert yt_client.wait_for_operation(op)
    rows = yt_client.read_table(tout)
    assert len(rows) == 1
    assert rows[0]["echo"] == _ENV_VALUE


@pytest.mark.yt_cluster
def test_run_map_append_second_pass_extends_table(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "mapper.py").write_text(MAPPER_IDENTITY_KV, encoding="utf-8")
    deps = [
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "mapper.py", "mapper.py"
        )
    ]
    tin_a = f"{yt_case_prefix}/map_app_a"
    tin_b = f"{yt_case_prefix}/map_app_b"
    tout = f"{yt_case_prefix}/map_app_out"
    yt_client.write_table(tin_a, [{"k": "a1", "v": 1}, {"k": "a2", "v": 2}])
    yt_client.write_table(tin_b, [{"k": "b1", "v": 3}, {"k": "b2", "v": 4}])
    op1 = yt_client.run_map("python3 mapper.py", tin_a, tout, deps, op_resources, {})
    assert yt_client.wait_for_operation(op1)
    op2 = yt_client.run_map(
        "python3 mapper.py",
        tin_b,
        tout,
        deps,
        op_resources,
        {},
        append=True,
    )
    assert yt_client.wait_for_operation(op2)
    assert yt_client.row_count(tout) == 4
    keys = sorted(r["k"] for r in yt_client.read_table(tout))
    assert keys == ["a1", "a2", "b1", "b2"]


@pytest.mark.yt_cluster
def test_run_map_job_alias_without_command(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "mapper.py").write_text(MAPPER_IDENTITY_KV, encoding="utf-8")
    tin = f"{yt_case_prefix}/map_job_kw_in"
    tout = f"{yt_case_prefix}/map_job_kw_out"
    yt_client.write_table(tin, [{"k": "j", "v": 9}])
    deps = [
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "mapper.py", "mapper.py"
        )
    ]
    op = yt_client.run_map(
        None,
        tin,
        tout,
        deps,
        op_resources,
        {},
        job="python3 mapper.py",
    )
    assert yt_client.wait_for_operation(op)
    assert yt_client.read_table(tout) == [{"k": "j", "v": 9}]


@pytest.mark.yt_cluster
def test_run_map_reads_second_uploaded_file_dependency(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "mapper.py").write_text(MAPPER_WITH_SIDECAR, encoding="utf-8")
    (local_case_dir / "sidecar.txt").write_text("sidecar-marker", encoding="utf-8")
    tin = f"{yt_case_prefix}/map_side_in"
    tout = f"{yt_case_prefix}/map_side_out"
    yt_client.write_table(tin, [{"k": "s", "v": 1}])
    deps = [
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "mapper.py", "mapper.py"
        ),
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "sidecar.txt", "sidecar.txt"
        ),
    ]
    op = yt_client.run_map("python3 mapper.py", tin, tout, deps, op_resources, {})
    assert yt_client.wait_for_operation(op)
    rows = yt_client.read_table(tout)
    assert rows == [{"k": "s", "v": 1, "tag": "sidecar-marker"}]


@pytest.mark.yt_cluster
def test_run_map_forwards_title_and_max_row_weight(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "mapper.py").write_text(MAPPER_IDENTITY_KV, encoding="utf-8")
    tin = f"{yt_case_prefix}/map_spec_in"
    tout = f"{yt_case_prefix}/map_spec_out"
    yt_client.write_table(tin, [{"k": "t", "v": 0}])
    deps = [
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "mapper.py", "mapper.py"
        )
    ]
    op = yt_client.run_map(
        "python3 mapper.py",
        tin,
        tout,
        deps,
        op_resources,
        {},
        title="yt_cluster_map_spec_kw",
        max_row_weight="64M",
    )
    assert yt_client.wait_for_operation(op)
    assert yt_client.read_table(tout) == [{"k": "t", "v": 0}]


@pytest.mark.yt_cluster
def test_run_map_accepts_max_failed_jobs_above_one(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "mapper.py").write_text(MAPPER_IDENTITY_KV, encoding="utf-8")
    tin = f"{yt_case_prefix}/map_mfj_in"
    tout = f"{yt_case_prefix}/map_mfj_out"
    yt_client.write_table(tin, [{"k": "m", "v": 5}])
    deps = [
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "mapper.py", "mapper.py"
        )
    ]
    op = yt_client.run_map(
        "python3 mapper.py",
        tin,
        tout,
        deps,
        op_resources,
        {},
        max_failed_jobs=3,
    )
    assert yt_client.wait_for_operation(op)
    assert yt_client.read_table(tout) == [{"k": "m", "v": 5}]


_SECRET_IT = "cluster-it-secret-marker"
_PUBLIC_STAGE = "YT_STAGE_NAME"
_PUBLIC_STAGE_VAL = "it_secure_env_stage"

MAPPER_ENV_PUBLIC_AND_SECRET = """#!/usr/bin/env python3
import json
import os
import sys

for line in sys.stdin:
    row = json.loads(line)
    out = {
        "k": row["k"],
        "v": int(row["v"]),
        "secret_echo": os.environ.get("MY_CLUSTER_SECRET", ""),
        "stage_echo": os.environ.get("YT_STAGE_NAME", ""),
    }
    print(json.dumps(out), flush=True)
"""


@pytest.mark.yt_cluster
def test_run_map_keeps_secrets_out_of_plain_environment_in_spec(
    yt_case_prefix: str, yt_client, op_resources, local_case_dir
) -> None:
    (local_case_dir / "mapper.py").write_text(
        MAPPER_ENV_PUBLIC_AND_SECRET, encoding="utf-8"
    )
    tin = f"{yt_case_prefix}/map_vault_in"
    tout = f"{yt_case_prefix}/map_vault_out"
    yt_client.write_table(tin, [{"k": "vault", "v": 1}])
    deps = [
        upload_job_dep(
            yt_client, yt_case_prefix, local_case_dir / "mapper.py", "mapper.py"
        )
    ]
    op = yt_client.run_map(
        "python3 mapper.py",
        tin,
        tout,
        deps,
        op_resources,
        {
            "MY_CLUSTER_SECRET": _SECRET_IT,
            _PUBLIC_STAGE: _PUBLIC_STAGE_VAL,
        },
    )
    assert yt_client.wait_for_operation(op)
    rows = yt_client.read_table(tout)
    assert len(rows) == 1
    assert rows[0]["secret_echo"] == _SECRET_IT
    assert rows[0]["stage_echo"] == _PUBLIC_STAGE_VAL

    info = get_operation(op.id, attributes=["full_spec"], client=yt_client.client)
    mapper_env = (info.get("full_spec") or {}).get("mapper", {}).get(
        "environment"
    ) or {}
    assert "MY_CLUSTER_SECRET" not in mapper_env
    assert mapper_env.get(_PUBLIC_STAGE) == _PUBLIC_STAGE_VAL
