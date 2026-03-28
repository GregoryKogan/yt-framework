"""Contract tests for yt_framework.yt.client_dev.YTDevClient."""

import logging
import sys
from pathlib import Path

import pytest
from omegaconf import OmegaConf
from yt.wrapper import TypedJob  # pyright: ignore[reportMissingImports]

from yt_framework.yt.client_base import OperationResources
from yt_framework.yt.client_dev import YTDevClient


def _null_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


def test_dev_client_write_and_read_table_roundtrip_uses_basename_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev"), pipeline_dir=tmp_path)
    client.write_table("//cluster/foo/bar", [{"x": 1}], append=False)
    jsonl = tmp_path / ".dev" / "bar.jsonl"
    assert jsonl.is_file() and client.read_table("//cluster/foo/bar") == [
        {"x": 1}
    ], "tables map to .dev/{basename}.jsonl"


def test_dev_client_exists_always_reports_true() -> None:
    client = YTDevClient(
        _null_logger("tests.client_dev.exists"), pipeline_dir=Path(".")
    )
    assert client.exists("//any/path")


def test_dev_client_run_sort_logs_no_op(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    client = YTDevClient(_null_logger("tests.client_dev.sort"), pipeline_dir=tmp_path)
    client.run_sort("//tmp/sorted", ["key"])
    assert "run_sort no-op" in caplog.text


def test_dev_client_upload_directory_returns_empty_list(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.updir"), pipeline_dir=tmp_path)
    assert client.upload_directory(tmp_path / "local", "//yt/dest") == []


def test_dev_client_warns_when_pipeline_dir_defaults_to_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("YT_PIPELINE_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.WARNING)
    YTDevClient(_null_logger("tests.client_dev.cwd_warn"), pipeline_dir=None)
    assert "using cwd as pipeline_dir" in caplog.text


def test_dev_client_row_count_is_zero_when_jsonl_missing(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.rc"), pipeline_dir=tmp_path)
    assert client.row_count("//tmp/missing_table") == 0


def test_dev_client_get_table_columns_raises_when_table_has_no_rows(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.cols"), pipeline_dir=tmp_path)
    with pytest.raises(ValueError, match="empty, cannot determine columns"):
        client._get_table_columns("//tmp/only_internal")


def test_dev_client_get_local_checkpoint_path_returns_existing_file_from_config(
    tmp_path: Path,
) -> None:
    ckpt = tmp_path / "w.bin"
    ckpt.write_bytes(b"x")
    stages = tmp_path / "stages" / "s1"
    stages.mkdir(parents=True)
    (stages / "config.yaml").write_text(
        f"client:\n  local_checkpoint_path: {ckpt}\n", encoding="utf-8"
    )
    client = YTDevClient(_null_logger("tests.client_dev.ckpt"), pipeline_dir=tmp_path)
    resolved = client._get_local_checkpoint_path()
    assert resolved == str(ckpt.resolve())


def test_dev_client_find_checkpoint_in_config_reads_nested_operation_checkpoint(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.fcc"), pipeline_dir=tmp_path)
    cfg = OmegaConf.create(
        {
            "client": {
                "operations": {
                    "train": {
                        "checkpoint": {"local_checkpoint_path": "/data/model.ckpt"}
                    }
                }
            }
        }
    )
    assert client._find_checkpoint_in_config(cfg) == "/data/model.ckpt"


def test_dev_client_run_map_raises_not_implemented_for_typed_job_mapper(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.tj"), pipeline_dir=tmp_path)
    client.write_table("//tmp/in", [{"k": 1}])
    with pytest.raises(NotImplementedError, match="string commands"):
        client.run_map(
            TypedJob(),
            "//tmp/in",
            "//tmp/out",
            [],
            OperationResources(),
            {},
        )


def test_dev_client_run_map_copies_jsonl_output_when_subprocess_succeeds(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.rm"), pipeline_dir=tmp_path)
    client.write_table("//tmp/in", [{"n": 42}])
    op = client.run_map(
        "cat",
        "//tmp/in",
        "//tmp/out",
        [],
        OperationResources(),
        {},
    )
    assert op.get_state() == "completed" and client.read_table("//tmp/out") == [
        {"n": 42}
    ], "dev run_map should copy mapper stdout into output table jsonl"


def test_dev_client_run_vanilla_rewrites_double_slash_build_prefix_to_local_stages(
    tmp_path: Path,
) -> None:
    task = "vtask"
    script = tmp_path / "stages" / task / "src" / "hello.py"
    script.parent.mkdir(parents=True)
    script.write_text("# path rewrite smoke test\n", encoding="utf-8")
    client = YTDevClient(
        _null_logger("tests.client_dev.vbuild_re"), pipeline_dir=tmp_path
    )
    rel = f"stages/{task}/src/hello.py"
    cmd = f"python3 //tmp/ignored/build/{rel}"
    op = client.run_vanilla(
        cmd,
        [("//yt/deps/hello.py", rel)],
        {},
        task,
    )
    assert (
        op.get_state() == "completed"
    ), "dev should map //…/build/stages/… to sandbox stages/…"


def test_dev_client_run_vanilla_succeeds_for_trivial_bash_command(
    tmp_path: Path,
) -> None:
    client = YTDevClient(
        _null_logger("tests.client_dev.vanilla"), pipeline_dir=tmp_path
    )
    op = client.run_vanilla("true", [], {}, "noop_task")
    assert op.get_state() == "completed"


def test_dev_client_run_vanilla_warns_when_stage_config_missing(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    (tmp_path / "stages" / "missing_cfg").mkdir(parents=True)
    client = YTDevClient(
        _null_logger("tests.client_dev.vanilla_cfg"), pipeline_dir=tmp_path
    )
    client.run_vanilla("true", [], {}, "missing_cfg")
    assert "config file not found" in caplog.text


def test_dev_client_upload_files_copies_ytjobs_package_file_into_sandbox(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.yj_pkg"), pipeline_dir=tmp_path)
    sandbox = tmp_path / ".dev" / "sb_yj"
    sandbox.mkdir(parents=True)
    client._upload_files(
        [("//yt/deps/cfg", "ytjobs/config/__init__.py")],
        sandbox,
    )
    dest = sandbox / "ytjobs" / "config" / "__init__.py"
    assert dest.is_file() and b"get_config_path" in dest.read_bytes()


def test_dev_client_upload_files_logs_skip_when_dependency_not_found_locally(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    client = YTDevClient(_null_logger("tests.client_dev.skip"), pipeline_dir=tmp_path)
    sandbox = tmp_path / ".dev" / "sb_skip"
    sandbox.mkdir(parents=True)
    client._upload_files(
        [("//yt/missing.bin", "stages/nope/missing.bin")],
        sandbox,
    )
    assert "skipping file" in caplog.text and "not found locally" in caplog.text


def test_dev_client_upload_files_copies_checkpoint_when_names_match_config(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    ckpt = tmp_path / "weights" / "model.ckpt"
    ckpt.parent.mkdir(parents=True)
    ckpt.write_bytes(b"ck")
    stages = tmp_path / "stages" / "train"
    stages.mkdir(parents=True)
    (stages / "config.yaml").write_text(
        f"client:\n  local_checkpoint_path: {ckpt}\n", encoding="utf-8"
    )
    client = YTDevClient(
        _null_logger("tests.client_dev.up_ckpt"), pipeline_dir=tmp_path
    )
    sandbox = tmp_path / ".dev" / "sb"
    sandbox.mkdir(parents=True)
    client._upload_files([("//yt/artifacts/model.ckpt", "model.ckpt")], sandbox)
    dest = sandbox / "model.ckpt"
    assert (
        dest.is_file() and dest.read_bytes() == b"ck"
    ), "checkpoint copied into sandbox"


def test_dev_client_upload_files_copies_tarball_from_dot_build_when_present(
    tmp_path: Path,
) -> None:
    build = tmp_path / ".build"
    build.mkdir(parents=True)
    archive = build / "bundle.tar.gz"
    archive.write_bytes(b"gz")
    client = YTDevClient(_null_logger("tests.client_dev.up_tar"), pipeline_dir=tmp_path)
    sandbox = tmp_path / ".dev" / "sb2"
    sandbox.mkdir(parents=True)
    client._upload_files([("//yt/code/bundle.tar.gz", "bundle.tar.gz")], sandbox)
    assert (sandbox / "bundle.tar.gz").read_bytes() == b"gz"


def test_dev_client_upload_files_swallows_import_error_when_ytjobs_placeholder_in_sys_modules(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`import ytjobs` can fail during dependency copy; dev client must not propagate it."""
    caplog.set_level(logging.DEBUG)
    client = YTDevClient(_null_logger("tests.client_dev.yj_imp"), pipeline_dir=tmp_path)
    sandbox = tmp_path / ".dev" / "sb_yj_imp"
    sandbox.mkdir(parents=True)
    monkeypatch.setitem(sys.modules, "ytjobs", None)
    client._upload_files(
        [("//yt/pkg/x", "ytjobs/config/__init__.py")],
        sandbox,
    )
    assert (
        "skipping file" in caplog.text and "not found locally" in caplog.text
    ), "ytjobs branch should fall through when import ytjobs fails"


def test_dev_client_setup_checkpoint_config_sets_job_path_and_checkpoint_file(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    ckpt = tmp_path / "store" / "real_weights.bin"
    ckpt.parent.mkdir(parents=True)
    ckpt.write_bytes(b"w")
    stages = tmp_path / "stages" / "train"
    stages.mkdir(parents=True)
    cfg = stages / "config.yaml"
    cfg.write_text(
        f"job:\n  model_name: my_alias.ckpt\nclient:\n"
        f"  local_checkpoint_path: {ckpt}\n",
        encoding="utf-8",
    )
    client = YTDevClient(_null_logger("tests.client_dev.scc"), pipeline_dir=tmp_path)
    env: dict[str, str] = {}
    client._setup_checkpoint_config(env)
    assert (
        env.get("JOB_CONFIG_PATH") == str(cfg)
        and env.get("CHECKPOINT_FILE") == "my_alias.ckpt"
        and "checkpoint file set to" in caplog.text
    ), "_setup_checkpoint_config should expose JOB_CONFIG_PATH and CHECKPOINT_FILE"


def test_dev_client_setup_checkpoint_config_warns_when_checkpoint_path_missing(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    stages = tmp_path / "stages" / "train"
    stages.mkdir(parents=True)
    cfg = stages / "config.yaml"
    missing = tmp_path / "nope" / "missing.ckpt"
    cfg.write_text(
        f"job:\n  model_name: m\nclient:\n  local_checkpoint_path: {missing}\n",
        encoding="utf-8",
    )
    client = YTDevClient(
        _null_logger("tests.client_dev.scc_miss"), pipeline_dir=tmp_path
    )
    env: dict[str, str] = {}
    client._setup_checkpoint_config(env)
    assert (
        env.get("JOB_CONFIG_PATH") == str(cfg)
        and "CHECKPOINT_FILE" not in env
        and "local_checkpoint_path not found" in caplog.text
    ), "missing checkpoint file should warn and omit CHECKPOINT_FILE"


def test_dev_client_run_yql_copies_select_into_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.yql"), pipeline_dir=tmp_path)
    client.write_table("//cluster/src", [{"n": 1}], append=False)
    yql = "INSERT INTO `//cluster/dst` WITH TRUNCATE " "SELECT * FROM `//cluster/src`;"
    client.run_yql(yql)
    assert client.read_table("//cluster/dst") == [
        {"n": 1}
    ], "dev run_yql should materialize INSERT … SELECT into .dev basename jsonl"


def test_dev_client_run_map_reduce_copies_input_jsonl_to_output(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.mr"), pipeline_dir=tmp_path)
    client.write_table("//tmp/mr_in", [{"row": 1}], append=False)
    op = client.run_map_reduce(
        "python3 mapper.py",
        "python3 reducer.py",
        "//tmp/mr_in",
        "//tmp/mr_out",
        ["id"],
        [],
        OperationResources(),
        {},
    )
    assert op.get_state() == "completed" and client.read_table("//tmp/mr_out") == [
        {"row": 1}
    ], "dev run_map_reduce copies input table jsonl to output"


def test_dev_client_run_map_reduce_writes_empty_output_when_input_missing(
    tmp_path: Path,
) -> None:
    client = YTDevClient(
        _null_logger("tests.client_dev.mr_miss"), pipeline_dir=tmp_path
    )
    op = client.run_map_reduce(
        "m",
        "r",
        "//tmp/missing_in",
        "//tmp/mr_empty_out",
        [],
        [],
        OperationResources(),
        {},
    )
    assert (
        op.get_state() == "completed" and client.read_table("//tmp/mr_empty_out") == []
    )


def test_dev_client_run_reduce_copies_input_jsonl_to_output(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.red"), pipeline_dir=tmp_path)
    client.write_table("//tmp/rd_in", [{"k": "v"}], append=False)
    op = client.run_reduce(
        "python3 reducer.py",
        "//tmp/rd_in",
        "//tmp/rd_out",
        ["k"],
        [],
        OperationResources(),
        {},
    )
    assert op.get_state() == "completed" and client.read_table("//tmp/rd_out") == [
        {"k": "v"}
    ], "dev run_reduce copies input table jsonl to output"


def test_dev_client_join_tables_materializes_joined_rows_in_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.join"), pipeline_dir=tmp_path)
    client.write_table("//tmp/j_left", [{"id": 1, "v": "a"}], append=False)
    client.write_table("//tmp/j_right", [{"id": 1, "w": "b"}], append=False)
    client.join_tables(
        "//tmp/j_left",
        "//tmp/j_right",
        "//tmp/j_out",
        "id",
        dry_run=False,
    )
    assert client.read_table("//tmp/j_out") == [
        {"id": 1, "v": "a", "w": "b"}
    ], "dev join_tables should run YQL via DuckDB and write output jsonl"


def test_dev_client_filter_table_materializes_where_result_in_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.filt"), pipeline_dir=tmp_path)
    client.write_table(
        "//tmp/f_in", [{"id": 1, "keep": True}, {"id": 2, "keep": False}]
    )
    client.filter_table(
        "//tmp/f_in",
        "//tmp/f_out",
        '"keep" = true',
        dry_run=False,
    )
    assert client.read_table("//tmp/f_out") == [{"id": 1, "keep": True}]


def test_dev_client_select_columns_materializes_subset_columns_in_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.sel"), pipeline_dir=tmp_path)
    client.write_table(
        "//tmp/sel_in",
        [{"id": 1, "name": "a", "extra": 9}],
        append=False,
    )
    client.select_columns(
        "//tmp/sel_in",
        "//tmp/sel_out",
        ["id", "name"],
        dry_run=False,
    )
    assert client.read_table("//tmp/sel_out") == [{"id": 1, "name": "a"}]


def test_dev_client_union_tables_materializes_stacked_rows_in_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.un"), pipeline_dir=tmp_path)
    client.write_table("//tmp/u_a", [{"k": 1}], append=False)
    client.write_table("//tmp/u_b", [{"k": 2}], append=False)
    client.union_tables(["//tmp/u_a", "//tmp/u_b"], "//tmp/u_out", dry_run=False)
    assert client.read_table("//tmp/u_out") == [{"k": 1}, {"k": 2}]


def test_dev_client_distinct_materializes_deduplicated_rows_in_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.dist"), pipeline_dir=tmp_path)
    client.write_table(
        "//tmp/d_in",
        [{"x": 1, "y": 1}, {"x": 1, "y": 2}, {"x": 2, "y": 1}],
        append=False,
    )
    client.distinct("//tmp/d_in", "//tmp/d_out", columns=["x"], dry_run=False)
    rows = client.read_table("//tmp/d_out")
    assert sorted(rows, key=lambda r: r["x"]) == [{"x": 1}, {"x": 2}]


def test_dev_client_sort_table_materializes_ordered_rows_in_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(
        _null_logger("tests.client_dev.sort_tbl"), pipeline_dir=tmp_path
    )
    client.write_table(
        "//tmp/st_in",
        [{"rank": 2, "label": "b"}, {"rank": 1, "label": "a"}],
        append=False,
    )
    client.sort_table(
        "//tmp/st_in",
        "//tmp/st_out",
        order_by="rank",
        ascending=True,
        dry_run=False,
    )
    assert client.read_table("//tmp/st_out") == [
        {"rank": 1, "label": "a"},
        {"rank": 2, "label": "b"},
    ]


def test_dev_client_limit_table_materializes_first_n_rows_in_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.lim"), pipeline_dir=tmp_path)
    client.write_table(
        "//tmp/lim_in",
        [{"i": 1}, {"i": 2}, {"i": 3}],
        append=False,
    )
    client.limit_table("//tmp/lim_in", "//tmp/lim_out", limit=2, dry_run=False)
    assert client.read_table("//tmp/lim_out") == [{"i": 1}, {"i": 2}]


def test_dev_client_group_by_aggregate_materializes_grouped_rows_in_output_jsonl(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.client_dev.gb"), pipeline_dir=tmp_path)
    client.write_table(
        "//tmp/gb_in",
        [
            {"region": "east", "amount": 10},
            {"region": "east", "amount": 20},
            {"region": "west", "amount": 5},
        ],
        append=False,
    )
    client.group_by_aggregate(
        "//tmp/gb_in",
        "//tmp/gb_out",
        "region",
        {"n": "count", "total": ("sum", "amount")},
        dry_run=False,
    )
    rows = client.read_table("//tmp/gb_out")
    by_region = {r["region"]: r for r in rows}
    assert by_region == {
        "east": {"region": "east", "n": 2, "total": 30},
        "west": {"region": "west", "n": 1, "total": 5},
    }, "dev group_by_aggregate should aggregate via DuckDB into output jsonl"
