"""Targeted coverage for yt client split mixins and prod/dev runtime helpers."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf
from yt.wrapper import TypedJob  # pyright: ignore[reportMissingImports]

from yt_framework.yt.clients._client_split._client_prod_cmd_helpers import (
    _partition_and_maybe_wrap_leg,
    _public_env_keys_for_partition,
)
from yt_framework.yt.clients.client_base import OperationResources
from yt_framework.yt.clients.client_dev import YTDevClient
from yt_framework.yt.clients.operation_specs import (
    MapReduceSubmitSpec,
    ReduceSubmitSpec,
)
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
from yt_framework.yt.support import _client_dev_runtime as dr
from yt_framework.yt.support import _client_prod_runtime as pr


def _null_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


class _TypedLeg(TypedJob):
    def prepare_operation(self, *args: object, **kwargs: object) -> None:  # type: ignore[override]
        pass


def test_public_env_keys_partition_accepts_list_container() -> None:
    assert _public_env_keys_for_partition(["a", "b"]) == ["a", "b"]


def test_public_env_keys_partition_accepts_frozenset() -> None:
    assert sorted(_public_env_keys_for_partition(frozenset({"a", "b"}))) == ["a", "b"]


def test_public_env_keys_partition_wraps_scalar() -> None:
    assert _public_env_keys_for_partition("PUBLIC_ONLY") == ["PUBLIC_ONLY"]


def test_partition_leg_uses_plain_environment_when_flag_true() -> None:
    pub, sec, leg = _partition_and_maybe_wrap_leg(
        "echo hi",
        {"S": "secret"},
        environment_public_keys=None,
        use_plain_environment_for_secrets=True,
    )
    assert sec == {} and pub["S"] == "secret" and leg == "echo hi"


def test_spec_builder_secure_vault_calls_callable_hook() -> None:
    b = MagicMock()
    b.secure_vault = MagicMock(return_value="chained")
    out = pr._spec_builder_secure_vault(b, {"K": "v"})
    b.secure_vault.assert_called_once_with({"K": "v"})
    assert out == "chained"


def test_spec_builder_secure_vault_non_callable_returns_builder() -> None:
    b = MagicMock()
    b.secure_vault = "not-callable"
    assert pr._spec_builder_secure_vault(b, {"K": "v"}) is b


def test_apply_max_row_weight_builder_noop_when_value_none() -> None:
    b = object()
    assert pr.apply_max_row_weight_builder(b, None) is b


def test_prod_create_table_parent_noop_when_parent_segment_empty() -> None:
    log = _null_logger("tests.crt.parent")
    cp = MagicMock()
    pr.prod_create_table_parent(
        make_parents=True,
        table_path="/a",
        create_path=cp,
        logger=log,
    )
    cp.assert_not_called()


def test_prod_process_upload_raises_when_uploaded_without_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pr,
        "_prod_upload_directory_step",
        lambda *a, **k: ("uploaded", None),
    )
    with pytest.raises(RuntimeError, match="internal"):
        pr._prod_process_upload_directory_file(
            Path("x"),
            [],
            Path(),
            "//yt/d",
            MagicMock(should_ignore=lambda p: False),
            create_path=MagicMock(),
            upload_file=MagicMock(),
            logger=_null_logger("tests.crt.up"),
        )


def test_dev_append_pythonpath_root_swallows_distribution_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        dr,
        "distribution_site_root",
        lambda _pkg: (_ for _ in ()).throw(ModuleNotFoundError("no")),
    )
    parts: list[str] = ["/tmp/pipeline"]
    dr._dev_append_distinct_pythonpath_root(parts, package_name="yt_framework")
    assert parts == ["/tmp/pipeline"]


def test_dev_pythonpath_entries_appends_merged_pythonpath(tmp_path: Path) -> None:
    out = dr.dev_pythonpath_entries(
        tmp_path,
        {"PYTHONPATH": "/extra"},
    )
    assert str(tmp_path) in out and "/extra" in out


def test_dev_find_checkpoint_in_operations_returns_none_when_missing() -> None:
    cfg = OmegaConf.create({"client": {"operations": {}}})
    assert dr.dev_find_checkpoint_in_operations(cfg) is None


def test_dev_find_checkpoint_in_operations_returns_first_checkpoint_path() -> None:
    cfg = OmegaConf.create(
        {
            "client": {
                "operations": {
                    "m": {"checkpoint": {"local_checkpoint_path": "/data/ck.bin"}},
                },
            },
        },
    )
    assert dr.dev_find_checkpoint_in_operations(cfg) == "/data/ck.bin"


def test_dev_find_checkpoint_in_operations_returns_none_when_ops_have_no_path() -> None:
    cfg = OmegaConf.create(
        {
            "client": {
                "operations": {
                    "m": {"checkpoint": {}},
                },
            },
        },
    )
    assert dr.dev_find_checkpoint_in_operations(cfg) is None


def test_dev_cfg_path_for_stage_returns_none_for_non_directory(tmp_path: Path) -> None:
    f = tmp_path / "not_dir"
    f.write_text("x", encoding="utf-8")
    assert dr.dev_cfg_path_for_stage(f) is None


def test_dev_find_checkpoint_in_config_returns_none_for_list_root() -> None:
    assert dr.dev_find_checkpoint_in_config(OmegaConf.create([])) is None


def test_dev_try_checkpoint_returns_none_when_yaml_not_dict(
    tmp_path: Path,
) -> None:
    p = tmp_path / "c.yaml"
    p.write_text("[]\n", encoding="utf-8")
    assert (
        dr.dev_try_checkpoint_stage_cfg(
            p,
            _null_logger("tests.drt.try"),
            dr.dev_find_checkpoint_in_config,
        )
        is None
    )


def test_dev_scan_stages_checkpoint_swallows_iterdir_errors(
    tmp_path: Path,
) -> None:
    stages = tmp_path / "stages"
    stages.write_text("not-a-dir\n", encoding="utf-8")
    assert (
        dr.dev_scan_stages_checkpoint(
            stages,
            _null_logger("tests.drt.scan"),
            dr.dev_find_checkpoint_in_config,
        )
        is None
    )


def test_dev_apply_checkpoint_fallback_warns_on_inner_load_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    st = tmp_path / "stages" / "s1"
    st.mkdir(parents=True)
    (st / "config.yaml").write_text("pipeline: {}\n", encoding="utf-8")
    env: dict[str, str] = {}

    def _merge(_cfg: object, _e: dict[str, str]) -> None:
        msg = "merge boom"
        raise ValueError(msg)

    caplog.set_level(logging.WARNING)
    dr.dev_apply_stage_checkpoint_fallback(
        tmp_path / "stages",
        env,
        _null_logger("tests.drt.fallback"),
        _merge,
    )
    assert "failed to load checkpoint config" in caplog.text


def test_dev_apply_checkpoint_returns_early_when_no_stage_config(
    tmp_path: Path,
) -> None:
    stages = tmp_path / "stages"
    stages.mkdir()
    env: dict[str, str] = {}
    dr.dev_apply_stage_checkpoint_fallback(
        stages,
        env,
        _null_logger("tests.drt.early"),
        lambda _c, _e: None,
    )
    assert "JOB_CONFIG_PATH" not in env


def test_dev_apply_checkpoint_outer_except_logs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stages = tmp_path / "stages"
    stages.mkdir()

    def _boom(_p: Path) -> Path | None:
        msg = "scan boom"
        raise OSError(msg)

    monkeypatch.setattr(dr, "_dev_first_existing_stage_config", _boom)
    caplog.set_level(logging.DEBUG, logger="tests.drt.outer")
    log = logging.getLogger("tests.drt.outer")
    log.handlers.clear()
    dr.dev_apply_stage_checkpoint_fallback(
        stages,
        {},
        log,
        lambda _c, _e: None,
    )
    assert "could not setup checkpoint config" in caplog.text


def test_dev_first_existing_stage_config_returns_none_for_empty_stages(
    tmp_path: Path,
) -> None:
    stages = tmp_path / "stages"
    stages.mkdir()
    assert dr._dev_first_existing_stage_config(stages) is None


def test_dev_rewrite_returns_unchanged_when_build_split_parts_mismatch() -> None:
    cmd = "//pool/x/build/z cmd"
    assert dr.dev_rewrite_build_path_cmd(cmd, build_split_parts=3) == cmd


def test_dev_fallback_replace_logs_debug_when_logger_present() -> None:
    log = MagicMock()
    dr._dev_fallback_replace_build_segment(
        "echo //a/b/build/c d",
        ["echo //a/b", "c d"],
        "c",
        cast("logging.Logger | None", log),
    )
    log.debug.assert_called_once()


def test_dev_fallback_replace_returns_unchanged_when_joined_path_absent() -> None:
    assert (
        dr._dev_fallback_replace_build_segment(
            "echo hello",
            ["echo", "x"],
            "x",
            None,
        )
        == "echo hello"
    )


def test_dev_rewrite_logs_fallback_when_regex_misses(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    cmd = "//pool/x/build/a/b my_mapper.sh"
    out = dr.dev_rewrite_build_path_cmd(
        cmd,
        build_split_parts=2,
        logger=_null_logger("tests.drt.rew"),
    )
    assert "my_mapper.sh" in out


def test_dev_copy_map_output_returns_early_when_proc_nonzero(tmp_path: Path) -> None:
    sand = tmp_path / "sand.jsonl"
    sand.write_bytes(b"x")
    out = tmp_path / "out.jsonl"
    dr.dev_copy_map_output_table(
        proc_returncode=1,
        sandbox_output=sand,
        append=False,
        output_table_local_path=out,
    )
    assert not out.exists()


def test_dev_copy_map_output_returns_early_when_sandbox_missing(tmp_path: Path) -> None:
    out = tmp_path / "out.jsonl"
    missing = tmp_path / "missing.jsonl"
    dr.dev_copy_map_output_table(
        proc_returncode=0,
        sandbox_output=missing,
        append=False,
        output_table_local_path=out,
    )
    assert not out.exists()


def test_dev_copy_map_output_table_appends_when_requested(tmp_path: Path) -> None:
    out = tmp_path / "out.jsonl"
    out.write_text("a\n", encoding="utf-8")
    sand = tmp_path / "sand.jsonl"
    sand.write_bytes(b"b\n")
    dr.dev_copy_map_output_table(
        proc_returncode=0,
        sandbox_output=sand,
        append=True,
        output_table_local_path=out,
    )
    assert "a" in out.read_text(encoding="utf-8") and "b" in out.read_text(
        encoding="utf-8",
    )


def test_dev_import_ytjobs_dir_returns_none_without_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys
    import types

    fake = types.ModuleType("ytjobs")
    monkeypatch.setitem(sys.modules, "ytjobs", fake)
    assert dr.dev_import_ytjobs_dir() is None


def test_dev_resolve_ytjobs_source_returns_none_when_file_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dr, "dev_import_ytjobs_dir", lambda: Path("/nonexistent/pkg"))
    assert dr.dev_resolve_ytjobs_source("ytjobs/missing.py") is None


@pytest.mark.parametrize(
    ("method", "request_obj"),
    [
        (
            "join_tables_request",
            JoinTablesRequest(
                left_table="//l",
                right_table="//r",
                output_table="//o",
                on="id",
                select_columns=["id"],
                dry_run=True,
            ),
        ),
        (
            "filter_table_request",
            FilterTableRequest(
                input_table="//i",
                output_table="//o",
                condition="1=1",
                columns=["id"],
                dry_run=True,
            ),
        ),
        (
            "select_columns_request",
            SelectColumnsRequest(
                input_table="//i",
                output_table="//o",
                columns=["id"],
                dry_run=True,
            ),
        ),
        (
            "group_by_aggregate_request",
            GroupByAggregateRequest(
                input_table="//i",
                output_table="//o",
                group_by="g",
                aggregations={"n": "count"},
                dry_run=True,
            ),
        ),
        (
            "union_tables_request",
            UnionTablesRequest(
                tables=("//a", "//b"),
                output_table="//o",
                columns=["id"],
                dry_run=True,
            ),
        ),
        (
            "distinct_request",
            DistinctRequest(
                input_table="//i",
                output_table="//o",
                columns=["id"],
                dry_run=True,
            ),
        ),
        (
            "sort_table_request",
            SortTableRequest(
                input_table="//i",
                output_table="//o",
                order_by="id",
                columns=["id"],
                dry_run=True,
            ),
        ),
        (
            "limit_table_request",
            LimitTableRequest(
                input_table="//i",
                output_table="//o",
                limit=3,
                columns=["id"],
                dry_run=True,
            ),
        ),
    ],
)
def test_dev_yql_helpers_return_query_string_on_dry_run(
    tmp_path: Path,
    method: str,
    request_obj: object,
) -> None:
    client = YTDevClient(_null_logger("tests.dry.yql"), pipeline_dir=tmp_path)
    fn = getattr(client, method)
    q = fn(request_obj)
    assert isinstance(q, str) and len(q) > 10


def test_dev_vanilla_submit_copies_stage_config_when_present(tmp_path: Path) -> None:
    from yt_framework.yt.clients.operation_specs import VanillaSubmitSpec

    task = "my_stage"
    cfg = tmp_path / "stages" / task / "config.yaml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("k: 1\n", encoding="utf-8")
    client = YTDevClient(_null_logger("tests.dev.vanilla_cfg"), pipeline_dir=tmp_path)
    spec = VanillaSubmitSpec(
        command="echo",
        files=(),
        env=(),
        task_name=task,
        resources=OperationResources(),
    )
    op = client.run_vanilla_submit(spec)
    assert op is not None
    sand = client._dev_dir() / f"{task}_sandbox"
    assert (sand / "stages" / task / "config.yaml").is_file()


def test_dev_vanilla_submit_warns_when_stage_config_missing_in_sandbox(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from yt_framework.yt.clients.operation_specs import VanillaSubmitSpec

    caplog.set_level(logging.WARNING)
    client = YTDevClient(_null_logger("tests.dev.vanilla_miss"), pipeline_dir=tmp_path)
    spec = VanillaSubmitSpec(
        command="echo",
        files=(),
        env=(),
        task_name="no_cfg_stage",
        resources=OperationResources(),
    )
    client.run_vanilla_submit(spec)
    assert "config file not found" in caplog.text


def _mr_resources() -> OperationResources:
    return OperationResources()


def test_dev_map_reduce_submit_describes_invalid_mapper_leg(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.dev.mr_bad"), pipeline_dir=tmp_path)
    spec = MapReduceSubmitSpec(
        mapper=42,
        reducer="./r.sh",
        input_table="//i",
        output_table="//o",
        reduce_by=("k",),
        files=(),
        resources=_mr_resources(),
        env=(),
    )
    client.run_map_reduce_submit(spec)


def test_dev_reduce_submit_describes_invalid_reducer_leg(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.dev.red_bad"), pipeline_dir=tmp_path)
    spec = ReduceSubmitSpec(
        reducer=42,
        input_table="//i",
        output_table="//o",
        reduce_by=("k",),
        files=(),
        resources=_mr_resources(),
        env=(),
    )
    client.run_reduce_submit(spec)


def test_dev_map_reduce_submit_logs_typed_job_legs(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.dev.mr_typed"), pipeline_dir=tmp_path)
    inp = client._table_local_path("//i")
    inp.parent.mkdir(parents=True, exist_ok=True)
    inp.write_text("row\n", encoding="utf-8")
    spec = MapReduceSubmitSpec(
        mapper=_TypedLeg(),
        reducer=_TypedLeg(),
        input_table="//i",
        output_table="//o",
        reduce_by=("k",),
        files=(),
        resources=_mr_resources(),
        env=(),
    )
    client.run_map_reduce_submit(spec)


def test_dev_reduce_submit_logs_typed_job_leg(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.dev.red_typed"), pipeline_dir=tmp_path)
    inp = client._table_local_path("//i")
    inp.parent.mkdir(parents=True, exist_ok=True)
    inp.write_text("row\n", encoding="utf-8")
    spec = ReduceSubmitSpec(
        reducer=_TypedLeg(),
        input_table="//i",
        output_table="//o",
        reduce_by=("k",),
        files=(),
        resources=_mr_resources(),
        env=(),
    )
    client.run_reduce_submit(spec)


def test_dev_try_copy_tarball_returns_false_without_build_dir(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.dev.tar_no_build"), pipeline_dir=tmp_path)
    assert (
        client._try_copy_tarball_from_build(
            yt_path="//yt/x.tar.gz",
            local_name="x.tar.gz",
            sandbox_dir=tmp_path / "sand",
        )
        is False
    )


def test_dev_try_copy_tarball_returns_false_when_archive_missing(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.dev.tar_miss"), pipeline_dir=tmp_path)
    (tmp_path / ".build").mkdir()
    assert (
        client._try_copy_tarball_from_build(
            yt_path="//yt/missing.tar.gz",
            local_name="missing.tar.gz",
            sandbox_dir=tmp_path / "sand2",
        )
        is False
    )


def test_dev_try_copy_checkpoint_skips_when_filename_mismatch(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.dev.ckpt"), pipeline_dir=tmp_path)
    ck = tmp_path / "w.bin"
    ck.write_bytes(b"x")
    sand = tmp_path / "sandbox"
    sand.mkdir()
    assert (
        client._try_copy_checkpoint_file(
            yt_path="//yt/other.bin",
            local_name="other.bin",
            sandbox_dir=sand,
            local_checkpoint_path=str(ck),
        )
        is False
    )


def test_dev_try_copy_checkpoint_warns_when_path_missing(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = YTDevClient(_null_logger("tests.dev.ckpt_miss"), pipeline_dir=tmp_path)
    caplog.set_level(logging.WARNING)
    missing = tmp_path / "nope.bin"
    assert (
        client._try_copy_checkpoint_file(
            yt_path="//yt/nope.bin",
            local_name="nope.bin",
            sandbox_dir=tmp_path / "s",
            local_checkpoint_path=str(missing),
        )
        is False
    )
    assert "does not exist" in caplog.text


def test_dev_prepare_map_sandbox_raises_when_input_missing(tmp_path: Path) -> None:
    client = YTDevClient(_null_logger("tests.dev.map_prep"), pipeline_dir=tmp_path)
    with pytest.raises(FileNotFoundError, match="input table file not found"):
        client._prepare_map_sandbox("//missing", "//out")


def test_dev_merge_checkpoint_env_returns_early_for_non_dict_stage_cfg(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.dev.mrg"), pipeline_dir=tmp_path)
    env: dict[str, str] = {}
    client._merge_checkpoint_env_from_stage([], env)
    assert env == {}


def test_dev_merge_checkpoint_env_returns_when_no_local_checkpoint_in_cfg(
    tmp_path: Path,
) -> None:
    client = YTDevClient(_null_logger("tests.dev.mrg2"), pipeline_dir=tmp_path)
    env: dict[str, str] = {}
    cfg = OmegaConf.create({"job": {"model_name": "m"}})
    client._merge_checkpoint_env_from_stage(cfg, env)
    assert "CHECKPOINT_FILE" not in env
