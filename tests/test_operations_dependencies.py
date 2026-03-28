"""Tests for yt_framework.operations.dependencies file-list builders."""

import logging
from pathlib import Path

import pytest

from yt_framework.operations.dependencies import (
    add_checkpoint,
    build_map_dependencies,
    build_stage_dependencies,
    build_vanilla_dependencies,
    build_ytjobs_dependencies,
)

_LOG = logging.getLogger("tests.operations_dependencies")


def test_build_stage_dependencies_returns_empty_when_no_config_or_src(
    tmp_path: Path,
) -> None:
    stage = tmp_path / "empty_stage"
    stage.mkdir()
    assert build_stage_dependencies("//bf", stage, _LOG) == []


def test_build_stage_dependencies_includes_config_yaml_when_present(
    tmp_path: Path,
) -> None:
    stage = tmp_path / "cfg_st"
    stage.mkdir()
    (stage / "config.yaml").write_text("k: v\n", encoding="utf-8")
    deps = build_stage_dependencies("//root", stage, _LOG)
    assert deps == [
        ("//root/stages/cfg_st/config.yaml", "stages/cfg_st/config.yaml"),
    ]


def test_build_stage_dependencies_lists_py_files_under_src(tmp_path: Path) -> None:
    stage = tmp_path / "py_st"
    stage.mkdir()
    src = stage / "src"
    src.mkdir()
    (src / "mapper.py").write_text("x = 1\n", encoding="utf-8")
    deps = build_stage_dependencies("//b", stage, _LOG)
    assert ("//b/stages/py_st/src/mapper.py", "stages/py_st/src/mapper.py") in deps


def test_add_checkpoint_appends_yt_path_when_model_and_base_set() -> None:
    base = [("//bf/a", "a")]
    out = add_checkpoint(base, "weights", "//ck", _LOG)
    assert out == [("//bf/a", "a"), ("//ck/weights", "weights")]


def test_add_checkpoint_returns_same_list_when_base_missing() -> None:
    base = [("//bf/a", "a")]
    out = add_checkpoint(base, "m", None, _LOG)
    assert out is base


def test_build_ytjobs_dependencies_uses_build_folder_ytjobs_prefix() -> None:
    deps = build_ytjobs_dependencies("//bf", _LOG)
    assert deps, "expected non-empty ytjobs file list"
    assert all(yt.startswith("//bf/ytjobs/") for yt, _ in deps)


def test_add_checkpoint_warns_when_model_without_checkpoint_base(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    add_checkpoint([], "solo_model", None, _LOG)
    assert "checkpoint_base is not configured" in caplog.text


def test_build_map_dependencies_returns_mapper_path_and_merges_stage_ytjobs_checkpoint(
    tmp_path: Path,
) -> None:
    stage = tmp_path / "map_st"
    stage.mkdir()
    (stage / "config.yaml").write_text("k: v\n", encoding="utf-8")
    src = stage / "src"
    src.mkdir()
    (src / "mapper.py").write_text("def run():\n    pass\n", encoding="utf-8")
    mapper_path, deps = build_map_dependencies("//bf", stage, "weights", "//ck", _LOG)
    assert mapper_path == "//bf/stages/map_st/src/mapper.py"
    assert ("//bf/stages/map_st/src/mapper.py", "stages/map_st/src/mapper.py") in deps
    assert ("//ck/weights", "weights") in deps
    assert any(yt.startswith("//bf/ytjobs/") for yt, _ in deps)


def test_add_checkpoint_logs_debug_when_checkpoint_base_without_model_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    out = add_checkpoint([], None, "//ck-only", _LOG)
    assert out == []
    assert any(
        "no model_name specified" in r.getMessage() for r in caplog.records
    ), "expected debug skip when checkpoint_base set without model_name"


def test_build_vanilla_dependencies_returns_vanilla_script_path(tmp_path: Path) -> None:
    stage = tmp_path / "van_st"
    stage.mkdir()
    src = stage / "src"
    src.mkdir()
    (src / "vanilla.py").write_text("print(1)\n", encoding="utf-8")
    script_path, deps = build_vanilla_dependencies("//bf", stage, None, None, _LOG)
    assert script_path == "//bf/stages/van_st/src/vanilla.py"
    assert (
        "//bf/stages/van_st/src/vanilla.py",
        "stages/van_st/src/vanilla.py",
    ) in deps
