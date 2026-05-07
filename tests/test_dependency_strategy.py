"""Tests for yt_framework.operations.dependency_strategy.TarArchiveDependencyBuilder."""

import logging
from pathlib import Path

import pytest
from omegaconf import OmegaConf

from yt_framework.operations.dependency_strategy import TarArchiveDependencyBuilder

_LOG = logging.getLogger("tests.dependency_strategy")


def _builder() -> TarArchiveDependencyBuilder:
    return TarArchiveDependencyBuilder()


def test_tar_builder_map_wraps_bootstrap_in_bash_c_and_sets_mapper_script_path(
    tmp_path: Path,
) -> None:
    stage_dir = tmp_path / "my_stage"
    stage_dir.mkdir()
    op = OmegaConf.create({})
    st = OmegaConf.create({})
    r = _builder().build_dependencies(
        "map",
        stage_dir,
        "src.tar.gz",
        "//build",
        op,
        st,
        _LOG,
    )
    assert r.script_path == "//build/stages/my_stage/src/mapper.py"
    assert r.command is not None
    assert r.command.startswith("bash -c '")
    assert "tar -xzf src.tar.gz" in r.command
    assert "operation_wrapper_my_stage_map.sh" in r.command


def test_tar_builder_vanilla_points_script_path_at_vanilla_py(tmp_path: Path) -> None:
    stage_dir = tmp_path / "van"
    stage_dir.mkdir()
    r = _builder().build_dependencies(
        "vanilla",
        stage_dir,
        "x.tar.gz",
        "//b",
        OmegaConf.create({}),
        OmegaConf.create({}),
        _LOG,
    )
    assert r.script_path == "//b/stages/van/src/vanilla.py"


def test_tar_builder_adds_file_path_pair_as_yt_and_local_tuple(tmp_path: Path) -> None:
    stage_dir = tmp_path / "st"
    stage_dir.mkdir()
    op = OmegaConf.create({"file_paths": [["//yt/secret", "local.pem"]]})
    r = _builder().build_dependencies(
        "map", stage_dir, "a.tar.gz", "//bf", op, OmegaConf.create({}), _LOG
    )
    assert ("//yt/secret", "local.pem") in r.dependencies


def test_tar_builder_adds_string_file_path_as_basename_local_name(
    tmp_path: Path,
) -> None:
    stage_dir = tmp_path / "st"
    stage_dir.mkdir()
    op = OmegaConf.create({"file_paths": ["//pool/data/blob.bin"]})
    r = _builder().build_dependencies(
        "map", stage_dir, "a.tar.gz", "//bf", op, OmegaConf.create({}), _LOG
    )
    assert ("//pool/data/blob.bin", "blob.bin") in r.dependencies


def test_tar_builder_adds_checkpoint_when_job_model_and_operation_checkpoint_set(
    tmp_path: Path,
) -> None:
    stage_dir = tmp_path / "st"
    stage_dir.mkdir()
    op = OmegaConf.create({"checkpoint": {"checkpoint_base": "//ckpt"}})
    st = OmegaConf.create({"job": {"model_name": "m1"}})
    r = _builder().build_dependencies(
        "map", stage_dir, "a.tar.gz", "//bf", op, st, _LOG
    )
    assert ("//ckpt/m1", "m1") in r.dependencies


def test_tar_builder_map_reduce_tar_bootstrap_sets_distinct_mapper_reducer_commands(
    tmp_path: Path,
) -> None:
    stage_dir = tmp_path / "mr_stage"
    stage_dir.mkdir()
    op = OmegaConf.create({"tar_command_bootstrap": True})
    r = _builder().build_dependencies(
        "map_reduce",
        stage_dir,
        "code.tar.gz",
        "//bf",
        op,
        OmegaConf.create({}),
        _LOG,
        mapper="./run_mapper.sh",
        reducer="./run_reducer.sh",
    )
    assert r.command is None
    assert r.mapper_command is not None
    assert r.mapper_command.startswith("bash -c '")
    assert r.reducer_command is not None
    assert r.reducer_command.startswith("bash -c '")
    assert r.mapper_command != r.reducer_command


def test_tar_builder_reduce_tar_bootstrap_sets_reducer_command_string_leg(
    tmp_path: Path,
) -> None:
    stage_dir = tmp_path / "red_st"
    stage_dir.mkdir()
    op = OmegaConf.create({"tar_command_bootstrap": True})
    r = _builder().build_dependencies(
        "reduce",
        stage_dir,
        "z.tar.gz",
        "//bf",
        op,
        OmegaConf.create({}),
        _LOG,
        reducer="./reduce.sh",
    )
    assert r.reducer_command is not None
    assert "bash -c '" in r.reducer_command
    assert r.mapper_command is None


def test_tar_builder_skips_tokenizer_dependency_when_name_unresolvable(
    tmp_path: Path,
) -> None:
    stage_dir = tmp_path / "tok_st"
    stage_dir.mkdir()
    op = OmegaConf.create(
        {"tokenizer_artifact": {"artifact_base": "//artifacts", "artifact_name": ""}}
    )
    r = _builder().build_dependencies(
        "map",
        stage_dir,
        "main.tar.gz",
        "//bf",
        op,
        OmegaConf.create({}),
        _LOG,
    )
    assert r.dependencies == [("//bf/main.tar.gz", "main.tar.gz")]


def test_tar_builder_warns_when_tokenizer_artifact_cannot_resolve_name(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    stage_dir = tmp_path / "w"
    stage_dir.mkdir()
    op = OmegaConf.create(
        {"tokenizer_artifact": {"artifact_base": "//a", "artifact_name": ""}}
    )
    _builder().build_dependencies(
        "map", stage_dir, "m.tar.gz", "//bf", op, OmegaConf.create({}), _LOG
    )
    assert "cannot be resolved" in caplog.text


def test_tar_builder_appends_tokenizer_artifact_when_resolved(tmp_path: Path) -> None:
    stage_dir = tmp_path / "tok_st"
    stage_dir.mkdir()
    op = OmegaConf.create(
        {
            "tokenizer_artifact": {
                "artifact_base": "//artifacts",
                "artifact_name": "mytok",
            }
        }
    )
    r = _builder().build_dependencies(
        "map",
        stage_dir,
        "main.tar.gz",
        "//bf",
        op,
        OmegaConf.create({}),
        _LOG,
    )
    assert ("//artifacts/mytok.tar.gz", "mytok.tar.gz") in r.dependencies
