"""Tests for yt_framework.operations.tokenizer_artifact name resolution helpers."""

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf

from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.stage import StageContext
from yt_framework.operations.tokenizer_artifact import (
    _prepare_local_archive,
    init_tokenizer_artifact_directory,
    resolve_tokenizer_archive_name,
    resolve_tokenizer_artifact_name,
    resolve_tokenizer_artifact_yt_path,
)


def _null_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


def test_resolve_tokenizer_artifact_name_uses_explicit_artifact_name() -> None:
    stage = OmegaConf.create({})
    tok = OmegaConf.create({"artifact_name": "  my_tok  "})
    assert resolve_tokenizer_artifact_name(stage, tok) == "my_tok"


def test_resolve_tokenizer_artifact_name_falls_back_to_job_tokenizer_name() -> None:
    stage = OmegaConf.create({"job": {"tokenizer_name": "tok_a"}})
    tok = OmegaConf.create({})
    assert resolve_tokenizer_artifact_name(stage, tok) == "tok_a"


def test_resolve_tokenizer_artifact_name_uses_model_name_basename_when_no_tokenizer() -> (
    None
):
    stage = OmegaConf.create({"job": {"model_name": "hub/org/weights"}})
    tok = OmegaConf.create({})
    assert resolve_tokenizer_artifact_name(stage, tok) == "weights"


def test_resolve_tokenizer_artifact_name_uses_local_artifact_path_filename() -> None:
    stage = OmegaConf.create({})
    tok = OmegaConf.create({"local_artifact_path": "/data/models/foo.tar.gz"})
    assert resolve_tokenizer_artifact_name(stage, tok) == "foo"


def test_resolve_tokenizer_artifact_name_returns_none_when_unresolvable() -> None:
    stage = OmegaConf.create({})
    tok = OmegaConf.create({})
    assert resolve_tokenizer_artifact_name(stage, tok) is None


def test_resolve_tokenizer_archive_name_appends_tar_gz_when_missing() -> None:
    assert resolve_tokenizer_archive_name("art") == "art.tar.gz"


def test_resolve_tokenizer_archive_name_preserves_existing_tar_gz_suffix() -> None:
    assert resolve_tokenizer_archive_name("art.tar.gz") == "art.tar.gz"


def test_resolve_tokenizer_artifact_yt_path_returns_none_without_artifact_base() -> (
    None
):
    stage = OmegaConf.create({})
    tok = OmegaConf.create({"artifact_name": "x"})
    assert resolve_tokenizer_artifact_yt_path(stage, tok) is None


def test_resolve_tokenizer_artifact_yt_path_returns_none_when_name_unresolvable() -> (
    None
):
    stage = OmegaConf.create({})
    tok = OmegaConf.create({"artifact_base": "//yt/pool/artifacts"})
    assert resolve_tokenizer_artifact_yt_path(stage, tok) is None


def test_resolve_tokenizer_artifact_yt_path_joins_base_and_archive_name() -> None:
    stage = OmegaConf.create({})
    tok = OmegaConf.create({"artifact_base": "//tmp/artifacts", "artifact_name": "t"})
    assert resolve_tokenizer_artifact_yt_path(stage, tok) == "//tmp/artifacts/t.tar.gz"


def test_prepare_local_archive_returns_same_path_for_existing_tar_gz(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "bundle.tar.gz"
    archive.write_bytes(b"gz")
    assert _prepare_local_archive(archive, "bundle") == archive


def test_prepare_local_archive_packs_directory_to_temporary_tar_gz(
    tmp_path: Path,
) -> None:
    src = tmp_path / "tokdir"
    src.mkdir()
    (src / "x.txt").write_text("hi", encoding="utf-8")
    out = _prepare_local_archive(src, "tok")
    assert out.is_file() and out.name.endswith(".tar.gz") and out != src
    out.unlink(missing_ok=True)


def test_prepare_local_archive_raises_for_non_tar_file(
    tmp_path: Path,
) -> None:
    f = tmp_path / "plain.bin"
    f.write_bytes(b"no")
    with pytest.raises(ValueError, match="directory or to a .tar.gz file"):
        _prepare_local_archive(f, "p")


def test_init_tokenizer_artifact_directory_raises_when_base_set_but_name_unresolvable(
    tmp_path: Path,
) -> None:
    yt = MagicMock()
    deps = PipelineStageDependencies(
        yt_client=yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=tmp_path,
    )
    ctx = StageContext(
        name="tok_st",
        config=OmegaConf.create({}),
        stage_dir=tmp_path,
        logger=_null_logger("tests.tok.bad_name"),
        deps=deps,
    )
    tok = OmegaConf.create({"artifact_base": "//yt/artifacts"})
    with pytest.raises(ValueError, match="artifact_name cannot be resolved"):
        init_tokenizer_artifact_directory(ctx, tok)


def test_init_tokenizer_artifact_directory_no_ops_without_artifact_base(
    tmp_path: Path,
) -> None:
    yt = MagicMock()
    deps = PipelineStageDependencies(
        yt_client=yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=tmp_path,
    )
    ctx = StageContext(
        name="tok_st",
        config=OmegaConf.create({}),
        stage_dir=tmp_path,
        logger=_null_logger("tests.tok.init"),
        deps=deps,
    )
    init_tokenizer_artifact_directory(ctx, OmegaConf.create({"artifact_name": "x"}))
    yt.create_path.assert_not_called()


def test_init_tokenizer_artifact_directory_verifies_yt_when_no_local_path(
    tmp_path: Path,
) -> None:
    yt = MagicMock()
    yt.exists.return_value = True
    deps = PipelineStageDependencies(
        yt_client=yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=tmp_path,
    )
    ctx = StageContext(
        name="tok_st",
        config=OmegaConf.create({"job": {"tokenizer_name": "mytok"}}),
        stage_dir=tmp_path,
        logger=_null_logger("tests.tok.verify"),
        deps=deps,
    )
    tok = OmegaConf.create({"artifact_base": "//yt/pool/artifacts"})
    init_tokenizer_artifact_directory(ctx, tok)
    yt.create_path.assert_called_once_with("//yt/pool/artifacts", node_type="map_node")
    yt.upload_file.assert_not_called()


def test_init_tokenizer_artifact_directory_uploads_packed_dir_when_missing_in_yt(
    tmp_path: Path,
) -> None:
    yt = MagicMock()
    yt.exists.side_effect = [False, True]
    src = tmp_path / "local_tok"
    src.mkdir()
    (src / "vocab.txt").write_text("a", encoding="utf-8")
    deps = PipelineStageDependencies(
        yt_client=yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=tmp_path,
    )
    ctx = StageContext(
        name="tok_st",
        config=OmegaConf.create({}),
        stage_dir=tmp_path,
        logger=_null_logger("tests.tok.up"),
        deps=deps,
    )
    tok = OmegaConf.create(
        {
            "artifact_base": "//yt/art",
            "artifact_name": "packed",
            "local_artifact_path": str(src),
        }
    )
    init_tokenizer_artifact_directory(ctx, tok)
    yt.upload_file.assert_called_once()
    args, kwargs = yt.upload_file.call_args
    assert kwargs.get("create_parent_dir") is True
    local_sent, yt_dest = args[0], args[1]
    assert (
        str(local_sent).endswith(".tar.gz") and str(yt_dest) == "//yt/art/packed.tar.gz"
    )


def test_init_tokenizer_artifact_directory_warns_when_local_path_missing_but_yt_has_artifact(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    yt = MagicMock()
    yt.exists.return_value = True
    deps = PipelineStageDependencies(
        yt_client=yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=tmp_path,
    )
    ctx = StageContext(
        name="tok_st",
        config=OmegaConf.create({"job": {"tokenizer_name": "tok"}}),
        stage_dir=tmp_path,
        logger=_null_logger("tests.tok.miss_local_ok"),
        deps=deps,
    )
    missing = tmp_path / "nope" / "missing.tar.gz"
    tok = OmegaConf.create(
        {
            "artifact_base": "//yt/art",
            "local_artifact_path": str(missing),
        }
    )
    init_tokenizer_artifact_directory(ctx, tok)
    assert (
        "local_artifact_path does not exist" in caplog.text
        and not yt.upload_file.called
    ), "missing local path should warn and skip upload when YT already has the file"


def test_init_tokenizer_artifact_directory_skips_upload_when_local_exists_and_yt_has_artifact(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    yt = MagicMock()
    yt.exists.return_value = True
    deps = PipelineStageDependencies(
        yt_client=yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=tmp_path,
    )
    ctx = StageContext(
        name="tok_st",
        config=OmegaConf.create({"job": {"tokenizer_name": "tok"}}),
        stage_dir=tmp_path,
        logger=_null_logger("tests.tok.skip_up"),
        deps=deps,
    )
    local_tgz = tmp_path / "tok.tar.gz"
    local_tgz.write_bytes(b"x")
    tok = OmegaConf.create(
        {
            "artifact_base": "//yt/art",
            "local_artifact_path": str(local_tgz),
        }
    )
    init_tokenizer_artifact_directory(ctx, tok)
    assert (
        "already exists in YT" in caplog.text and not yt.upload_file.called
    ), "existing YT artifact should log skip and not upload"


def test_init_tokenizer_artifact_directory_raises_when_local_missing_and_yt_has_no_artifact(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    yt = MagicMock()
    yt.exists.return_value = False
    deps = PipelineStageDependencies(
        yt_client=yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=tmp_path,
    )
    ctx = StageContext(
        name="tok_st",
        config=OmegaConf.create({"job": {"tokenizer_name": "tok"}}),
        stage_dir=tmp_path,
        logger=_null_logger("tests.tok.miss_local_fnf"),
        deps=deps,
    )
    missing = tmp_path / "gone" / "artifact.tar.gz"
    tok = OmegaConf.create(
        {
            "artifact_base": "//yt/pool/artifacts",
            "local_artifact_path": str(missing),
        }
    )
    with pytest.raises(FileNotFoundError, match="Tokenizer artifact not found in YT"):
        init_tokenizer_artifact_directory(ctx, tok)
    assert "local_artifact_path does not exist" in caplog.text
