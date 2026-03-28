"""Tests for yt_framework.operations.tokenizer_artifact name resolution helpers."""

from omegaconf import OmegaConf

from yt_framework.operations.tokenizer_artifact import (
    resolve_tokenizer_archive_name,
    resolve_tokenizer_artifact_name,
    resolve_tokenizer_artifact_yt_path,
)


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


def test_resolve_tokenizer_artifact_yt_path_joins_base_and_archive_name() -> None:
    stage = OmegaConf.create({})
    tok = OmegaConf.create({"artifact_base": "//tmp/artifacts", "artifact_name": "t"})
    assert resolve_tokenizer_artifact_yt_path(stage, tok) == "//tmp/artifacts/t.tar.gz"
