"""Tests for yt_framework.utils.env: .env loading helpers."""

import warnings
from pathlib import Path

import pytest

from yt_framework.utils.env import load_env_file, load_secrets


def test_load_env_file_missing_returns_empty_without_warning(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "nope.env"
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always", UserWarning)
        result = load_env_file(missing)
    assert result == {}, "missing optional file should yield no keys"
    assert recorded == [], "optional missing file must not emit UserWarning"


def test_load_env_file_parses_non_comment_lines(tmp_path: Path) -> None:
    path = tmp_path / "app.env"
    path.write_text("FOO=bar\n# skip\n\nBAZ= qux \n")
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        result = load_env_file(path)
    assert result == {"FOO": "bar", "BAZ": "qux"}


def test_load_env_file_splits_on_first_equals_only(tmp_path: Path) -> None:
    path = tmp_path / "k.env"
    path.write_text("K=a=b\n")
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        result = load_env_file(path)
    assert result == {"K": "a=b"}, "value may contain '='"


def test_load_secrets_loads_named_file_in_directory(tmp_path: Path) -> None:
    cfg = tmp_path / "configs"
    cfg.mkdir()
    (cfg / "secrets.env").write_text("YT_TOKEN=secret\n")
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        out = load_secrets(cfg)
    assert out == {"YT_TOKEN": "secret"}


def test_load_secrets_respects_custom_env_filename(tmp_path: Path) -> None:
    (tmp_path / "local.env").write_text("X=1\n")
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        out = load_secrets(tmp_path, env_file="local.env")
    assert out == {"X": "1"}


def test_load_env_file_read_error_warns_and_returns_empty(tmp_path: Path) -> None:
    not_a_file = tmp_path / "is_dir"
    not_a_file.mkdir()
    with pytest.warns(UserWarning, match="Could not load"):
        result = load_env_file(not_a_file)
    assert result == {}
