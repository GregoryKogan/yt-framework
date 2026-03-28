"""Tests for yt_framework.core.pipeline config normalization helpers."""

import pytest
from omegaconf import OmegaConf

from yt_framework.core.pipeline import (
    _normalize_upload_modules,
    _normalize_upload_paths,
)


def test_normalize_upload_modules_none_returns_empty_list() -> None:
    assert _normalize_upload_modules(None) == []


def test_normalize_upload_modules_blank_string_returns_empty_list() -> None:
    assert _normalize_upload_modules("   ") == []


def test_normalize_upload_modules_non_blank_string_returns_single_element() -> None:
    assert _normalize_upload_modules("  mod_a  ") == ["mod_a"]


def test_normalize_upload_modules_list_strips_entries() -> None:
    raw = OmegaConf.create([" a ", "b", ""])
    assert _normalize_upload_modules(raw) == ["a", "b"]


def test_normalize_upload_modules_rejects_non_sequence_non_string() -> None:
    with pytest.raises(ValueError, match="upload_modules"):
        _normalize_upload_modules(42)


def test_normalize_upload_paths_none_returns_empty_list() -> None:
    assert _normalize_upload_paths(None) == []


def test_normalize_upload_paths_rejects_non_list_container() -> None:
    with pytest.raises(ValueError, match="upload_paths must be a list"):
        _normalize_upload_paths({"source": "x"})


def test_normalize_upload_paths_requires_source_key() -> None:
    with pytest.raises(ValueError, match="missing required 'source'"):
        _normalize_upload_paths([{"target": "only"}])


def test_normalize_upload_paths_converts_dictconfig_element_to_str_mapping() -> None:
    raw = OmegaConf.create([{"source": "/src", "target": "//yt/t"}])
    out = _normalize_upload_paths(raw)
    assert out == [{"source": "/src", "target": "//yt/t"}]
