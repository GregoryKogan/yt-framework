"""Tests for yt_framework.core.pipeline_config normalization helpers."""

import pytest
from omegaconf import OmegaConf

from yt_framework.core.pipeline_config import (
    enabled_stage_names,
    normalize_upload_modules,
    normalize_upload_paths,
    pickling_dict_from_config,
    yt_mode_from_pipeline_config,
)


def test_normalize_upload_modules_none() -> None:
    assert normalize_upload_modules(None) == []


def test_normalize_upload_modules_invalid_type_raises() -> None:
    with pytest.raises(ValueError, match="upload_modules"):
        normalize_upload_modules(3.14)


def test_yt_mode_from_pipeline_config_none() -> None:
    assert yt_mode_from_pipeline_config(None) is None


def test_yt_mode_from_pipeline_config_accepts_prod_and_dev_with_whitespace() -> None:
    assert yt_mode_from_pipeline_config("  PROD ") == "prod"
    assert yt_mode_from_pipeline_config("dev") == "dev"


def test_yt_mode_from_pipeline_config_invalid_raises() -> None:
    with pytest.raises(ValueError, match="pipeline.mode"):
        yt_mode_from_pipeline_config("staging")


def test_pickling_dict_from_config_empty() -> None:
    assert pickling_dict_from_config(None) == {}
    assert pickling_dict_from_config({}) == {}


def test_pickling_dict_from_config_mapping_returns_plain_dict() -> None:
    cfg = OmegaConf.create({"ignore_system_modules": True})
    assert pickling_dict_from_config(cfg) == {"ignore_system_modules": True}


def test_pickling_dict_from_config_none_container_returns_empty_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from yt_framework.core import pipeline_config as pc

    cfg = OmegaConf.create({"a": 1})
    monkeypatch.setattr(
        pc.OmegaConf,
        "to_container",
        lambda *_a, **_k: None,
    )
    assert pc.pickling_dict_from_config(cfg) == {}


def test_pickling_dict_from_config_non_mapping_raises() -> None:
    raw = OmegaConf.create(["a"])
    with pytest.raises(TypeError, match="pipeline.pickling"):
        pickling_dict_from_config(raw)


def test_enabled_stage_names_none_returns_empty() -> None:
    assert enabled_stage_names(None) == []


def test_enabled_stage_names_list_via_omega() -> None:
    assert enabled_stage_names(OmegaConf.create(["a", "b"])) == ["a", "b"]


def test_enabled_stage_names_scalar_non_string() -> None:
    assert enabled_stage_names(42) == ["42"]


def test_enabled_stage_names_blank_string_returns_empty() -> None:
    assert enabled_stage_names("   ") == []


def test_normalize_upload_paths_coerces_dictconfig_element() -> None:
    raw = OmegaConf.create([{"source": "/x"}])
    assert normalize_upload_paths(raw) == [{"source": "/x"}]
