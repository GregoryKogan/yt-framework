"""Tests for ytjobs.config.get_config_path."""

import os
from pathlib import Path

import pytest

from ytjobs.config import get_config_path


def test_get_config_path_reads_job_config_path_env(tmp_path: Path) -> None:
    cfg = tmp_path / "job.yaml"
    cfg.write_text("x: 1\n", encoding="utf-8")
    old = os.environ.get("JOB_CONFIG_PATH")
    try:
        os.environ["JOB_CONFIG_PATH"] = str(cfg)
        assert get_config_path() == cfg
    finally:
        if old is None:
            os.environ.pop("JOB_CONFIG_PATH", None)
        else:
            os.environ["JOB_CONFIG_PATH"] = old


def test_get_config_path_raises_when_env_missing() -> None:
    old = os.environ.pop("JOB_CONFIG_PATH", None)
    try:
        with pytest.raises(ValueError, match="JOB_CONFIG_PATH"):
            get_config_path()
    finally:
        if old is not None:
            os.environ["JOB_CONFIG_PATH"] = old
