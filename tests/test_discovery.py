"""Tests for yt_framework.core.discovery.discover_stages."""

import logging
from pathlib import Path

import pytest

from yt_framework.core.discovery import discover_stages


def test_discover_stages_returns_empty_when_stages_dir_missing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    log = logging.getLogger("test.discovery")
    assert discover_stages(tmp_path, log) == []
    assert any(
        "Stages directory not found" in r.getMessage() for r in caplog.records
    ), "missing stages/ should warn"


def test_discover_stages_skips_non_directory_entries_under_stages(
    tmp_path: Path,
) -> None:
    stages = tmp_path / "stages"
    stages.mkdir()
    (stages / "not_a_dir").write_text("", encoding="utf-8")
    log = logging.getLogger("test.discovery")
    assert discover_stages(tmp_path, log) == []


def test_discover_stages_skips_subdirectory_without_stage_py(tmp_path: Path) -> None:
    stages = tmp_path / "stages"
    (stages / "no_stage_module").mkdir(parents=True)
    log = logging.getLogger("test.discovery")
    assert discover_stages(tmp_path, log) == []


def _write_minimal_stage_tree(pipeline_dir: Path, stage_folder: str) -> None:
    pkg = pipeline_dir / "stages"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    stage_dir = pkg / stage_folder
    stage_dir.mkdir()
    (stage_dir / "__init__.py").write_text("", encoding="utf-8")
    (stage_dir / "stage.py").write_text(
        "from yt_framework.core.stage import BaseStage\n"
        "class DiscoveredStage(BaseStage):\n"
        "    def run(self, debug):\n"
        "        return debug\n",
        encoding="utf-8",
    )


def test_discover_stages_imports_base_stage_subclass_from_stage_module(
    tmp_path: Path,
) -> None:
    _write_minimal_stage_tree(tmp_path, "disc_ok")
    log = logging.getLogger("test.discovery")
    found = discover_stages(tmp_path, log)
    assert len(found) == 1 and found[0].__name__ == "DiscoveredStage", (
        "expected one concrete BaseStage subclass from stages/disc_ok/stage.py"
    )


def test_discover_stages_continues_when_stage_module_import_fails(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    stages = tmp_path / "stages"
    stages.mkdir()
    (stages / "__init__.py").write_text("", encoding="utf-8")
    bad = stages / "disc_bad"
    bad.mkdir()
    (bad / "__init__.py").write_text("", encoding="utf-8")
    (bad / "stage.py").write_text("this is not valid python\n", encoding="utf-8")
    log = logging.getLogger("test.discovery")
    assert discover_stages(tmp_path, log) == []
    assert any("Failed to import stage" in r.getMessage() for r in caplog.records)
