"""Tests for yt_framework.core.stage.StageContext (fork semantics)."""

import logging
from pathlib import Path
from unittest.mock import MagicMock

from omegaconf import OmegaConf

from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.stage import StageContext
from yt_framework.yt.client_base import BaseYTClient

_LOG = logging.getLogger("tests.stage")


def _minimal_stage_context(tmp_path: Path) -> StageContext:
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    deps = PipelineStageDependencies(
        yt_client=MagicMock(spec=BaseYTClient),
        pipeline_config=OmegaConf.create({}),
        configs_dir=cfg_dir,
    )
    stage_dir = tmp_path / "stage_a"
    stage_dir.mkdir()
    return StageContext(
        name="alpha",
        config=OmegaConf.create({"k": 1}),
        stage_dir=stage_dir,
        logger=_LOG,
        deps=deps,
    )


def test_stage_context_fork_overrides_name_and_stage_dir(tmp_path: Path) -> None:
    ctx = _minimal_stage_context(tmp_path)
    other_dir = tmp_path / "stage_b"
    other_dir.mkdir()
    forked = ctx.fork(name="beta", stage_dir=other_dir)
    assert (
        forked.name == "beta" and forked.stage_dir == other_dir
    ), "fork should replace name and stage_dir"


def test_stage_context_fork_preserves_config_logger_and_deps(tmp_path: Path) -> None:
    ctx = _minimal_stage_context(tmp_path)
    other_dir = tmp_path / "stage_b"
    other_dir.mkdir()
    forked = ctx.fork(name="beta", stage_dir=other_dir)
    assert (
        forked.config is ctx.config
        and forked.logger is ctx.logger
        and forked.deps is ctx.deps
    )


def test_stage_context_fork_with_explicit_none_keeps_parent_name_and_dir(
    tmp_path: Path,
) -> None:
    ctx = _minimal_stage_context(tmp_path)
    forked = ctx.fork(name=None, stage_dir=None)
    assert forked.name == ctx.name and forked.stage_dir == ctx.stage_dir
