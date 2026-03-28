"""Tests for yt_framework.operations.vanilla.run_vanilla."""

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf

from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.stage import StageContext
from yt_framework.operations.vanilla import run_vanilla
from yt_framework.yt.client_base import BaseYTClient

_LOG = logging.getLogger("tests.vanilla")
_LOG.addHandler(logging.NullHandler())


def _vanilla_stage_context(tmp_path: Path) -> StageContext:
    stage_dir = tmp_path / "van_stage"
    stage_dir.mkdir()
    configs = tmp_path / "configs"
    configs.mkdir()
    fake_yt = MagicMock(spec=BaseYTClient)
    op = MagicMock()
    op.id = "op-vanilla-1"
    fake_yt.run_vanilla.return_value = op
    fake_yt.wait_for_operation.return_value = True
    deps = PipelineStageDependencies(
        yt_client=fake_yt,
        pipeline_config=OmegaConf.create({"pipeline": {"build_folder": "//bf"}}),
        configs_dir=configs,
    )
    return StageContext(
        name="van_stage",
        config=OmegaConf.create({}),
        stage_dir=stage_dir,
        logger=_LOG,
        deps=deps,
    )


def _minimal_vanilla_config() -> OmegaConf:
    return OmegaConf.create({"resources": {"pool": "default"}})


def test_run_vanilla_returns_false_when_client_returns_none(tmp_path: Path) -> None:
    ctx = _vanilla_stage_context(tmp_path)
    ctx.deps.yt_client.run_vanilla.return_value = None
    assert run_vanilla(ctx, _minimal_vanilla_config()) is False


def test_run_vanilla_returns_false_when_wait_reports_failure(tmp_path: Path) -> None:
    ctx = _vanilla_stage_context(tmp_path)
    ctx.deps.yt_client.wait_for_operation.return_value = False
    assert run_vanilla(ctx, _minimal_vanilla_config()) is False


def test_run_vanilla_passes_task_name_from_context(tmp_path: Path) -> None:
    ctx = _vanilla_stage_context(tmp_path)
    assert run_vanilla(ctx, _minimal_vanilla_config()) is True
    assert (
        ctx.deps.yt_client.run_vanilla.call_args.kwargs["task_name"] == "van_stage"
    ), "task_name must match stage context name"


def test_run_vanilla_forwards_string_operation_description_as_title(
    tmp_path: Path,
) -> None:
    ctx = _vanilla_stage_context(tmp_path)
    cfg = OmegaConf.create(
        {"resources": {"pool": "p"}, "operation_description": "vanilla-job"}
    )
    assert run_vanilla(ctx, cfg) is True
    assert ctx.deps.yt_client.run_vanilla.call_args.kwargs.get("title") == "vanilla-job"
