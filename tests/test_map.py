"""Tests for yt_framework.operations.map.run_map."""

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf

from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.stage import StageContext
from yt_framework.operations.map import run_map
from yt_framework.yt.client_base import BaseYTClient

_LOG = logging.getLogger("tests.map")
_LOG.addHandler(logging.NullHandler())


def _map_stage_context(tmp_path: Path) -> StageContext:
    stage_dir = tmp_path / "map_stage"
    stage_dir.mkdir()
    configs = tmp_path / "configs"
    configs.mkdir()
    fake_yt = MagicMock(spec=BaseYTClient)
    op = MagicMock()
    op.id = "op-map-1"
    fake_yt.run_map.return_value = op
    fake_yt.wait_for_operation.return_value = True
    deps = PipelineStageDependencies(
        yt_client=fake_yt,
        pipeline_config=OmegaConf.create({"pipeline": {"build_folder": "//bf"}}),
        configs_dir=configs,
    )
    return StageContext(
        name="map_stage",
        config=OmegaConf.create({}),
        stage_dir=stage_dir,
        logger=_LOG,
        deps=deps,
    )


def _minimal_map_config() -> OmegaConf:
    return OmegaConf.create(
        {
            "input_table": "//in/t",
            "output_table": "//out/t",
            "resources": {"pool": "default"},
        }
    )


def test_run_map_raises_when_input_table_missing(tmp_path: Path) -> None:
    ctx = _map_stage_context(tmp_path)
    cfg = OmegaConf.create({"output_table": "//o"})
    with pytest.raises(ValueError, match="input_table"):
        run_map(ctx, cfg)


def test_run_map_raises_when_output_table_missing(tmp_path: Path) -> None:
    ctx = _map_stage_context(tmp_path)
    cfg = OmegaConf.create({"input_table": "//i"})
    with pytest.raises(ValueError, match="output_table"):
        run_map(ctx, cfg)


def test_run_map_raises_when_mapper_and_job_conflict(tmp_path: Path) -> None:
    ctx = _map_stage_context(tmp_path)
    cfg = _minimal_map_config()
    with pytest.raises(ValueError, match="mapper.*job"):
        run_map(ctx, cfg, mapper="a", job="b")


def test_run_map_returns_false_when_wait_reports_failure(tmp_path: Path) -> None:
    ctx = _map_stage_context(tmp_path)
    ctx.deps.yt_client.wait_for_operation.return_value = False
    assert run_map(ctx, _minimal_map_config()) is False


def test_run_map_forwards_string_operation_description_as_title(
    tmp_path: Path,
) -> None:
    ctx = _map_stage_context(tmp_path)
    cfg = OmegaConf.create(
        {
            "input_table": "//in/t",
            "output_table": "//out/t",
            "resources": {"pool": "p"},
            "operation_description": "my-label",
        }
    )
    assert run_map(ctx, cfg) is True
    call_kw = ctx.deps.yt_client.run_map.call_args.kwargs
    assert call_kw.get("title") == "my-label"


def test_run_map_forwards_dict_operation_description(tmp_path: Path) -> None:
    ctx = _map_stage_context(tmp_path)
    cfg = OmegaConf.create(
        {
            "input_table": "//in/t",
            "output_table": "//out/t",
            "resources": {"pool": "p"},
            "operation_description": {"foo": 1},
        }
    )
    assert run_map(ctx, cfg) is True
    call_kw = ctx.deps.yt_client.run_map.call_args.kwargs
    assert call_kw.get("operation_description") == {"foo": 1}
