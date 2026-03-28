"""Tests for yt_framework.operations.sort.run_sort."""

import logging
from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf

from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.stage import StageContext
from yt_framework.operations.sort import run_sort
from yt_framework.yt.client_base import BaseYTClient

_LOG = logging.getLogger("tests.sort")


def _context_with_yt() -> StageContext:
    fake_yt = MagicMock(spec=BaseYTClient)
    deps = PipelineStageDependencies(
        yt_client=fake_yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=MagicMock(),
    )
    return StageContext(
        name="s",
        config=OmegaConf.create({}),
        stage_dir=MagicMock(),
        logger=_LOG,
        deps=deps,
    )


def test_run_sort_raises_when_input_table_missing() -> None:
    ctx = _context_with_yt()
    cfg = OmegaConf.create({"sort_by": ["a"]})
    with pytest.raises(ValueError, match="input_table"):
        run_sort(ctx, cfg)


def test_run_sort_raises_when_sort_by_empty() -> None:
    ctx = _context_with_yt()
    cfg = OmegaConf.create({"input_table": "//tmp/t", "sort_by": []})
    with pytest.raises(ValueError, match="sort_by"):
        run_sort(ctx, cfg)


def test_run_sort_calls_yt_client_run_sort_with_pool_from_resources() -> None:
    ctx = _context_with_yt()
    cfg = OmegaConf.create(
        {
            "input_table": "//tmp/t",
            "sort_by": ["shard", "id"],
            "resources": {"pool": "heavy", "pool_tree": "default"},
        }
    )
    assert run_sort(ctx, cfg) is True
    ctx.deps.yt_client.run_sort.assert_called_once_with(
        table_path="//tmp/t",
        sort_by=["shard", "id"],
        pool="heavy",
        pool_tree="default",
    )
