"""Tests for yt_framework.operations.map_reduce.run_map_reduce and run_reduce."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from omegaconf import OmegaConf

from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.stage import StageContext
from yt_framework.operations.dependency_strategy import DependencyBuildResult
from yt_framework.operations.map_reduce import run_map_reduce, run_reduce
from yt_framework.yt.client_base import BaseYTClient

_LOG = logging.getLogger("tests.map_reduce")
_LOG.addHandler(logging.NullHandler())


def _mr_stage_context(tmp_path: Path, name: str = "mr_stage") -> StageContext:
    stage_dir = tmp_path / name
    stage_dir.mkdir()
    configs = tmp_path / "configs"
    configs.mkdir()
    fake_yt = MagicMock(spec=BaseYTClient)
    op = MagicMock()
    op.id = "op-mr-1"
    fake_yt.run_map_reduce.return_value = op
    fake_yt.run_reduce.return_value = op
    fake_yt.wait_for_operation.return_value = True
    deps = PipelineStageDependencies(
        yt_client=fake_yt,
        pipeline_config=OmegaConf.create({"pipeline": {"build_folder": "//bf"}}),
        configs_dir=configs,
    )
    return StageContext(
        name=name,
        config=OmegaConf.create({}),
        stage_dir=stage_dir,
        logger=_LOG,
        deps=deps,
    )


def _minimal_map_reduce_cfg() -> OmegaConf:
    return OmegaConf.create(
        {
            "input_table": "//in/t",
            "output_table": "//out/t",
            "reduce_by": ["k"],
            "resources": {"pool": "default"},
        }
    )


def _minimal_reduce_cfg() -> OmegaConf:
    return OmegaConf.create(
        {
            "input_table": "//in/r",
            "output_table": "//out/r",
            "reduce_by": ["id"],
            "resources": {"pool": "default"},
        }
    )


def test_run_map_reduce_raises_when_reduce_by_empty(tmp_path: Path) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": [],
            "resources": {"pool": "p"},
        }
    )
    with pytest.raises(ValueError, match="reduce_by"):
        run_map_reduce(ctx, cfg, map_job="m", reduce_job="r")


def test_run_map_reduce_raises_when_mapper_and_map_job_differ(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = _minimal_map_reduce_cfg()
    with pytest.raises(ValueError, match="mapper.*map_job"):
        run_map_reduce(
            ctx, cfg, mapper="old", map_job="new", reducer="r", reduce_job="r"
        )


def test_run_map_reduce_warns_when_legacy_mapper_kwarg_used(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = _minimal_map_reduce_cfg()
    with pytest.warns(DeprecationWarning, match="map_job"):
        run_map_reduce(ctx, cfg, mapper="m", reduce_job="r")


def test_run_map_reduce_warns_when_legacy_reducer_kwarg_used(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = _minimal_map_reduce_cfg()
    with pytest.warns(DeprecationWarning, match="reduce_job"):
        run_map_reduce(ctx, cfg, map_job="m", reducer="r")


def test_run_map_reduce_raises_when_reducer_and_reduce_job_differ(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = _minimal_map_reduce_cfg()
    with pytest.raises(ValueError, match="reducer.*reduce_job"):
        run_map_reduce(ctx, cfg, map_job="m", reducer="old", reduce_job="new")


@patch(
    "yt_framework.operations.map_reduce.TarArchiveDependencyBuilder.build_dependencies"
)
def test_run_map_reduce_raises_on_partial_tar_bootstrap_commands(
    mock_build: MagicMock, tmp_path: Path
) -> None:
    mock_build.return_value = DependencyBuildResult(
        script_path="s",
        dependencies=[],
        command=None,
        mapper_command="only-mapper",
        reducer_command=None,
    )
    ctx = _mr_stage_context(tmp_path)
    cfg = _minimal_map_reduce_cfg()
    with pytest.raises(RuntimeError, match="partial map-reduce tar bootstrap"):
        run_map_reduce(ctx, cfg, map_job="m", reduce_job="r")


@patch(
    "yt_framework.operations.map_reduce.TarArchiveDependencyBuilder.build_dependencies"
)
def test_run_map_reduce_replaces_string_legs_when_both_tar_bootstrap_commands_set(
    mock_build: MagicMock, tmp_path: Path
) -> None:
    mock_build.return_value = DependencyBuildResult(
        script_path="s",
        dependencies=[("local", "remote")],
        command=None,
        mapper_command="bash -c mapper-wrap",
        reducer_command="bash -c reducer-wrap",
    )
    ctx = _mr_stage_context(tmp_path)
    cfg = _minimal_map_reduce_cfg()
    assert run_map_reduce(ctx, cfg, map_job="m", reduce_job="r") is True
    kwargs = ctx.deps.yt_client.run_map_reduce.call_args.kwargs
    assert kwargs["mapper"] == "bash -c mapper-wrap"
    assert kwargs["reducer"] == "bash -c reducer-wrap"


def test_run_map_reduce_forwards_string_operation_description_as_title(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": ["k"],
            "resources": {"pool": "p"},
            "operation_description": "mr-label",
        }
    )
    assert run_map_reduce(ctx, cfg, map_job="m", reduce_job="r") is True
    kw = ctx.deps.yt_client.run_map_reduce.call_args.kwargs
    assert kw.get("title") == "mr-label"


def test_run_map_reduce_forwards_dict_operation_description(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": ["k"],
            "resources": {"pool": "p"},
            "operation_description": {"kind": "mr"},
        }
    )
    assert run_map_reduce(ctx, cfg, map_job="m", reduce_job="r") is True
    kw = ctx.deps.yt_client.run_map_reduce.call_args.kwargs
    assert kw.get("operation_description") == {"kind": "mr"}


def test_run_map_reduce_forwards_max_row_weight_override(tmp_path: Path) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": ["k"],
            "resources": {"pool": "p"},
            "max_row_weight": "64M",
        }
    )
    assert run_map_reduce(ctx, cfg, map_job="m", reduce_job="r") is True
    kw = ctx.deps.yt_client.run_map_reduce.call_args.kwargs
    assert kw.get("max_row_weight") == "64M"


def test_run_reduce_raises_when_reduce_by_empty(tmp_path: Path) -> None:
    ctx = _mr_stage_context(tmp_path, name="red_empty")
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": [],
            "resources": {"pool": "p"},
        }
    )
    with pytest.raises(ValueError, match="reduce_by"):
        run_reduce(ctx, cfg, job="r")


def test_run_map_reduce_returns_false_when_wait_reports_failure(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path)
    ctx.deps.yt_client.wait_for_operation.return_value = False
    assert (
        run_map_reduce(
            ctx,
            _minimal_map_reduce_cfg(),
            map_job="m",
            reduce_job="r",
        )
        is False
    )


def test_run_map_reduce_calls_client_with_map_job_count(tmp_path: Path) -> None:
    ctx = _mr_stage_context(tmp_path)
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": ["x"],
            "map_job_count": 7,
            "resources": {"pool": "p"},
        }
    )
    assert run_map_reduce(ctx, cfg, map_job="m", reduce_job="r") is True
    assert ctx.deps.yt_client.run_map_reduce.call_args.kwargs.get("map_job_count") == 7


def test_run_reduce_returns_false_when_wait_reports_failure(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path, name="red_stage")
    ctx.deps.yt_client.wait_for_operation.return_value = False
    assert run_reduce(ctx, _minimal_reduce_cfg(), job="r") is False


def test_run_reduce_raises_when_reducer_and_job_differ(tmp_path: Path) -> None:
    ctx = _mr_stage_context(tmp_path, name="red_stage")
    cfg = _minimal_reduce_cfg()
    with pytest.raises(ValueError, match="reducer.*job"):
        run_reduce(ctx, cfg, reducer="a", job="b")


def test_run_reduce_forwards_dict_operation_description(tmp_path: Path) -> None:
    ctx = _mr_stage_context(tmp_path, name="red_stage")
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": ["k"],
            "resources": {"pool": "p"},
            "operation_description": {"label": "x"},
        }
    )
    assert run_reduce(ctx, cfg, job="my-reducer") is True
    kw = ctx.deps.yt_client.run_reduce.call_args.kwargs
    assert kw.get("operation_description") == {"label": "x"}


def test_run_reduce_forwards_string_operation_description_as_title(
    tmp_path: Path,
) -> None:
    ctx = _mr_stage_context(tmp_path, name="red_title")
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": ["k"],
            "resources": {"pool": "p"},
            "operation_description": "reduce-title",
        }
    )
    assert run_reduce(ctx, cfg, job="r") is True
    kw = ctx.deps.yt_client.run_reduce.call_args.kwargs
    assert kw.get("title") == "reduce-title"


def test_run_reduce_forwards_max_row_weight_override(tmp_path: Path) -> None:
    ctx = _mr_stage_context(tmp_path, name="red_mrw")
    cfg = OmegaConf.create(
        {
            "input_table": "//i",
            "output_table": "//o",
            "reduce_by": ["k"],
            "resources": {"pool": "p"},
            "max_row_weight": "64M",
        }
    )
    assert run_reduce(ctx, cfg, job="r") is True
    kw = ctx.deps.yt_client.run_reduce.call_args.kwargs
    assert kw.get("max_row_weight") == "64M"


@patch(
    "yt_framework.operations.map_reduce.TarArchiveDependencyBuilder.build_dependencies"
)
def test_run_reduce_replaces_reducer_when_tar_bootstrap_command_set(
    mock_build: MagicMock, tmp_path: Path
) -> None:
    mock_build.return_value = DependencyBuildResult(
        script_path="s",
        dependencies=[],
        command=None,
        reducer_command="bash -c reduce-wrap",
    )
    ctx = _mr_stage_context(tmp_path, name="red_tar")
    cfg = _minimal_reduce_cfg()
    assert run_reduce(ctx, cfg, job="orig") is True
    kw = ctx.deps.yt_client.run_reduce.call_args.kwargs
    assert kw["reducer"] == "bash -c reduce-wrap"
