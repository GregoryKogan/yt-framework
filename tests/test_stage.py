"""Tests for yt_framework.core.stage.StageContext (fork semantics) and BaseStage."""

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
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


def _load_stage_impl_module(
    tmp_path: Path, impl_body: str, config_text: str | None
) -> object:
    stage_dir = tmp_path / "plugin_stage"
    stage_dir.mkdir(parents=True)
    if config_text is not None:
        (stage_dir / "config.yaml").write_text(config_text, encoding="utf-8")
    impl_path = stage_dir / "impl.py"
    impl_path.write_text(impl_body, encoding="utf-8")
    mod_name = f"_test_stage_impl_{id(stage_dir)}"
    spec = importlib.util.spec_from_file_location(mod_name, impl_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _deps_and_logger(cfg_dir: Path) -> tuple[PipelineStageDependencies, logging.Logger]:
    cfg_dir.mkdir(parents=True, exist_ok=True)
    deps = PipelineStageDependencies(
        yt_client=MagicMock(spec=BaseYTClient),
        pipeline_config=OmegaConf.create({}),
        configs_dir=cfg_dir,
    )
    return deps, _LOG


def test_base_stage_raises_file_not_found_when_config_yaml_missing(
    tmp_path: Path,
) -> None:
    mod = _load_stage_impl_module(
        tmp_path,
        impl_body=(
            "from yt_framework.core.stage import BaseStage\n"
            "class S(BaseStage):\n"
            "    def run(self, debug):\n"
            "        return {}\n"
        ),
        config_text=None,
    )
    deps, logger = _deps_and_logger(tmp_path / "cfg")
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        mod.S(deps, logger)


def test_base_stage_raises_value_error_when_config_is_not_mapping(
    tmp_path: Path,
) -> None:
    mod = _load_stage_impl_module(
        tmp_path,
        impl_body=(
            "from yt_framework.core.stage import BaseStage\n"
            "class S(BaseStage):\n"
            "    def run(self, debug):\n"
            "        return {}\n"
        ),
        config_text="- a\n",
    )
    deps, logger = _deps_and_logger(tmp_path / "cfg")
    with pytest.raises(ValueError, match="must contain a dictionary"):
        mod.S(deps, logger)


def test_base_stage_context_property_returns_matching_stage_context(
    tmp_path: Path,
) -> None:
    mod = _load_stage_impl_module(
        tmp_path,
        impl_body=(
            "from yt_framework.core.stage import BaseStage\n"
            "class S(BaseStage):\n"
            "    def run(self, debug):\n"
            "        return {}\n"
        ),
        config_text="pipeline: {}\n",
    )
    deps, logger = _deps_and_logger(tmp_path / "cfg")
    stage = mod.S(deps, logger)
    ctx = stage.context
    assert (
        isinstance(ctx, StageContext)
        and ctx.name == "plugin_stage"
        and ctx.logger is logger
        and ctx.deps is deps
        and ctx.stage_dir == Path(mod.__file__).parent
    ), "context should mirror BaseStage auto-detected name, paths, logger, deps"


def test_base_stage_run_can_delegate_to_super_pass_body(tmp_path: Path) -> None:
    mod = _load_stage_impl_module(
        tmp_path,
        impl_body=(
            "from yt_framework.core.stage import BaseStage\n"
            "class S(BaseStage):\n"
            "    def run(self, debug):\n"
            "        return super().run(debug)\n"
        ),
        config_text="{}\n",
    )
    deps, logger = _deps_and_logger(tmp_path / "cfg")
    stage = mod.S(deps, logger)
    assert stage.run({}) is None, "abstract base run body is pass → None"
