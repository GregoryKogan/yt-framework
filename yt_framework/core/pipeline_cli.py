"""CLI bootstrap for :class:`~yt_framework.core.pipeline.BasePipeline` subclasses."""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from pathlib import Path
from typing import Any

from omegaconf import DictConfig, OmegaConf

from yt_framework.utils.logging import log_success


def build_pipeline_cli_parser(cls_name: str) -> argparse.ArgumentParser:
    """Build the ``argparse`` parser used by :meth:`~yt_framework.core.pipeline.BasePipeline.main`."""
    parser = argparse.ArgumentParser(description=f"Run {cls_name}")
    parser.add_argument(
        "--config",
        type=str,
        default="configs/config.yaml",
        help="Path to config file (default: configs/config.yaml)",
    )
    return parser


def resolve_pipeline_config_path(pipeline_dir: Path, config_arg: str) -> Path:
    """Resolve ``--config`` to an absolute path (relative paths are under ``pipeline_dir``)."""
    config_path = Path(config_arg)
    if not config_path.is_absolute():
        config_path = pipeline_dir / config_path
    return config_path


def read_pipeline_mode_for_header(config_path: Path, logger: logging.Logger) -> str:
    """Return ``pipeline.mode`` from the config file for log banners; default ``dev`` on errors."""
    try:
        temp_config = OmegaConf.load(config_path)
    except Exception:
        logger.exception("Failed to determine mode")
        return "dev"
    if isinstance(temp_config, DictConfig):
        return str(temp_config.pipeline.get("mode", "dev"))
    return "dev"


def load_dict_config_or_exit(
    config_path: Path,
    logger: logging.Logger,
) -> DictConfig:
    """Load YAML as a :class:`~omegaconf.DictConfig` or log and ``sys.exit(1)``."""
    try:
        loaded_cfg = OmegaConf.load(config_path)
    except Exception:
        logger.exception("Failed to load config")
        traceback.print_exc()
        sys.exit(1)
    if not isinstance(loaded_cfg, DictConfig):
        logger.error(
            "Config file must contain a dictionary, got %s",
            type(loaded_cfg).__name__,
        )
        sys.exit(1)
    return loaded_cfg


def run_pipeline_instance_or_exit(
    cls: type[Any],
    config: DictConfig,
    pipeline_dir: Path,
    logger: logging.Logger,
) -> None:
    """Instantiate ``cls``, run the pipeline, then ``sys.exit(0)`` or ``sys.exit(1)``."""
    try:
        pipeline = cls(config=config, pipeline_dir=pipeline_dir)
        pipeline.run()
        log_success(logger, "Pipeline completed successfully")
        sys.exit(0)
    except Exception:
        logger.exception("Pipeline failed")
        traceback.print_exc()
        sys.exit(1)
