"""Filesystem scan that imports `stages/*/stage.py` and collects `BaseStage` types."""

import importlib
import logging
import sys
from pathlib import Path

from yt_framework.core.stage import BaseStage


def _purge_stages_modules_from_sys() -> None:
    """Drop cached ``stages`` package tree so imports resolve the current ``pipeline_dir``."""
    for key in list(sys.modules):
        if key == "stages" or key.startswith("stages."):
            sys.modules.pop(key, None)


def _ensure_pipeline_on_sys_path(pipeline_dir: Path) -> None:
    if str(pipeline_dir) not in sys.path:
        sys.path.insert(0, str(pipeline_dir))


def _first_stage_subclass(
    module: object,
    stage_name: str,
    logger: logging.Logger,
) -> type[BaseStage] | None:
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if not isinstance(attr, type):
            continue
        if not issubclass(attr, BaseStage) or attr is BaseStage:
            continue
        logger.debug("Discovered stage: %s -> %s", stage_name, attr.__name__)
        return attr
    return None


def _import_stage_module(
    module_name: str,
    stage_file: Path,
    stage_name: str,
    logger: logging.Logger,
) -> type[BaseStage] | None:
    try:
        module = importlib.import_module(module_name)
    except (
        AttributeError,
        ImportError,
        ModuleNotFoundError,
        OSError,
        SyntaxError,
    ) as e:
        logger.warning("Failed to import stage from %s: %s", stage_file, e)
        return None
    return _first_stage_subclass(module, stage_name, logger)


def discover_stages(
    pipeline_dir: Path,
    logger: logging.Logger,
) -> list[type[BaseStage]]:
    """Automatically discover all stage classes from the ``stages`` directory tree.

    Searches for ``stage.py`` under each ``stages`` child directory and imports
    all ``BaseStage`` subclasses found.

    Expected layout: ``pipeline_dir/stages/<stage_name>/stage.py`` with a
    ``BaseStage`` subclass in each module.

    Args:
        pipeline_dir: Path to pipeline directory
        logger: Logger instance

    Returns:
        List of discovered stage classes

    """
    stages_dir = pipeline_dir / "stages"

    if not stages_dir.exists():
        logger.warning("Stages directory not found: %s", stages_dir)
        return []

    discovered_stages: list[type[BaseStage]] = []

    _purge_stages_modules_from_sys()

    # Iterate through each subdirectory in stages/
    for stage_dir in sorted(stages_dir.iterdir()):  # Sort for consistent order
        if not stage_dir.is_dir():
            continue

        stage_file = stage_dir / "stage.py"
        if not stage_file.exists():
            logger.debug("Skipping %s: no stage.py file", stage_dir.name)
            continue

        # Import the stage module dynamically
        stage_name = stage_dir.name
        module_name = f"stages.{stage_name}.stage"

        _ensure_pipeline_on_sys_path(pipeline_dir)
        found = _import_stage_module(module_name, stage_file, stage_name, logger)
        if found is not None:
            discovered_stages.append(found)

    if discovered_stages:
        stage_names = [sc.__name__ for sc in discovered_stages]
        logger.info(
            "[Discovery] Found %s stage%s: %s",
            len(discovered_stages),
            "s" if len(discovered_stages) != 1 else "",
            ", ".join(stage_names),
        )
    return discovered_stages
