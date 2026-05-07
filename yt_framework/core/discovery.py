"""Filesystem scan that imports `stages/*/stage.py` and collects `BaseStage` types."""

import importlib
import logging
import sys
from pathlib import Path

from yt_framework.core.stage import BaseStage


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

        try:
            # Add pipeline_dir to sys.path temporarily if needed
            if str(pipeline_dir) not in sys.path:
                sys.path.insert(0, str(pipeline_dir))

            # Import the module
            module = importlib.import_module(module_name)

            # Find all BaseStage subclasses in the module
            for attr_name in dir(module):
                attr = getattr(module, attr_name)

                # Check if it's a class, inherits from BaseStage, and isn't BaseStage itself
                if (
                    isinstance(attr, type)
                    and issubclass(attr, BaseStage)
                    and attr is not BaseStage
                ):
                    discovered_stages.append(attr)
                    logger.debug(
                        "Discovered stage: %s -> %s", stage_name, attr.__name__
                    )
                    break  # Only take first BaseStage subclass per module

        except Exception as e:
            logger.warning("Failed to import stage from %s: %s", stage_file, e)
            continue

    if discovered_stages:
        stage_names = [sc.__name__ for sc in discovered_stages]
        logger.info(
            "[Discovery] Found %s stage%s: %s",
            len(discovered_stages),
            "s" if len(discovered_stages) != 1 else "",
            ", ".join(stage_names),
        )
    return discovered_stages
