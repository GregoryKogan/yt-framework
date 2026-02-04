"""
Stage Registry
==============

Builder for registering pipeline stages with automatic name detection.
"""

from typing import Dict, Type
from pathlib import Path
import inspect

from yt_framework.core.stage import BaseStage


class StageRegistry:
    """
    Builder for registering pipeline stages.

    Automatically detects stage names from directory structure.

    Example:
        registry = StageRegistry()
        registry.add_stage(CreateTableStage)
        registry.add_stage(RunMapStage)
        pipeline.set_stage_registry(registry)
    """

    def __init__(self) -> None:
        """Initialize empty stage registry."""
        self._stages: Dict[str, Type[BaseStage]] = {}

    def add_stage(self, stage_class: Type[BaseStage]) -> "StageRegistry":
        """
        Register a stage class.

        Stage name is automatically detected from the directory containing stage.py.

        Args:
            stage_class: Stage class to register

        Returns:
            Self for method chaining
        """
        # Detect stage name from stage class file location
        stage_file = Path(inspect.getfile(stage_class))
        stage_name = stage_file.parent.name

        self._stages[stage_name] = stage_class
        return self

    def get_stage(self, stage_name: str) -> Type[BaseStage]:
        """Get stage class by name."""
        return self._stages[stage_name]

    def has_stage(self, stage_name: str) -> bool:
        """Check if stage is registered."""
        return stage_name in self._stages

    def get_all_stages(self) -> Dict[str, Type[BaseStage]]:
        """Get all registered stages."""
        return self._stages.copy()
