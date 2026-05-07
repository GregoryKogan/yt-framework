"""Mutable registry of `BaseStage` subclasses keyed by stage name."""

import inspect
from pathlib import Path

from yt_framework.core.stage import BaseStage


class StageRegistry:
    """Builder for registering pipeline stages.

    Automatically detects stage names from directory structure.

    Example:
        registry = StageRegistry()
        registry.add_stage(CreateTableStage)
        registry.add_stage(RunMapStage)
        pipeline.set_stage_registry(registry)

    """

    def __init__(self) -> None:
        """Initialize empty stage registry."""
        self._stages: dict[str, type[BaseStage]] = {}

    def add_stage(self, stage_class: type[BaseStage]) -> "StageRegistry":
        """Register a stage class.

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

    def get_stage(self, stage_name: str) -> type[BaseStage]:
        """Get stage class by name.

        Args:
            stage_name: Name of the stage to retrieve (matches directory name).

        Returns:
            Type[BaseStage]: The stage class registered with the given name.

        Raises:
            KeyError: If no stage is registered with the given name.

        Example:
            >>> registry = StageRegistry()
            >>> registry.add_stage(MyStage)
            >>> stage_class = registry.get_stage("my_stage")

        """
        return self._stages[stage_name]

    def has_stage(self, stage_name: str) -> bool:
        """Check if stage is registered.

        Args:
            stage_name: Name of the stage to check.

        Returns:
            bool: True if the stage is registered, False otherwise.

        """
        return stage_name in self._stages

    def get_all_stages(self) -> dict[str, type[BaseStage]]:
        """Get all registered stages.

        Returns:
            Dict[str, Type[BaseStage]]: Dictionary mapping stage names to stage classes.
                Returns a copy to prevent external modification.

        """
        return self._stages.copy()
