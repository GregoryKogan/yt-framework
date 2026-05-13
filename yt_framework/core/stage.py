"""`BaseStage` — pipeline authors subclass this and implement `run()`.

The framework derives the stage name from the `stages/<name>/` directory.
"""

import inspect
import logging
from abc import ABC, abstractmethod
from pathlib import Path

from omegaconf import DictConfig, OmegaConf

from yt_framework.core.debug_context import DebugContext
from yt_framework.operations.stage_contracts import StageContext, StageDependencies


class BaseStage(ABC):
    """Abstract base class for pipeline stages.

    Stage name and config are automatically detected from the directory.
    Directory structure: stages/<stage_name>/stage.py
    Config file: stages/<stage_name>/config.yaml

    Each stage must:
    - Implement `__init__` to receive deps and logger
    - Implement `run` method to execute stage logic

    Example:
        class MyStage(BaseStage):
            def __init__(self, deps, logger):
                super().__init__(deps, logger)
                # self.config is automatically loaded from stages/<stage_name>/config.yaml
                # Access dependencies via self.deps.yt_client, self.deps.pipeline_config

            def run(self, debug: DebugContext) -> DebugContext:
                # Stage logic here
                # Note: 'debug' here is the shared data dict, NOT dependencies
                return {"result": "value"}

    """

    def __init__(
        self,
        deps: StageDependencies,
        logger: logging.Logger,
    ) -> None:
        """Initialize stage with injected dependencies.

        Stage name and config are automatically detected from the directory containing stage.py.

        Args:
            deps: Injected dependencies (yt_client, pipeline_config, configs_dir)
            logger: Logger instance for stage logging

        Returns:
            None

        Raises:
            FileNotFoundError: If config.yaml file is not found in stage directory.
            TypeError: If config.yaml does not contain a dictionary (wrong node type).

        """
        self.deps = deps
        self.logger = logger

        # Auto-detect stage name and load config from directory
        # Get the file where the actual stage class is defined (not base_stage.py)
        stage_file = Path(inspect.getfile(self.__class__))
        stage_dir = stage_file.parent
        # stage_file is stage.py, parent is the stage directory (e.g., create_table)
        self.name: str = stage_dir.name

        # Automatically load stage-specific config
        config_path = stage_dir / "config.yaml"
        if not config_path.exists():
            msg = f"Config file not found: {config_path}"
            raise FileNotFoundError(msg)
        loaded: object = OmegaConf.load(config_path)
        if not isinstance(loaded, DictConfig):
            msg = f"Stage config file must contain a dictionary, got {type(loaded).__name__}"
            raise TypeError(msg)
        self.config = loaded

    @property
    def stage_dir(self) -> Path:
        """Path to the stage directory.

        Returns:
            Path: Absolute path to the stage directory (stages/<stage_name>/).

        """
        return Path(inspect.getfile(self.__class__)).parent

    @abstractmethod
    def run(self, debug: DebugContext) -> DebugContext:
        """Execute the stage.

        Args:
            debug: Shared context dictionary from previous stages.
                   Contains results from earlier stages. Can be empty dict
                   for the first stage.

        Returns:
            DebugContext: Dictionary with stage results to be merged into context
                         and passed to the next stage.

        """

    @property
    def context(self) -> StageContext:
        """Stage context containing all stage-related information.

        Returns:
            StageContext: Dataclass instance with name, config, stage_dir,
                         logger, and deps attributes.

        """
        return StageContext(
            name=self.name,
            config=self.config,
            stage_dir=self.stage_dir,
            logger=self.logger,
            deps=self.deps,
        )
