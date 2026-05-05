"""`BaseStage` — pipeline authors subclass this and implement `run()`.

The framework derives the stage name from the `stages/<name>/` directory.
"""

from abc import ABC, abstractmethod
from typing import cast, Optional, TYPE_CHECKING, Dict, Any
from dataclasses import dataclass, replace as _dataclass_replace
from pathlib import Path
import logging
import inspect

from omegaconf import OmegaConf, DictConfig
from yt_framework.core.dependencies import StageDependencies

if TYPE_CHECKING:
    from yt_framework.core.pipeline import DebugContext
else:
    # Avoid circular import at runtime - DebugContext is Dict[str, Any]
    DebugContext = Dict[str, Any]


@dataclass
class StageContext:
    """Stage context containing all stage-related information.

    Attributes:
        name: Stage name (automatically detected from directory name).
        config: Stage-specific configuration loaded from config.yaml.
        stage_dir: Path to the stage directory (stages/<stage_name>/).
        logger: Logger instance for stage logging.
        deps: Injected dependencies (yt_client, pipeline_config, configs_dir).
    """

    name: str
    config: DictConfig
    stage_dir: Path
    logger: logging.Logger
    deps: StageDependencies

    def fork(
        self,
        name: Optional[str] = None,
        stage_dir: Optional[Path] = None,
    ) -> "StageContext":
        """Return a shallow copy with selective overrides.

        Use this in multi-operation stages when a later operation needs a
        slightly different context (e.g., a different ``stage_dir`` so that
        :class:`~yt_framework.operations.dependency_strategy.TarArchiveDependencyBuilder`
        resolves wrapper scripts from the correct location).

        Only ``name`` and ``stage_dir`` can be overridden; all other fields
        (``config``, ``logger``, ``deps``) are inherited from the parent
        context, which is intentional — they represent shared pipeline state.

        Args:
            name: Override for the stage name.  Defaults to the current name.
            stage_dir: Override for the stage directory.  Defaults to the
                current ``stage_dir``.

        Returns:
            A new :class:`StageContext` with the specified fields replaced.

        Example::

            ctx_reduce = self.context.fork(name="mds", stage_dir=Path(__file__).parent)
            run_reduce(context=ctx_reduce, operation_config=reduce_cfg, reducer=MyReducer())
        """
        return _dataclass_replace(
            self,
            name=name if name is not None else self.name,
            stage_dir=stage_dir if stage_dir is not None else self.stage_dir,
        )


class BaseStage(ABC):
    """
    Abstract base class for pipeline stages.

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
        """
        Initialize stage with injected dependencies.

        Stage name and config are automatically detected from the directory containing stage.py.

        Args:
            deps: Injected dependencies (yt_client, pipeline_config, configs_dir)
            logger: Logger instance for stage logging

        Returns:
            None

        Raises:
            FileNotFoundError: If config.yaml file is not found in stage directory.
            ValueError: If config.yaml does not contain a dictionary.
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
            raise FileNotFoundError(f"Config file not found: {config_path}")
        loaded_config = OmegaConf.load(config_path)
        # Ensure it's a DictConfig (not ListConfig)
        if not isinstance(loaded_config, DictConfig):
            raise ValueError(
                f"Stage config file must contain a dictionary, got {type(loaded_config).__name__}"
            )
        self.config = cast(DictConfig, loaded_config)

    @property
    def stage_dir(self) -> Path:
        """Path to the stage directory.

        Returns:
            Path: Absolute path to the stage directory (stages/<stage_name>/).
        """
        return Path(inspect.getfile(self.__class__)).parent

    @abstractmethod
    def run(self, debug: DebugContext) -> DebugContext:
        """
        Execute the stage.

        Args:
            debug: Shared context dictionary from previous stages.
                   Contains results from earlier stages. Can be empty dict
                   for the first stage.

        Returns:
            DebugContext: Dictionary with stage results to be merged into context
                         and passed to the next stage.
        """
        pass

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
