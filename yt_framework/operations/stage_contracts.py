"""Stage execution contracts shared by operation drivers and ``core`` orchestration.

``StageDependencies`` and :class:`StageContext` live here so
``yt_framework.operations`` does not depend on ``yt_framework.core`` for types
(Tach layer: operations is inner to core).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import replace as _dataclass_replace
from pathlib import Path
from typing import Protocol

from omegaconf import DictConfig

from yt_framework.yt.clients.client_base import BaseYTClient


class StageDependencies(Protocol):
    """Protocol defining what dependencies stages need.

    This is NOT the same as the 'context' parameter in run() methods:
    - StageDependencies: Injected services/config (yt_client, config, etc.)
    - context parameter: Shared data dictionary passed between stages

    Benefits:
    - Dependency Inversion: Depends on abstraction, not concrete Pipeline
    - Interface Segregation: Only exposes what stages actually use
    - Testability: Easy to create mock dependencies for testing
    """

    @property
    def yt_client(self) -> BaseYTClient:
        """YT client for operations.

        Returns:
            BaseYTClient: YT client instance (either YTDevClient or YTProdClient)
                for performing table operations, running map/vanilla jobs, etc.

        """
        ...

    @property
    def pipeline_config(self) -> DictConfig:
        """Pipeline-level configuration (contains build_folder and secrets).

        Returns:
            DictConfig: OmegaConf configuration object containing pipeline-wide
                settings like mode, build_folder, and other pipeline parameters.

        """
        ...

    @property
    def configs_dir(self) -> Path:
        """Directory containing secrets.env and other config files.

        Returns:
            Path: Absolute path to the configs directory where secrets.env
                and other configuration files are stored.

        """
        ...


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
        name: str | None = None,
        stage_dir: Path | None = None,
    ) -> StageContext:
        """Return a shallow copy with selective overrides.

        Use this in multi-operation stages when a later operation needs a
        slightly different context (e.g., a different ``stage_dir`` so that
        :class:`~yt_framework.operations._internal.dependency_strategy.TarArchiveDependencyBuilder`
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
