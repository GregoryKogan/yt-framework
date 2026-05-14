"""Stage execution contracts shared by operation drivers and ``core`` orchestration.

:class:`StageDependencies` and :class:`StageContext` live in ``yt_framework.contracts``
so ``yt_framework.core`` can depend on this package instead of reaching through
``yt_framework.operations`` only for types, while operation drivers stay free of
``core`` imports (see ``tach.toml`` and ``docs/architecture/layers.md``).
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

    This is NOT the same as the ``context`` parameter in ``run()`` methods:
    - ``StageDependencies``: injected services and config (``yt_client``, etc.)
    - ``context`` parameter: shared data dict passed between stages

    """

    @property
    def yt_client(self) -> BaseYTClient:
        """YT client for operations (dev or prod implementation)."""
        ...

    @property
    def pipeline_config(self) -> DictConfig:
        """Pipeline-level configuration (``build_folder``, mode, secrets paths)."""
        ...

    @property
    def configs_dir(self) -> Path:
        """Directory containing ``secrets.env`` and other config files."""
        ...


@dataclass
class StageContext:
    """Stage context: name, config, paths, logger, and injected dependencies."""

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
        """Return a shallow copy with selective ``name`` / ``stage_dir`` overrides."""
        return _dataclass_replace(
            self,
            name=name if name is not None else self.name,
            stage_dir=stage_dir if stage_dir is not None else self.stage_dir,
        )
