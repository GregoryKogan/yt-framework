"""Concrete ``PipelineStageDependencies`` for stage ``run()`` injection.

The :class:`~yt_framework.contracts.StageDependencies` protocol and
:class:`~yt_framework.contracts.StageContext` live under ``yt_framework.contracts``
so ``core`` and operation drivers share types without ``operations`` importing
``core``.
"""

from dataclasses import dataclass
from pathlib import Path

from omegaconf import DictConfig

from yt_framework.yt.clients.client_base import BaseYTClient


@dataclass
class PipelineStageDependencies:
    """Default implementation of StageDependencies.

    Used by BasePipeline to inject dependencies into stages.
    This class is instantiated by the pipeline and passed to each stage.

    Attributes:
        yt_client: YT client instance for performing operations on YTsaurus cluster
            or local filesystem (dev mode).
        pipeline_config: Pipeline-level configuration containing mode, build_folder,
            and other pipeline-wide settings.
        configs_dir: Path to directory containing secrets.env and configuration files.

    """

    yt_client: BaseYTClient
    pipeline_config: DictConfig
    configs_dir: Path
