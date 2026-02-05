"""
Stage Dependencies
==================

Dependency injection container for stages.

This module provides a clean separation between stages and the pipeline,
following SOLID principles (especially Dependency Inversion and Interface Segregation).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from omegaconf import DictConfig
from yt_framework.yt.client_base import BaseYTClient


class StageDependencies(Protocol):
    """
    Protocol defining what dependencies stages need.

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
class PipelineStageDependencies:
    """
    Default implementation of StageDependencies.

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
