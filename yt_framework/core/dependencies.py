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
        """YT client for operations."""
        ...
    
    @property
    def pipeline_config(self) -> DictConfig:
        """Pipeline-level configuration (contains build_folder and secrets)."""
        ...
    
    @property
    def configs_dir(self) -> Path:
        """Directory containing secrets.env and other config files."""
        ...


@dataclass
class PipelineStageDependencies:
    """
    Default implementation of StageDependencies.
    
    Used by BasePipeline to inject dependencies into stages.
    This class is instantiated by the pipeline and passed to each stage.
    """
    yt_client: BaseYTClient
    pipeline_config: DictConfig
    configs_dir: Path
