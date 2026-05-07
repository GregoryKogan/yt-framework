"""Core framework classes."""

from .pipeline import BasePipeline
from .registry import StageRegistry
from .stage import BaseStage

__all__ = ["BasePipeline", "BaseStage", "StageRegistry"]
