"""Core framework classes."""

from .pipeline import BasePipeline
from .stage import BaseStage
from .registry import StageRegistry

__all__ = ["BasePipeline", "BaseStage", "StageRegistry"]
