"""Shared type alias for the stage-to-stage context dict (breaks import cycles)."""

from typing import Any, TypeAlias

DebugContext: TypeAlias = dict[str, Any]
