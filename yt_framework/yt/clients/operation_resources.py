"""Re-export operation resource types from ``yt_framework.yt.support``."""

from yt_framework.yt.support.operation_resources import (
    OperationResources,
    validate_cpu_limit,
)

__all__ = ["OperationResources", "validate_cpu_limit"]
