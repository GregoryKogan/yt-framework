"""
TypedJob vs string command detection for YT operations.

Used by dependency building and map-reduce wiring so behavior follows job kind,
not only operation name.
"""

from __future__ import annotations

from typing import Any, Literal

LegKind = Literal["typed", "command"]


def is_typed_job(obj: Any) -> bool:
    """Return True if ``obj`` is a YTsaurus ``TypedJob`` instance."""
    try:
        from yt.wrapper import TypedJob  # pyright: ignore[reportMissingImports]
    except ImportError:
        return False
    return isinstance(obj, TypedJob)


def map_reduce_leg_kind(obj: Any) -> LegKind:
    """Classify a map-reduce leg as ``TypedJob`` or command ``str``."""
    if is_typed_job(obj):
        return "typed"
    if isinstance(obj, str):
        return "command"
    raise TypeError(
        "map-reduce mapper and reducer must each be a yt.TypedJob instance or a "
        f"command string, got {type(obj).__name__}"
    )


def require_consistent_map_reduce_legs(mapper: Any, reducer: Any) -> None:
    """
    Mapper and reducer must both use the same wire protocol: both TypedJob or both str.

    Mixing TypedJob on one leg and a shell/Python command string on the other is
    unsupported.
    """
    km = map_reduce_leg_kind(mapper)
    kr = map_reduce_leg_kind(reducer)
    if km != kr:
        raise ValueError(
            "map-reduce mapper and reducer must use the same job kind: both TypedJob "
            f"instances or both command strings, not mixed. Got mapper={km}, reducer={kr}."
        )
