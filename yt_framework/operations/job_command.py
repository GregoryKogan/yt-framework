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


def resolve_aliased_job(
    *,
    legacy_name: str,
    legacy_value: Any,
    preferred_name: str,
    preferred_value: Any,
) -> Any:
    """Resolve legacy/preferred aliased job arguments with compatibility checks.

    Raises ``ValueError`` when both are set to different values, so callers
    can support both names while preventing silently-conflicting configs.
    """
    if (
        legacy_value is not None
        and preferred_value is not None
        and legacy_value != preferred_value
    ):
        raise ValueError(
            f"Both '{legacy_name}' and '{preferred_name}' are set with different values; "
            "please provide only one"
        )
    if preferred_value is not None:
        return preferred_value
    return legacy_value


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
