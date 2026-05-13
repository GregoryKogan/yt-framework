"""Shared helpers for partitioning env and wrapping map/vanilla command legs."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Collection

from yt_framework.yt.support.operation_secure_env import (
    partition_env_for_yt_spec,
    wrap_shell_cmd_vault_promote,
)


def _public_env_keys_for_partition(raw: object) -> Collection[str] | None:
    if raw is None:
        return None
    if isinstance(raw, (list, tuple, set, frozenset)):
        return [str(x) for x in raw]
    return [str(raw)]


def maybe_wrap_cmd_for_vault(leg: object, secure_flat: dict[str, str]) -> object:
    if secure_flat and isinstance(leg, str):
        return wrap_shell_cmd_vault_promote(leg)
    return leg


def _partition_and_maybe_wrap_leg(
    leg: object,
    env: dict[str, str],
    *,
    environment_public_keys: object,
    use_plain_environment_for_secrets: bool,
) -> tuple[dict[str, str], dict[str, str], Any]:
    if use_plain_environment_for_secrets:
        public_env, secure_flat = dict(env), {}
    else:
        public_env, secure_flat = partition_env_for_yt_spec(
            env,
            _public_env_keys_for_partition(environment_public_keys),
        )
    leg = maybe_wrap_cmd_for_vault(leg, secure_flat)
    return public_env, secure_flat, leg
