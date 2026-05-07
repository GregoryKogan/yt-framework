"""Unit tests for ``yt_framework.yt.operation_secure_env``."""

from __future__ import annotations

import os
import shlex

from yt_framework.yt.operation_secure_env import (
    DEFAULT_PUBLIC_ENV_KEYS,
    merge_secure_vault,
    partition_env_for_yt_spec,
    promote_secure_vault_environment,
    wrap_shell_command_with_secure_vault_promotion,
)


def test_partition_env_sends_secrets_to_vault_and_keeps_allowlist_public() -> None:
    env = {
        "YT_TOKEN": "secret",
        "YT_STAGE_NAME": "s1",
        "CUSTOM": "x",
    }
    public, secure = partition_env_for_yt_spec(env)
    assert public == {"YT_STAGE_NAME": "s1"}
    assert secure == {"YT_TOKEN": "secret", "CUSTOM": "x"}


def test_partition_env_extra_public_keys() -> None:
    public, secure = partition_env_for_yt_spec(
        {"A": "1", "B": "2"},
        extra_public_keys=["B"],
    )
    assert public == {"B": "2"}
    assert secure == {"A": "1"}


def test_promote_secure_vault_environment_uses_setdefault() -> None:
    os.environ["YT_SECURE_VAULT_FOO"] = "from_vault"
    os.environ["FOO"] = "already"
    try:
        promote_secure_vault_environment()
        assert os.environ["FOO"] == "already"
    finally:
        del os.environ["YT_SECURE_VAULT_FOO"]
        del os.environ["FOO"]


def test_promote_secure_vault_environment_fills_missing() -> None:
    os.environ["YT_SECURE_VAULT_BAR"] = "vb"
    try:
        os.environ.pop("BAR", None)
        promote_secure_vault_environment()
        assert os.environ["BAR"] == "vb"
    finally:
        del os.environ["YT_SECURE_VAULT_BAR"]
        del os.environ["BAR"]


def test_promote_skips_pickling_key() -> None:
    os.environ["YT_SECURE_VAULT__PICKLING_KEY"] = "pk"
    try:
        promote_secure_vault_environment()
        assert "_PICKLING_KEY" not in os.environ
    finally:
        del os.environ["YT_SECURE_VAULT__PICKLING_KEY"]


def test_wrap_command_roundtrip_shell_words() -> None:
    inner = "python3 mapper.py 'a b'"
    wrapped = wrap_shell_command_with_secure_vault_promotion(inner)
    assert wrapped.startswith("python3 -c ")
    assert wrapped.count(" bash -c ") == 1
    _, tail = wrapped.split(" bash -c ", 1)
    assert shlex.split(tail, posix=True) == [inner]


def test_merge_secure_vault_docker_and_user_override() -> None:
    out = merge_secure_vault(
        {"K": "v"},
        docker_image="img:latest",
        docker_auth={"username": "u", "password": "p"},
        user_secure_vault={"K": "user", "docker_auth": {"password": "p2"}},
    )
    assert out["K"] == "user"
    assert out["docker_auth"]["username"] == "u"
    assert out["docker_auth"]["password"] == "p2"


def test_default_public_keys_contains_expected() -> None:
    assert "YT_STAGE_NAME" in DEFAULT_PUBLIC_ENV_KEYS
