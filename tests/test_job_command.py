"""Tests for job_command utilities: resolve_aliased_job and require_consistent_map_reduce_legs."""

import builtins

import pytest
from yt.wrapper import TypedJob  # pyright: ignore[reportMissingImports]

from yt_framework.operations.job_command import (
    is_typed_job,
    map_reduce_leg_kind,
    require_consistent_map_reduce_legs,
    resolve_aliased_job,
)


class _MinimalTypedJob(TypedJob):
    def prepare_operation(self, *args, **kwargs) -> None:  # type: ignore[override]
        pass


# ---------------------------------------------------------------------------
# resolve_aliased_job
# ---------------------------------------------------------------------------


def test_resolve_aliased_job_legacy_only() -> None:
    result = resolve_aliased_job(
        legacy_name="command",
        legacy_value="cmd",
        preferred_name="job",
        preferred_value=None,
    )
    assert result == "cmd"


def test_resolve_aliased_job_preferred_only() -> None:
    result = resolve_aliased_job(
        legacy_name="command",
        legacy_value=None,
        preferred_name="job",
        preferred_value="preferred_cmd",
    )
    assert result == "preferred_cmd"


def test_resolve_aliased_job_both_same_value() -> None:
    result = resolve_aliased_job(
        legacy_name="command",
        legacy_value="cmd",
        preferred_name="job",
        preferred_value="cmd",
    )
    assert result == "cmd"


def test_resolve_aliased_job_both_different_raises() -> None:
    with pytest.raises(ValueError, match="different values"):
        resolve_aliased_job(
            legacy_name="mapper",
            legacy_value="cmd_a",
            preferred_name="map_job",
            preferred_value="cmd_b",
        )


def test_resolve_aliased_job_both_none() -> None:
    result = resolve_aliased_job(
        legacy_name="command",
        legacy_value=None,
        preferred_name="job",
        preferred_value=None,
    )
    assert result is None


def test_resolve_aliased_job_preferred_wins_over_none_legacy() -> None:
    result = resolve_aliased_job(
        legacy_name="reducer",
        legacy_value=None,
        preferred_name="job",
        preferred_value="my_reducer",
    )
    assert result == "my_reducer"


# ---------------------------------------------------------------------------
# map_reduce_leg_kind
# ---------------------------------------------------------------------------


def test_map_reduce_leg_kind_string() -> None:
    assert map_reduce_leg_kind("my command") == "command"


def test_map_reduce_leg_kind_invalid_raises() -> None:
    with pytest.raises(TypeError, match="command string"):
        map_reduce_leg_kind(42)


# ---------------------------------------------------------------------------
# require_consistent_map_reduce_legs
# ---------------------------------------------------------------------------


def test_require_consistent_both_strings() -> None:
    require_consistent_map_reduce_legs("mapper_cmd", "reducer_cmd")


def test_require_consistent_invalid_type_raises() -> None:
    with pytest.raises(TypeError, match="command string"):
        # int is neither TypedJob nor str — map_reduce_leg_kind raises TypeError
        require_consistent_map_reduce_legs("mapper_cmd", 42)


# ---------------------------------------------------------------------------
# is_typed_job
# ---------------------------------------------------------------------------


def test_is_typed_job_string_is_false() -> None:
    assert not is_typed_job("some command")


def test_is_typed_job_none_is_false() -> None:
    assert not is_typed_job(None)


def test_is_typed_job_int_is_false() -> None:
    assert not is_typed_job(42)


def test_is_typed_job_returns_false_when_yt_wrapper_import_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "yt.wrapper":
            msg = "simulated missing yt.wrapper"
            raise ImportError(msg)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert not is_typed_job(_MinimalTypedJob())


def test_is_typed_job_subclass_instance_is_true() -> None:
    assert is_typed_job(_MinimalTypedJob())


def test_map_reduce_leg_kind_typed_job_returns_typed() -> None:
    assert map_reduce_leg_kind(_MinimalTypedJob()) == "typed"


def test_require_consistent_both_typed_jobs() -> None:
    require_consistent_map_reduce_legs(_MinimalTypedJob(), _MinimalTypedJob())


def test_require_consistent_typed_mapper_and_string_reducer_raises() -> None:
    with pytest.raises(ValueError, match="same job kind"):
        require_consistent_map_reduce_legs(_MinimalTypedJob(), "reducer_cmd")
