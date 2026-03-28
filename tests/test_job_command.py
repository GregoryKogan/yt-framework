"""Tests for job_command utilities: resolve_aliased_job and require_consistent_map_reduce_legs."""

import pytest
from yt.wrapper import TypedJob  # pyright: ignore[reportMissingImports]

from yt_framework.operations.job_command import (
    resolve_aliased_job,
    require_consistent_map_reduce_legs,
    map_reduce_leg_kind,
    is_typed_job,
)


class _MinimalTypedJob(TypedJob):
    def prepare_operation(self, *args, **kwargs):  # type: ignore[override]
        pass


# ---------------------------------------------------------------------------
# resolve_aliased_job
# ---------------------------------------------------------------------------


def test_resolve_aliased_job_legacy_only():
    result = resolve_aliased_job(
        legacy_name="command",
        legacy_value="cmd",
        preferred_name="job",
        preferred_value=None,
    )
    assert result == "cmd"


def test_resolve_aliased_job_preferred_only():
    result = resolve_aliased_job(
        legacy_name="command",
        legacy_value=None,
        preferred_name="job",
        preferred_value="preferred_cmd",
    )
    assert result == "preferred_cmd"


def test_resolve_aliased_job_both_same_value():
    result = resolve_aliased_job(
        legacy_name="command",
        legacy_value="cmd",
        preferred_name="job",
        preferred_value="cmd",
    )
    assert result == "cmd"


def test_resolve_aliased_job_both_different_raises():
    with pytest.raises(ValueError, match="different values"):
        resolve_aliased_job(
            legacy_name="mapper",
            legacy_value="cmd_a",
            preferred_name="map_job",
            preferred_value="cmd_b",
        )


def test_resolve_aliased_job_both_none():
    result = resolve_aliased_job(
        legacy_name="command",
        legacy_value=None,
        preferred_name="job",
        preferred_value=None,
    )
    assert result is None


def test_resolve_aliased_job_preferred_wins_over_none_legacy():
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


def test_map_reduce_leg_kind_string():
    assert map_reduce_leg_kind("my command") == "command"


def test_map_reduce_leg_kind_invalid_raises():
    with pytest.raises(TypeError, match="command string"):
        map_reduce_leg_kind(42)


# ---------------------------------------------------------------------------
# require_consistent_map_reduce_legs
# ---------------------------------------------------------------------------


def test_require_consistent_both_strings():
    require_consistent_map_reduce_legs("mapper_cmd", "reducer_cmd")


def test_require_consistent_invalid_type_raises():
    with pytest.raises(TypeError, match="command string"):
        # int is neither TypedJob nor str — map_reduce_leg_kind raises TypeError
        require_consistent_map_reduce_legs("mapper_cmd", 42)


# ---------------------------------------------------------------------------
# is_typed_job
# ---------------------------------------------------------------------------


def test_is_typed_job_string_is_false():
    assert not is_typed_job("some command")


def test_is_typed_job_none_is_false():
    assert not is_typed_job(None)


def test_is_typed_job_int_is_false():
    assert not is_typed_job(42)


def test_is_typed_job_subclass_instance_is_true():
    assert is_typed_job(_MinimalTypedJob())


def test_map_reduce_leg_kind_typed_job_returns_typed():
    assert map_reduce_leg_kind(_MinimalTypedJob()) == "typed"


def test_require_consistent_both_typed_jobs():
    require_consistent_map_reduce_legs(_MinimalTypedJob(), _MinimalTypedJob())


def test_require_consistent_typed_mapper_and_string_reducer_raises():
    with pytest.raises(ValueError, match="same job kind"):
        require_consistent_map_reduce_legs(_MinimalTypedJob(), "reducer_cmd")
