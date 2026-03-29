"""Tests for yt_framework.core.registry.StageRegistry."""

import pytest

from yt_framework.core.registry import StageRegistry
from yt_framework.core.stage import BaseStage


class _DummyStage(BaseStage):
    def run(self, debug):
        return debug


class _OtherDummyStage(BaseStage):
    def run(self, debug):
        return debug


def test_stage_registry_add_stage_returns_self_for_chaining() -> None:
    reg = StageRegistry()
    assert reg.add_stage(_DummyStage) is reg


def test_stage_registry_resolves_stage_by_containing_directory_name() -> None:
    reg = StageRegistry()
    reg.add_stage(_DummyStage)
    assert reg.get_stage("tests") is _DummyStage


def test_stage_registry_last_add_wins_when_classes_share_same_stage_directory() -> None:
    reg = StageRegistry()
    reg.add_stage(_DummyStage)
    reg.add_stage(_OtherDummyStage)
    assert reg.get_stage("tests") is _OtherDummyStage


def test_stage_registry_has_stage_reflects_registration() -> None:
    reg = StageRegistry()
    assert not reg.has_stage("tests")
    reg.add_stage(_DummyStage)
    assert reg.has_stage("tests")


def test_stage_registry_get_stage_raises_key_error_when_name_missing() -> None:
    reg = StageRegistry()
    with pytest.raises(KeyError, match="nosuch"):
        reg.get_stage("nosuch")


def test_stage_registry_get_all_stages_copy_is_independent_of_internal_map() -> None:
    reg = StageRegistry()
    reg.add_stage(_DummyStage)
    snapshot = reg.get_all_stages()
    snapshot.clear()
    assert reg.has_stage("tests")
