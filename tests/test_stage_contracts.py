"""Ensure backward-compatible re-exports stay importable."""

from yt_framework.operations import stage_contracts as sc


def test_stage_contracts_reexports_stage_context_and_dependencies() -> None:
    assert sc.StageContext is not None and sc.StageDependencies is not None
