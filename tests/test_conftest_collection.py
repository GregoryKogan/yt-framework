"""Unit tests for suite-level pytest collection guard."""

from __future__ import annotations

import conftest as suite_conftest
from integration.yt_cluster import _cluster_config


def test_pytest_ignore_collect_skips_yt_cluster_when_credentials_absent(
    monkeypatch,
) -> None:
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("YT_PROXY", raising=False)
    monkeypatch.delenv("YT_TOKEN", raising=False)
    monkeypatch.setattr(_cluster_config, "is_cluster_config_ready", lambda: False)

    collection_path = suite_conftest._TESTS_DIR / "integration/yt_cluster/test_jobs.py"
    assert suite_conftest.pytest_ignore_collect(collection_path=collection_path) is True


def test_pytest_ignore_collect_allows_yt_cluster_when_ready(monkeypatch) -> None:
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setattr(_cluster_config, "is_cluster_config_ready", lambda: True)

    collection_path = suite_conftest._TESTS_DIR / "integration/yt_cluster/test_jobs.py"
    assert suite_conftest.pytest_ignore_collect(collection_path=collection_path) is None
