"""Unit tests for cluster test fixture cleanup behavior."""

from __future__ import annotations

from types import SimpleNamespace

from integration.yt_cluster import conftest as cluster_conftest


def test_cleanup_cluster_clients_closes_client_and_global_sessions(monkeypatch) -> None:
    calls: list[str] = []

    raw_client = SimpleNamespace(_cleanup=lambda: calls.append("client"))
    client = SimpleNamespace(client=raw_client)

    import yt.wrapper as yt  # pyright: ignore[reportMissingImports]

    monkeypatch.setattr(yt.config, "_cleanup", lambda: calls.append("global"))

    cluster_conftest.cleanup_cluster_clients(client)

    assert calls == ["client", "global"]
