"""Cypress table I/O and prod client wiring (real cluster)."""

from __future__ import annotations

import pytest

from yt_framework.yt.factory import create_yt_client

from integration.yt_cluster._cluster_config import secrets_dict_for_yt_client


@pytest.mark.yt_cluster
def test_session_yt_root_exists(cluster_session, yt_client) -> None:
    assert yt_client.exists(
        cluster_session.yt_run_root
    ), "session Cypress root must exist"


@pytest.mark.yt_cluster
def test_write_read_row_count_and_append(yt_case_prefix: str, yt_client) -> None:
    t = f"{yt_case_prefix}/t_write"
    yt_client.write_table(t, [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    assert yt_client.row_count(t) == 2
    rows = yt_client.read_table(t)
    assert rows == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    yt_client.write_table(t, [{"a": 3, "b": "z"}], append=True)
    assert yt_client.row_count(t) == 3


@pytest.mark.yt_cluster
def test_create_path_and_exists(yt_case_prefix: str, yt_client) -> None:
    p = f"{yt_case_prefix}/nested/map_node"
    assert not yt_client.exists(p)
    yt_client.create_path(p, node_type="map_node")
    assert yt_client.exists(p)


@pytest.mark.yt_cluster
def test_factory_matches_session_client(cluster_session, yt_client) -> None:
    logger = yt_client.logger
    other = create_yt_client(
        logger=logger,
        mode="prod",
        secrets=secrets_dict_for_yt_client(cluster_session.secrets_full),
    )
    assert other.exists(cluster_session.yt_run_root)
