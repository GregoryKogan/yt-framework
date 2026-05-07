"""Session-scoped YT client and roots for real-cluster integration tests."""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from integration.yt_cluster._cluster_config import (
    configure_global_yt_for_checkpoint,
    load_cluster_test_secrets,
    new_session_run_id,
    operation_resources_from_env,
    secrets_dict_for_yt_client,
)
from yt_framework.yt.factory import create_yt_client

if TYPE_CHECKING:
    from collections.abc import Generator

    from yt_framework.yt.client_base import OperationResources
    from yt_framework.yt.client_prod import YTProdClient


def _null_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    log.setLevel(logging.INFO)
    return log


@dataclass(frozen=True)
class ClusterSessionContext:
    """Holds prod client and path roots for one pytest session."""

    client: YTProdClient
    yt_run_root: str
    local_run_root: Path
    resources: OperationResources
    secrets_full: dict[str, str]


@pytest.fixture(scope="session")
def cluster_session() -> Generator[ClusterSessionContext, None, None]:
    loaded = load_cluster_test_secrets()
    secrets = secrets_dict_for_yt_client(loaded)
    run_id = new_session_run_id()
    yt_run_root = f"//tmp/yt-framework/testing/{run_id}"
    local_run_root = Path(f"/tmp/yt-framework/testing/{run_id}")
    local_run_root.mkdir(parents=True, exist_ok=True)

    logger = _null_logger("tests.integration.yt_cluster.session")
    client = cast(
        "YTProdClient",
        create_yt_client(
            logger=logger,
            mode="prod",
            secrets=secrets,
        ),
    )
    resources = operation_resources_from_env(loaded)

    client.create_path(yt_run_root, node_type="map_node")
    configure_global_yt_for_checkpoint(secrets["YT_PROXY"], secrets["YT_TOKEN"])

    ctx = ClusterSessionContext(
        client=client,
        yt_run_root=yt_run_root,
        local_run_root=local_run_root,
        resources=resources,
        secrets_full=loaded,
    )
    try:
        yield ctx
    finally:
        try:
            client.client.remove(yt_run_root, force=True, recursive=True)
        except Exception:
            logger.exception("Failed to remove YT test root %s", yt_run_root)
        if local_run_root.is_dir():
            shutil.rmtree(local_run_root, ignore_errors=True)


@pytest.fixture
def yt_case_prefix(
    cluster_session: ClusterSessionContext, request: pytest.FixtureRequest
) -> str:
    """Unique YT path prefix per test function under the session root."""
    safe = request.node.name.replace("[", "_").replace("]", "_")
    return f"{cluster_session.yt_run_root}/{safe}"


@pytest.fixture
def yt_client(cluster_session: ClusterSessionContext) -> YTProdClient:
    return cluster_session.client


@pytest.fixture
def op_resources(cluster_session: ClusterSessionContext) -> OperationResources:
    return cluster_session.resources


@pytest.fixture
def local_case_dir(
    cluster_session: ClusterSessionContext, request: pytest.FixtureRequest
) -> Path:
    """Host scratch directory per test; removed after the test."""
    safe = request.node.name.replace("[", "_").replace("]", "_")
    p = cluster_session.local_run_root / safe
    p.mkdir(parents=True, exist_ok=True)
    yield p
    if p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
