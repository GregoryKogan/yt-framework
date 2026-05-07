"""Discover YT credentials and optional pool/resource knobs for cluster IT."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Final

from yt_framework.utils.env import load_env_file
from yt_framework.yt.client_base import OperationResources

# tests/integration/yt_cluster/_cluster_config.py -> parents[2] == tests/
_TESTS_DIR: Final[Path] = Path(__file__).resolve().parents[2]
_REPO_ROOT: Final[Path] = _TESTS_DIR.parent


def _merge_os_overrides(cfg: dict[str, str]) -> dict[str, str]:
    """Process environment wins over file-loaded values for known keys."""
    out = dict(cfg)
    keys = (
        "YT_PROXY",
        "YT_TOKEN",
        "YT_TEST_POOL",
        "YT_TEST_POOL_TREE",
        "YT_TEST_MEMORY_GB",
        "YT_TEST_CPU_LIMIT",
        "YT_TEST_JOB_COUNT",
    )
    for k in keys:
        v = os.environ.get(k)
        if v is not None and str(v).strip() != "":
            out[k] = v.strip()
    return out


def load_cluster_test_secrets() -> dict[str, str]:
    """Load cluster test credentials and knobs.

    Priority:
    1. File at YT_FRAMEWORK_CLUSTER_TEST_ENV (if set).
    2. Else repo-root ``yt-cluster-test.env`` if it exists.
    3. Else start empty, then apply process env (so CI can inject YT_PROXY/YT_TOKEN).
    """
    explicit = os.environ.get("YT_FRAMEWORK_CLUSTER_TEST_ENV")
    if explicit:
        cfg = load_env_file(Path(explicit))
    elif (_REPO_ROOT / "yt-cluster-test.env").exists():
        cfg = load_env_file(_REPO_ROOT / "yt-cluster-test.env")
    else:
        cfg = {}
    return _merge_os_overrides(cfg)


def is_cluster_config_ready() -> bool:
    s = load_cluster_test_secrets()
    return bool(s.get("YT_PROXY", "").strip() and s.get("YT_TOKEN", "").strip())


def new_session_run_id() -> str:
    """Unique id for //tmp/yt-framework/testing/<id> and host /tmp mirror."""
    return uuid.uuid4().hex


def secrets_dict_for_yt_client(loaded: dict[str, str]) -> dict[str, str]:
    """Subset passed to ``create_yt_client(..., secrets=...)``."""
    return {
        "YT_PROXY": loaded["YT_PROXY"].strip(),
        "YT_TOKEN": loaded["YT_TOKEN"].strip(),
    }


def operation_resources_from_env(loaded: dict[str, str]) -> OperationResources:
    """Minimal OperationResources; docker_image stays None (cluster default image)."""

    def _opt_int(key: str, default: int) -> int:
        raw = loaded.get(key)
        if raw is None or str(raw).strip() == "":
            return default
        return int(str(raw).strip())

    pool = loaded.get("YT_TEST_POOL", "default").strip() or "default"
    pool_tree_raw = loaded.get("YT_TEST_POOL_TREE", "").strip()
    pool_tree = pool_tree_raw or None
    memory_gb = _opt_int("YT_TEST_MEMORY_GB", 1)
    cpu_limit = _opt_int("YT_TEST_CPU_LIMIT", 1)
    job_count = _opt_int("YT_TEST_JOB_COUNT", 1)
    return OperationResources(
        pool=pool,
        pool_tree=pool_tree,
        docker_image=None,
        memory_gb=memory_gb,
        cpu_limit=cpu_limit,
        job_count=job_count,
    )


def configure_global_yt_for_checkpoint(proxy: str, token: str) -> None:
    """Point ``yt.wrapper`` global client at the same cell (for ``ytjobs.checkpoint``)."""
    import contextlib

    import yt.wrapper as yt  # pyright: ignore[reportMissingImports]

    yt.config.set_proxy(proxy)
    yt.config["token"] = token
    with contextlib.suppress(Exception):
        yt.config["proxy"]["enable_proxy_discovery"] = False  # type: ignore[index]
