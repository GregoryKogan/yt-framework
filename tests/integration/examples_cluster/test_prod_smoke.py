"""Run prod-mode example pipelines on a real cell (opt-in per demo)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from integration.example_pipelines._manifest import repo_root
from integration.examples_cluster._requirements import (
    docker_example_opt_in,
    s3_example_has_config_and_secrets,
)
from integration.yt_cluster._cluster_config import load_cluster_test_secrets
from yt_framework.utils.env import load_secrets

_CLUSTER_TIMEOUT_S = 900


def _copy_example(slug: str, dest_parent: Path) -> Path:
    src = repo_root() / "examples" / slug
    dest = dest_parent / slug
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(
        src,
        dest,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        dirs_exist_ok=False,
    )
    return dest


def _write_merged_secrets(configs_dir: Path, cluster_secrets: dict[str, str]) -> None:
    """Prefer on-disk example ``secrets.env``, then overlay cluster YT keys."""
    merged = dict(load_secrets(configs_dir))
    merged.update({k: v for k, v in cluster_secrets.items() if str(v).strip()})
    lines = [f"{k}={v}" for k, v in sorted(merged.items()) if str(v).strip()]
    (configs_dir / "secrets.env").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    root = str(repo_root())
    pp = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = root if not pp else root + os.pathsep + pp
    bindir = str(Path(sys.executable).resolve().parent)
    env["PATH"] = bindir + os.pathsep + env.get("PATH", "")
    return env


def _run(tmp_pipeline: Path, argv: tuple[str, ...]) -> None:
    cmd = [sys.executable, *argv]
    proc = subprocess.run(  # noqa: S603
        cmd,
        cwd=tmp_pipeline,
        check=False,
        capture_output=True,
        text=True,
        timeout=_CLUSTER_TIMEOUT_S,
        env=_subprocess_env(),
    )
    if proc.returncode != 0:
        tail_out = (proc.stdout or "")[-6000:]
        tail_err = (proc.stderr or "")[-6000:]
        msg = (
            f"Example subprocess failed: {cmd} (cwd={tmp_pipeline})\n"
            f"exit={proc.returncode}\n--- stdout (tail) ---\n{tail_out}\n"
            f"--- stderr (tail) ---\n{tail_err}"
        )
        raise AssertionError(msg)


@pytest.mark.yt_cluster
@pytest.mark.integration
def test_example_06_s3_integration_prod(tmp_path: Path) -> None:
    cluster = load_cluster_test_secrets()
    ok, reason = s3_example_has_config_and_secrets(
        repo_root(),
        env_opt_in=os.environ.get("YT_FRAMEWORK_EXAMPLE_S3", "").lower()
        in ("1", "true", "yes"),
        cluster_secrets=cluster,
    )
    if not ok:
        pytest.skip(reason)
    tree = _copy_example("06_s3_integration", tmp_path)
    _write_merged_secrets(tree / "configs", cluster)
    _run(tree, ("pipeline.py",))


@pytest.mark.yt_cluster
@pytest.mark.integration
def test_example_07_custom_docker_prod(tmp_path: Path) -> None:
    ok, reason = docker_example_opt_in(
        env_opt_in=os.environ.get("YT_FRAMEWORK_EXAMPLE_DOCKER", "").lower()
        in ("1", "true", "yes"),
    )
    if not ok:
        pytest.skip(reason)
    cluster = load_cluster_test_secrets()
    tree = _copy_example("07_custom_docker", tmp_path)
    _write_merged_secrets(tree / "configs", cluster)
    _run(tree, ("pipeline.py",))
