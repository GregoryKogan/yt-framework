"""Run ``ci_tier: always`` and optional ``manual`` example pipelines via subprocess."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from integration.example_pipelines._manifest import (
    discover_pipeline_dirs,
    load_manifest_specs,
    repo_root,
)

_MARK = pytest.mark.examples


def _timeout_seconds(slug: str) -> int:
    if slug in {"04_map_operation", "09_multiple_operations", "video_gpu"}:
        return 300
    if slug in {"03_yql_operations", "02_multi_stage_pipeline"}:
        return 240
    return 120


def _argv_cases() -> list[tuple[str, tuple[str, ...]]]:
    out: list[tuple[str, tuple[str, ...]]] = []
    for spec in load_manifest_specs():
        if spec.ci_tier != "always":
            continue
        if spec.slug not in discover_pipeline_dirs():
            continue
        out.extend((spec.slug, argv) for argv in spec.commands)
    return out


def _manual_argv_cases() -> list[tuple[str, tuple[str, ...]]]:
    out: list[tuple[str, tuple[str, ...]]] = []
    for spec in load_manifest_specs():
        if spec.ci_tier != "manual":
            continue
        out.extend((spec.slug, argv) for argv in spec.commands)
    return out


_CI_ALWAYS_CASES = _argv_cases()
_CI_ALWAYS_IDS = [f"{s}:{':'.join(a)}" for s, a in _CI_ALWAYS_CASES]

_MANUAL_CASES = _manual_argv_cases()
_MANUAL_IDS = [f"{s}:{':'.join(a)}" for s, a in _MANUAL_CASES]


def _subprocess_env() -> dict[str, str]:
    """Match driver deps: repo on ``PYTHONPATH``, job shells resolve ``python3`` first."""
    env = os.environ.copy()
    root = str(repo_root())
    pp = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = root if not pp else root + os.pathsep + pp
    bindir = str(Path(sys.executable).resolve().parent)
    env["PATH"] = bindir + os.pathsep + env.get("PATH", "")
    return env


def _run_pipeline(slug: str, argv: tuple[str, ...], timeout: int) -> None:
    cwd = repo_root() / "examples" / slug
    cmd = [sys.executable, *argv]
    proc = subprocess.run(  # noqa: S603
        cmd,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_subprocess_env(),
    )
    if proc.returncode != 0:
        tail_out = (proc.stdout or "")[-4000:]
        tail_err = (proc.stderr or "")[-4000:]
        msg = (
            f"Example {slug!r} failed: {cmd} (cwd={cwd})\n"
            f"exit={proc.returncode}\n--- stdout (tail) ---\n{tail_out}\n"
            f"--- stderr (tail) ---\n{tail_err}"
        )
        raise AssertionError(msg)


@_MARK
@pytest.mark.integration
@pytest.mark.parametrize(
    ("slug", "argv"),
    _CI_ALWAYS_CASES,
    ids=_CI_ALWAYS_IDS,
)
def test_example_pipeline_dev_smoke(slug: str, argv: tuple[str, ...]) -> None:
    _run_pipeline(slug, argv, _timeout_seconds(slug))


@_MARK
@pytest.mark.integration
@pytest.mark.parametrize(
    ("slug", "argv"),
    _MANUAL_CASES,
    ids=_MANUAL_IDS,
)
def test_example_pipeline_manual_when_enabled(slug: str, argv: tuple[str, ...]) -> None:
    if os.environ.get("YT_FRAMEWORK_EXAMPLE_VIDEO_GPU", "").lower() not in (
        "1",
        "true",
        "yes",
    ):
        pytest.skip("Set YT_FRAMEWORK_EXAMPLE_VIDEO_GPU=1 to run video_gpu locally.")
    if slug != "video_gpu":
        pytest.skip("Only video_gpu is supported for manual tier in this harness.")
    try:
        import torch  # noqa: F401
    except ImportError:
        pytest.skip(
            "torch is not installed; install optional GPU stack to run this example."
        )
    _run_pipeline(slug, argv, _timeout_seconds(slug))
