from __future__ import annotations

"""
Worker-side bootstrap for TypedJob legs.

YT Framework command-mode legs always run a wrapper that:
- extracts `source.tar.gz`
- sets PYTHONPATH / JOB_CONFIG_PATH

TypedJob legs historically rely on stage-side `_ensure_stage_on_path()` helpers.
This base class centralizes that behavior so stage code stays simple.
"""

import os
import sys
import tarfile
import threading
from typing import Any, Optional

import yt.wrapper as yt


_BOOTSTRAPPED_LOCK = threading.Lock()
_BOOTSTRAPPED_KEYS: set[str] = set()


def _find_source_tarball_root() -> Optional[str]:
    """Return sandbox root that contains `source.tar.gz` (or None)."""
    seen: set[str] = set()
    candidates: list[str] = [os.getcwd()]

    # TypedJob reducers may run with cwd under tmpfs/modules; tarball stays higher.
    for _ in range(6):
        parent = os.path.dirname(candidates[-1])
        if parent == candidates[-1] or parent in seen:
            break
        candidates.append(parent)

    for extra in ("/slot/sandbox",):
        if extra not in candidates:
            candidates.append(extra)

    for base in candidates:
        if not base or base in seen:
            continue
        seen.add(base)
        if os.path.isfile(os.path.join(base, "source.tar.gz")):
            return base
    return None


def _bootstrap_once(stage_name: str) -> None:
    root = _find_source_tarball_root()

    # If code archive was already extracted, `source.tar.gz` may not exist anymore.
    # In that case, infer the root from the stage directory.
    if not root:
        candidates: list[str] = [os.getcwd()]
        for _ in range(6):
            parent = os.path.dirname(candidates[-1])
            if parent == candidates[-1]:
                break
            candidates.append(parent)

        for base in candidates:
            stage_src = os.path.join(base, "stages", stage_name, "src")
            if os.path.isdir(stage_src):
                root = base
                break

    if not root:
        # Last resort: still try current cwd to avoid hard failures in unusual sandboxes.
        root = os.getcwd()

    stage_src = os.path.join(root, "stages", stage_name, "src")
    ytjobs_marker = os.path.join(root, "ytjobs", "__init__.py")

    # Extract only if we haven't already bootstrapped this root.
    # Marker is `ytjobs/__init__.py` because ytjobs/ is always present in the archive.
    key = f"{root}::{stage_name}::{ytjobs_marker}"
    with _BOOTSTRAPPED_LOCK:
        if key in _BOOTSTRAPPED_KEYS:
            return
        _BOOTSTRAPPED_KEYS.add(key)

    tarball = os.path.join(root, "source.tar.gz")
    if os.path.isfile(tarball) and not os.path.isfile(ytjobs_marker):
        with tarfile.open(tarball, "r:gz") as tf:
            tf.extractall(root)

    # Ensure imports work for:
    # - framework packages (yt_framework/ embedded in archive root)
    # - user packages from upload_modules
    # - stage-local helpers under stages/<stage_name>/src
    if root not in sys.path:
        sys.path.insert(0, root)
    if os.path.isdir(stage_src) and stage_src not in sys.path:
        sys.path.insert(0, stage_src)

    # Parity with command-mode wrappers.
    job_config_path = os.path.join(root, "stages", stage_name, "config.yaml")
    if os.path.isfile(job_config_path):
        os.environ["JOB_CONFIG_PATH"] = job_config_path


class StageBootstrapTypedJob(yt.TypedJob):
    """
    Base class for TypedJob legs.

    It bootstraps worker-side imports by:
    - extracting `source.tar.gz` (if still present)
    - ensuring `sys.path` includes archive root + `stages/<stage>/src`
    - setting `JOB_CONFIG_PATH` (if available)

    Implementation detail:
    - `__getstate__` + `__setstate__` runs on the worker during unpickling of the job instance,
      which is early enough before `__call__` starts executing.
    """

    def __getstate__(self) -> Any:  # pragma: no cover (driver-side)
        # Ensure the pickling machinery carries a state object so `__setstate__` is called
        # during unpickling (required for our worker-side bootstrap).
        return dict(getattr(self, "__dict__", {}) or {})

    def __setstate__(self, state: Any) -> None:  # pragma: no cover (worker-side)
        stage_name = os.environ.get("YT_STAGE_NAME", "").strip()
        if stage_name:
            _bootstrap_once(stage_name)

        # Keep default object state restore.
        if isinstance(state, dict):
            self.__dict__.update(state)
        else:
            # Be permissive: some serializers may pass non-dict state.
            try:
                self.__dict__.update(state.__dict__)
            except Exception:
                pass

