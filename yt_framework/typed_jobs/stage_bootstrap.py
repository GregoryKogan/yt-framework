from __future__ import annotations

"""
Worker-side bootstrap for TypedJob legs.

YT Framework command-mode legs always run a wrapper that:
- extracts `source.tar.gz`
- sets PYTHONPATH / JOB_CONFIG_PATH

TypedJob legs historically rely on stage-side `_ensure_stage_on_path()` helpers.
This base class centralizes that behavior so stage code stays simple.
"""

import contextlib
import os
import sys
import tarfile
import threading
from typing import Any

import yt.wrapper as yt

_BOOTSTRAPPED_LOCK = threading.Lock()
_BOOTSTRAPPED_KEYS: set[str] = set()


def _find_source_tarball_root() -> str | None:
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

    tokenizer_artifact_file = os.environ.get("TOKENIZER_ARTIFACT_FILE", "").strip()
    tokenizer_artifact_dir = os.environ.get("TOKENIZER_ARTIFACT_DIR", "").strip()
    if tokenizer_artifact_file:
        artifact_tar = os.path.join(root, tokenizer_artifact_file)
        if not tokenizer_artifact_dir:
            artifact_name = (
                os.environ.get("TOKENIZER_ARTIFACT_NAME", "default").strip()
                or "default"
            )
            tokenizer_artifact_dir = os.path.join("tokenizer_artifacts", artifact_name)
            os.environ["TOKENIZER_ARTIFACT_DIR"] = tokenizer_artifact_dir
        artifact_dir_abs = os.path.join(root, tokenizer_artifact_dir)
        if os.path.isfile(artifact_tar):
            os.makedirs(artifact_dir_abs, exist_ok=True)
            marker = os.path.join(artifact_dir_abs, ".extracted")
            if not os.path.isfile(marker):
                with tarfile.open(artifact_tar, "r:gz") as tf:
                    tf.extractall(artifact_dir_abs)
                with open(marker, "w", encoding="utf-8") as m:
                    m.write("ok\n")


class StageBootstrapTypedJob(yt.TypedJob):
    """Base class for TypedJob legs with worker-side bootstrap.

    On unpickle, extracts ``source.tar.gz`` when needed, prepends the archive root
    and ``stages/<stage>/src`` to ``sys.path``, and sets ``JOB_CONFIG_PATH`` when
    the stage config file exists. Uses ``__getstate__`` / ``__setstate__`` so
    bootstrap runs on the worker before ``__call__``.
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
            with contextlib.suppress(Exception):
                self.__dict__.update(state.__dict__)
