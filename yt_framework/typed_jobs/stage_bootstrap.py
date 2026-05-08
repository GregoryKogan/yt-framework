"""Worker-side bootstrap for TypedJob legs.

YT Framework command-mode legs always run a wrapper that:
- extracts `source.tar.gz`
- sets PYTHONPATH / JOB_CONFIG_PATH

TypedJob legs historically rely on stage-side `_ensure_stage_on_path()` helpers.
This base class centralizes that behavior so stage code stays simple.
"""

from __future__ import annotations

import contextlib
import os
import sys
import tarfile
import threading
from pathlib import Path

import yt.wrapper as yt

_BOOTSTRAPPED_LOCK = threading.Lock()
_BOOTSTRAPPED_KEYS: set[str] = set()


def _safe_extractall(tf: tarfile.TarFile, destination: Path) -> None:
    """Extract archive members while rejecting path traversal entries."""
    destination = destination.resolve()
    members = tf.getmembers()
    for member in members:
        member_target = (destination / member.name).resolve()
        if not member_target.is_relative_to(destination):
            msg = (
                f"Refusing to extract archive member outside destination: {member.name}"
            )
            raise RuntimeError(msg)
    for member in members:
        tf.extract(member, destination)


def _find_source_tarball_root() -> str | None:
    """Return sandbox root that contains `source.tar.gz` (or None)."""
    seen: set[str] = set()
    candidates: list[str] = [str(Path.cwd())]

    # TypedJob reducers may run with cwd under tmpfs/modules; tarball stays higher.
    for _ in range(6):
        parent = str(Path(candidates[-1]).parent)
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
        if (Path(base) / "source.tar.gz").is_file():
            return base
    return None


def _infer_root_from_stage_dir(stage_name: str) -> str | None:
    candidates: list[str] = [str(Path.cwd())]
    for _ in range(6):
        parent = str(Path(candidates[-1]).parent)
        if parent == candidates[-1]:
            break
        candidates.append(parent)
    for base in candidates:
        stage_src_path = Path(base) / "stages" / stage_name / "src"
        if stage_src_path.is_dir():
            return base
    return None


def _resolve_bootstrap_root(stage_name: str) -> str:
    root = _find_source_tarball_root()
    if root:
        return root
    inferred_root = _infer_root_from_stage_dir(stage_name)
    if inferred_root:
        return inferred_root
    return str(Path.cwd())


def _ensure_code_archive_extracted(root: str, ytjobs_marker: str) -> None:
    tarball = str(Path(root) / "source.tar.gz")
    if Path(tarball).is_file() and not Path(ytjobs_marker).is_file():
        with tarfile.open(tarball, "r:gz") as tf:
            _safe_extractall(tf, Path(root))


def _ensure_import_paths(root: str, stage_name: str) -> None:
    stage_src = str(Path(root) / "stages" / stage_name / "src")
    if root not in sys.path:
        sys.path.insert(0, root)
    if Path(stage_src).is_dir() and stage_src not in sys.path:
        sys.path.insert(0, stage_src)


def _ensure_job_config_path(root: str, stage_name: str) -> None:
    job_config_path = str(Path(root) / "stages" / stage_name / "config.yaml")
    if Path(job_config_path).is_file():
        os.environ["JOB_CONFIG_PATH"] = job_config_path


def _extract_tokenizer_artifact_if_needed(root: str) -> None:
    tokenizer_artifact_file = os.environ.get("TOKENIZER_ARTIFACT_FILE", "").strip()
    tokenizer_artifact_dir = os.environ.get("TOKENIZER_ARTIFACT_DIR", "").strip()
    if not tokenizer_artifact_file:
        return

    artifact_tar = str(Path(root) / tokenizer_artifact_file)
    if not tokenizer_artifact_dir:
        artifact_name = os.environ.get("TOKENIZER_ARTIFACT_NAME", "default").strip()
        tokenizer_artifact_dir = str(
            Path("tokenizer_artifacts") / (artifact_name or "default")
        )
        os.environ["TOKENIZER_ARTIFACT_DIR"] = tokenizer_artifact_dir

    artifact_dir_abs = str(Path(root) / tokenizer_artifact_dir)
    if not Path(artifact_tar).is_file():
        return

    Path(artifact_dir_abs).mkdir(parents=True, exist_ok=True)
    marker = str(Path(artifact_dir_abs) / ".extracted")
    if Path(marker).is_file():
        return

    with tarfile.open(artifact_tar, "r:gz") as tf:
        _safe_extractall(tf, Path(artifact_dir_abs))
    Path(marker).write_text("ok\n", encoding="utf-8")


def _bootstrap_once(stage_name: str) -> None:
    root = _resolve_bootstrap_root(stage_name)
    ytjobs_marker = str(Path(root) / "ytjobs" / "__init__.py")

    # Extract only if we haven't already bootstrapped this root.
    # Marker is `ytjobs/__init__.py` because ytjobs/ is always present in the archive.
    key = f"{root}::{stage_name}::{ytjobs_marker}"
    with _BOOTSTRAPPED_LOCK:
        if key in _BOOTSTRAPPED_KEYS:
            return
        _BOOTSTRAPPED_KEYS.add(key)

    _ensure_code_archive_extracted(root, ytjobs_marker)
    _ensure_import_paths(root, stage_name)
    _ensure_job_config_path(root, stage_name)
    _extract_tokenizer_artifact_if_needed(root)


class StageBootstrapTypedJob(yt.TypedJob):
    """Base class for TypedJob legs with worker-side bootstrap.

    On unpickle, extracts ``source.tar.gz`` when needed, prepends the archive root
    and ``stages/<stage>/src`` to ``sys.path``, and sets ``JOB_CONFIG_PATH`` when
    the stage config file exists. Uses ``__getstate__`` / ``__setstate__`` so
    bootstrap runs on the worker before ``__call__``.
    """

    def __getstate__(self) -> object:  # pragma: no cover (driver-side)
        """Return picklable state so ``__setstate__`` runs on the worker."""
        # Ensure the pickling machinery carries a state object so `__setstate__` is called
        # during unpickling (required for our worker-side bootstrap).
        return dict(getattr(self, "__dict__", {}) or {})

    def __setstate__(self, state: object) -> None:  # pragma: no cover (worker-side)
        """Restore state and run worker-side path/bootstrap once per unpickle."""
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
