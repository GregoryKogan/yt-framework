"""Limit immediate directory fan-out under configured roots (git check-ignore aware)."""

from __future__ import annotations

import shutil
import subprocess
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def _git_bin() -> str | None:
    """Resolve ``git`` executable path for subprocess use."""
    return shutil.which("git")


def _git_check_ignore_batch(
    repo_root: Path, rel_paths: list[str]
) -> tuple[set[str], str | None]:
    """Return paths git treats as ignored; ``(set(), error)`` on git failure."""
    if not rel_paths:
        return set(), None
    git_exe = _git_bin()
    if git_exe is None:
        return set(), "git executable not found on PATH"
    payload = "\0".join(rel_paths) + "\0"
    try:
        proc = subprocess.run(
            [git_exe, "-C", str(repo_root), "check-ignore", "-z", "--stdin"],
            input=payload.encode(),
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        return set(), f"git check-ignore failed: {exc}"
    if proc.returncode not in (0, 1):
        err = proc.stderr.decode().strip() or f"exit {proc.returncode}"
        return set(), f"git check-ignore: {err}"
    raw = proc.stdout.split(b"\0")
    ignored: set[str] = set()
    for chunk in raw:
        if not chunk:
            continue
        ignored.add(chunk.decode())
    return ignored, None


def _dir_is_ignored(repo_root: Path, rel_dir: str) -> tuple[bool, str | None]:
    """Return whether git treats ``rel_dir`` as ignored (``check-ignore`` exit 0)."""
    git_exe = _git_bin()
    if git_exe is None:
        return False, "git executable not found on PATH"
    try:
        proc = subprocess.run(
            [git_exe, "-C", str(repo_root), "check-ignore", "-q", "--", rel_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        return False, f"git check-ignore failed: {exc}"
    if proc.returncode == 0:
        return True, None
    if proc.returncode == 1:
        return False, None
    err = proc.stderr.decode().strip() or f"exit {proc.returncode}"
    return False, f"git check-ignore: {err}"


def _violations_under_root(repo_root: Path, root: Path, limit: int) -> list[str]:
    """Scan one package root directory tree for fan-out violations."""
    violations: list[str] = []
    stack: deque[Path] = deque([root])
    while stack:
        d = stack.popleft()
        rel_d = d.relative_to(repo_root).as_posix()
        ign, err = _dir_is_ignored(repo_root, rel_d)
        if err:
            violations.append(f"max_dir_entries: {rel_d}: {err}")
            continue
        if ign:
            continue

        try:
            children = list(d.iterdir())
        except OSError as exc:
            violations.append(f"max_dir_entries: cannot list {rel_d}: {exc}")
            continue

        rel_children = [c.relative_to(repo_root).as_posix() for c in children]
        ignored_set, batch_err = _git_check_ignore_batch(repo_root, rel_children)
        if batch_err:
            violations.append(f"max_dir_entries: {rel_d}: {batch_err}")
            continue

        visible = [p for p in rel_children if p not in ignored_set]
        if len(visible) > limit:
            violations.append(
                f"{rel_d}: {len(visible)} immediate children (limit {limit})"
            )

        for c in children:
            if c.is_dir():
                stack.append(c)

    return violations


def collect_violations(repo_root: Path, roots: list[str], limit: int) -> list[str]:
    """List dirs whose non-ignored immediate child count exceeds ``limit``."""
    violations: list[str] = []
    if not (repo_root / ".git").exists():
        violations.append(
            f"max_dir_entries: not a git repository (missing .git under {repo_root})"
        )
        return violations

    for root_name in roots:
        root = repo_root / root_name
        if not root.is_dir():
            violations.append(f"max_dir_entries: root is not a directory: {root_name}")
            continue
        violations.extend(_violations_under_root(repo_root, root, limit))

    return violations
