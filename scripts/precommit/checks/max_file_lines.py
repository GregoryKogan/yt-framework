"""Enforce a maximum line count per Python file (wc -l semantics: newline count)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def wc_line_count(path: Path) -> int:
    """Return the same line count as POSIX ``wc -l`` (number of newline bytes)."""
    data = path.read_bytes()
    return data.count(b"\n")


def collect_violations(repo_root: Path, roots: list[str], limit: int) -> list[str]:
    """List human-readable violation messages for ``*.py`` files over ``limit`` lines."""
    violations: list[str] = []
    for root_name in roots:
        root = repo_root / root_name
        if not root.is_dir():
            violations.append(f"max_file_lines: root is not a directory: {root_name}")
            continue
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            n = wc_line_count(path)
            if n > limit:
                rel = path.relative_to(repo_root)
                violations.append(f"{rel}: {n} lines (limit {limit})")
    return violations
