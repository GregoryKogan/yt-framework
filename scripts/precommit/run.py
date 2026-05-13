"""Run configured repo policy checks (pre-commit local hook entrypoint)."""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.precommit.checks import max_dir_entries as max_dir_entries_check
from scripts.precommit.checks import max_file_lines as max_file_lines_check


def _load_pre_commit_tool_table(repo_root: Path) -> dict[str, Any]:
    pyproject = repo_root / "pyproject.toml"
    with pyproject.open("rb") as f:
        data = tomllib.load(f)
    tool = data.get("tool", {})
    if not isinstance(tool, dict):
        msg = "pyproject.toml [tool] must be a TOML table"
        raise TypeError(msg)
    ytf = tool.get("yt_framework", {})
    if not isinstance(ytf, dict):
        msg = "pyproject.toml [tool.yt_framework] must be a TOML table"
        raise TypeError(msg)
    pc = ytf.get("pre_commit", {})
    if not isinstance(pc, dict):
        msg = "pyproject.toml [tool.yt_framework.pre_commit] must be a TOML table"
        raise TypeError(msg)
    return pc


def main() -> int:
    """Load ``[tool.yt_framework.pre_commit]`` and run enabled checks; return shell exit code."""
    repo_root = _REPO_ROOT
    table = _load_pre_commit_tool_table(repo_root)
    failures: list[str] = []

    mfl = table.get("max_file_lines")
    if isinstance(mfl, dict) and mfl.get("enabled", True):
        limit = int(mfl.get("limit", 1500))
        roots_raw = mfl.get("roots", ["yt_framework", "ytjobs"])
        if not isinstance(roots_raw, list):
            msg = "max_file_lines.roots must be a TOML array of strings"
            raise TypeError(msg)
        roots = [str(x) for x in roots_raw]
        failures.extend(
            max_file_lines_check.collect_violations(repo_root, roots, limit)
        )

    mde = table.get("max_dir_entries")
    if isinstance(mde, dict) and mde.get("enabled", True):
        limit_de = int(mde.get("limit", 15))
        roots_de_raw = mde.get("roots", ["yt_framework", "ytjobs"])
        if not isinstance(roots_de_raw, list):
            msg = "max_dir_entries.roots must be a TOML array of strings"
            raise TypeError(msg)
        roots_de = [str(x) for x in roots_de_raw]
        failures.extend(
            max_dir_entries_check.collect_violations(repo_root, roots_de, limit_de)
        )

    if failures:
        sys.stderr.write("yt_framework pre_commit policy failed:\n")
        for line in failures:
            sys.stderr.write(f"  {line}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
