"""Exit non-zero if coverage.json reports missed statements under yt_framework or ytjobs."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def _is_first_party(path: str) -> bool:
    p = Path(path).as_posix()
    if p.startswith(("yt_framework/", "ytjobs/")):
        return True
    return "/yt_framework/" in p or "/ytjobs/" in p


def main(argv: list[str]) -> int:
    """Load ``coverage.json`` and exit with status 1 if any first-party line is uncovered.

    Args:
        argv: CLI args; optional path to JSON (default ``coverage.json``).

    Returns:
        Process exit code (0 if no missed statements under ``yt_framework`` / ``ytjobs``).

    """
    cov_path = Path(argv[1] if len(argv) > 1 else "coverage.json")
    data = json.loads(cov_path.read_text(encoding="utf-8"))
    bad: list[tuple[str, list[int]]] = []
    for file_path, payload in data["files"].items():
        if not _is_first_party(file_path):
            continue
        missing = payload.get("missing_lines") or []
        if missing:
            bad.append((file_path, missing))
    if not bad:
        return 0
    for fp, lines in sorted(bad):
        sys.stderr.write(f"{fp}: missing statements on lines {lines}\n")
    sys.stderr.write(
        f"Line-coverage gate failed: {len(bad)} file(s) with missed statements.\n",
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
