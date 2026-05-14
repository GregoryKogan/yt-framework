"""Run pytest with first-party coverage, then enforce 100% statement coverage (CI / pre-commit)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    """Run pytest with coverage JSON, then ``check_line_coverage`` on that file.

    Returns:
        Pytest exit code, or the checker exit code if pytest succeeded.

    """
    pytest_cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-m",
        "not yt_cluster",
        "-q",
        "--cov=yt_framework",
        "--cov=ytjobs",
        "--cov-report=json:coverage.json",
    ]
    r1 = subprocess.run(  # noqa: S603
        pytest_cmd,
        cwd=_REPO_ROOT,
        check=False,
        env=os.environ,
    )
    if r1.returncode != 0:
        return r1.returncode
    check_cmd = [
        sys.executable,
        str(_REPO_ROOT / "scripts/coverage/check_line_coverage.py"),
        str(_REPO_ROOT / "coverage.json"),
    ]
    r2 = subprocess.run(  # noqa: S603
        check_cmd,
        cwd=_REPO_ROOT,
        check=False,
        env=os.environ,
    )
    return r2.returncode


if __name__ == "__main__":
    raise SystemExit(main())
