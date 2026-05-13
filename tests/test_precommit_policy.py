"""Tests for ``scripts/precommit`` policy helpers (max file line count)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_mfl_path = _REPO_ROOT / "scripts" / "precommit" / "checks" / "max_file_lines.py"
_spec = importlib.util.spec_from_file_location("_precommit_max_file_lines", _mfl_path)
assert _spec is not None and _spec.loader is not None
_mfl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mfl)
collect_violations = _mfl.collect_violations
wc_line_count = _mfl.wc_line_count


def test_wc_line_count_counts_newlines_like_wc(tmp_path: Path) -> None:
    path = tmp_path / "sample.txt"
    path.write_bytes(b"one\ntwo\nthree")
    assert wc_line_count(path) == 2


def test_collect_violations_empty_when_under_limit(tmp_path: Path) -> None:
    root = tmp_path
    pkg = root / "yt_framework"
    pkg.mkdir()
    small = pkg / "small.py"
    small.write_bytes(b"x\n" * 2)
    assert collect_violations(root, ["yt_framework"], 10) == []


def test_collect_violations_reports_file_over_limit(tmp_path: Path) -> None:
    root = tmp_path
    pkg = root / "yt_framework"
    pkg.mkdir()
    big = pkg / "big.py"
    big.write_bytes(b"#\n" * 5)
    out = collect_violations(root, ["yt_framework"], 3)
    assert out == ["yt_framework/big.py: 5 lines (limit 3)"]


def test_collect_violations_reports_missing_root(tmp_path: Path) -> None:
    out = collect_violations(tmp_path, ["yt_framework"], 100)
    assert out == ["max_file_lines: root is not a directory: yt_framework"]
