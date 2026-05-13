"""Tests for ``scripts/precommit`` policy helpers."""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_check_module(relative: str, name: str) -> object:
    path = _REPO_ROOT / "scripts" / "precommit" / "checks" / relative
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_mfl = _load_check_module("max_file_lines.py", "_precommit_max_file_lines")
collect_violations_lines = _mfl.collect_violations
wc_line_count = _mfl.wc_line_count

_mde = _load_check_module("max_dir_entries.py", "_precommit_max_dir_entries")
collect_violations_dirs = _mde.collect_violations


def _git_init(repo: Path) -> None:
    git = shutil.which("git")
    assert git is not None
    subprocess.run(  # noqa: S603
        [git, "init"],
        cwd=repo,
        check=True,
        capture_output=True,
    )


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
    assert collect_violations_lines(root, ["yt_framework"], 10) == []


def test_collect_violations_reports_file_over_limit(tmp_path: Path) -> None:
    root = tmp_path
    pkg = root / "yt_framework"
    pkg.mkdir()
    big = pkg / "big.py"
    big.write_bytes(b"#\n" * 5)
    out = collect_violations_lines(root, ["yt_framework"], 3)
    assert out == ["yt_framework/big.py: 5 lines (limit 3)"]


def test_collect_violations_reports_missing_root(tmp_path: Path) -> None:
    out = collect_violations_lines(tmp_path, ["yt_framework"], 100)
    assert out == ["max_file_lines: root is not a directory: yt_framework"]


def test_max_dir_entries_requires_git(tmp_path: Path) -> None:
    out = collect_violations_dirs(tmp_path, ["yt_framework"], 15)
    assert out == [
        f"max_dir_entries: not a git repository (missing .git under {tmp_path})"
    ]


def test_max_dir_entries_flags_sixteen_children(tmp_path: Path) -> None:
    _git_init(tmp_path)
    busy = tmp_path / "yt_framework" / "busy"
    busy.mkdir(parents=True)
    for i in range(16):
        (busy / f"f{i}.txt").write_text("x", encoding="utf-8")
    out = collect_violations_dirs(tmp_path, ["yt_framework"], 15)
    assert [x for x in out if "yt_framework/busy" in x] == [
        "yt_framework/busy: 16 immediate children (limit 15)"
    ]


def test_max_dir_entries_allows_exactly_limit_children(tmp_path: Path) -> None:
    _git_init(tmp_path)
    busy = tmp_path / "yt_framework" / "busy"
    busy.mkdir(parents=True)
    for i in range(15):
        (busy / f"f{i}.txt").write_text("x", encoding="utf-8")
    out = collect_violations_dirs(tmp_path, ["yt_framework"], 15)
    assert [x for x in out if "busy" in x] == []
