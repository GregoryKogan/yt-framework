"""Tests for yt_framework.utils.sys_path: stage import path context manager."""

import sys
from pathlib import Path

from yt_framework.utils.sys_path import stage_src_path


def test_stage_src_path_inserts_stage_src_at_front_and_restores(tmp_path: Path) -> None:
    stage = tmp_path / "my_stage"
    src = stage / "src"
    src.mkdir(parents=True)
    before = list(sys.path)
    with stage_src_path(stage):
        assert sys.path[0] == str(src), "stage src must take precedence for imports"
    assert sys.path == before, "sys.path must be restored after the block"


def test_stage_src_path_moves_existing_entry_to_position_zero(tmp_path: Path) -> None:
    stage = tmp_path / "st"
    src = stage / "src"
    src.mkdir(parents=True)
    src_str = str(src)
    saved = list(sys.path)
    try:
        sys.path.append(src_str)
        sys.path.insert(0, "/other_front")
        before_len = len(sys.path)
        with stage_src_path(stage):
            assert sys.path[0] == src_str, (
                "duplicate src path should be reinserted at 0"
            )
        assert src_str not in sys.path
        assert len(sys.path) == before_len - 1
    finally:
        sys.path[:] = saved
