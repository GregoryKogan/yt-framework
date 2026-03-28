"""Tests for yt_framework.utils.ignore: .ytignore patterns and matcher."""

from pathlib import Path

from yt_framework.utils.ignore import (
    YTIgnoreMatcher,
    YTIgnorePattern,
    should_ignore_file,
)


def test_yt_ignore_pattern_rejects_path_outside_base_dir() -> None:
    base = Path("/project")
    pattern = YTIgnorePattern("*.pyc", base)
    assert not pattern.matches(Path("/other/pkg.pyc"))


def test_yt_ignore_pattern_matches_simple_suffix_under_base(tmp_path: Path) -> None:
    pattern = YTIgnorePattern("*.pyc", tmp_path)
    assert pattern.matches(tmp_path / "mod.pyc")


def test_yt_ignore_pattern_rooted_only_at_repository_root(tmp_path: Path) -> None:
    pattern = YTIgnorePattern("/build", tmp_path)
    assert pattern.matches(tmp_path / "build")
    assert not pattern.matches(tmp_path / "src" / "build")


def test_yt_ignore_pattern_recursive_glob_requires_subdirectory(tmp_path: Path) -> None:
    pattern = YTIgnorePattern("**/*.log", tmp_path)
    assert pattern.matches(tmp_path / "src" / "a.log")
    assert not pattern.matches(tmp_path / "a.log")


def test_yt_ignore_matcher_applies_glob_from_ytignore_file(tmp_path: Path) -> None:
    (tmp_path / ".ytignore").write_text("*.log\n", encoding="utf-8")
    matcher = YTIgnoreMatcher(tmp_path)
    assert matcher.should_ignore(tmp_path / "x.log")
    assert not matcher.should_ignore(tmp_path / "x.py")


def test_yt_ignore_matcher_negation_overrides_prior_ignore(tmp_path: Path) -> None:
    (tmp_path / ".ytignore").write_text("*.log\n!important.log\n", encoding="utf-8")
    matcher = YTIgnoreMatcher(tmp_path)
    assert matcher.should_ignore(tmp_path / "debug.log")
    assert not matcher.should_ignore(tmp_path / "important.log")


def test_yt_ignore_matcher_always_ignores_dot_ytignore_file(tmp_path: Path) -> None:
    (tmp_path / ".ytignore").write_text("", encoding="utf-8")
    matcher = YTIgnoreMatcher(tmp_path)
    assert matcher.should_ignore(tmp_path / ".ytignore")


def test_should_ignore_file_delegates_to_matcher(tmp_path: Path) -> None:
    (tmp_path / ".ytignore").write_text("*.pyc\n", encoding="utf-8")
    assert should_ignore_file(tmp_path / "f.pyc", tmp_path)
    assert not should_ignore_file(tmp_path / "f.py", tmp_path)
