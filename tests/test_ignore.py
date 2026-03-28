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


def test_yt_ignore_pattern_directory_trailing_slash_matches_nested_files(
    tmp_path: Path,
) -> None:
    pattern = YTIgnorePattern("build/", tmp_path)
    assert pattern.matches(tmp_path / "pkg" / "build" / "out.txt")


def test_yt_ignore_pattern_matches_segment_glob_without_double_star(
    tmp_path: Path,
) -> None:
    pattern = YTIgnorePattern("src/*.log", tmp_path)
    assert pattern.matches(tmp_path / "src" / "a.log")
    assert not pattern.matches(tmp_path / "src" / "sub" / "a.log")


def test_yt_ignore_pattern_plain_suffix_matches_file_in_subdirectory(
    tmp_path: Path,
) -> None:
    pattern = YTIgnorePattern("*.dat", tmp_path)
    assert pattern.matches(tmp_path / "nested" / "x.dat")


def test_yt_ignore_pattern_double_star_in_middle_of_pattern(tmp_path: Path) -> None:
    pattern = YTIgnorePattern("pre**/fix.txt", tmp_path)
    assert pattern.matches(tmp_path / "pre" / "mid" / "fix.txt")


def test_yt_ignore_matcher_skips_corrupt_ytignore_file(tmp_path: Path) -> None:
    (tmp_path / ".ytignore").write_bytes(b"\xff\xff")
    matcher = YTIgnoreMatcher(tmp_path)
    assert not matcher.should_ignore(tmp_path / "x.pyc")


def test_yt_ignore_matcher_ignores_comments_and_blank_negation_pattern(
    tmp_path: Path,
) -> None:
    (tmp_path / ".ytignore").write_text("# comment\n*.log\n!  \n", encoding="utf-8")
    matcher = YTIgnoreMatcher(tmp_path)
    assert matcher.should_ignore(tmp_path / "noise.log")


def test_yt_ignore_matcher_resolves_relative_file_path(tmp_path: Path) -> None:
    sub = tmp_path / "sub"
    sub.mkdir()
    (tmp_path / ".ytignore").write_text("*.log\n", encoding="utf-8")
    (sub / "a.log").write_text("", encoding="utf-8")
    matcher = YTIgnoreMatcher(tmp_path)
    assert matcher.should_ignore(Path("sub/a.log"))


def test_yt_ignore_pattern_single_star_path_segment_matches_middle_directory(
    tmp_path: Path,
) -> None:
    pattern = YTIgnorePattern("src/*/x.log", tmp_path)
    assert pattern.matches(tmp_path / "src" / "any" / "x.log")


def test_yt_ignore_pattern_directory_matches_immediate_parent_path_before_name_scan(
    tmp_path: Path,
) -> None:
    pattern = YTIgnorePattern("pkg/", tmp_path)
    assert pattern.matches(tmp_path / "pkg" / "file.txt")


def test_yt_ignore_pattern_directory_trailing_slash_no_segment_match_returns_false(
    tmp_path: Path,
) -> None:
    pattern = YTIgnorePattern("nowhere/", tmp_path)
    assert not pattern.matches(tmp_path / "a" / "b" / "c.txt")


def test_yt_ignore_pattern_plain_name_matches_filename_in_subdirectory(
    tmp_path: Path,
) -> None:
    pattern = YTIgnorePattern("marker.txt", tmp_path)
    assert pattern.matches(tmp_path / "deep" / "marker.txt")
