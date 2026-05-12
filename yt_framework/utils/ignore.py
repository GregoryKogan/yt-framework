"""`.ytignore` parsing (gitignore-style) for upload tarballs."""

import fnmatch
import logging
import re
from pathlib import Path


class YTIgnorePattern:
    """Represents a single .ytignore pattern.

    This class compiles a pattern string into a regex for efficient matching
    against file paths. It supports the same syntax as .gitignore files.

    Pattern Types:
    - Simple wildcards: `*.pyc`, `test?`
    - Character classes: `*.py[cod]`
    - Recursive wildcards: `**/*.log`
    - Directory patterns: `build/` (trailing slash)
    - Rooted patterns: `/config` (leading slash, root-only)
    - Negation patterns: `!important.py` (un-ignore)

    Examples:
        Simple wildcard:

        >>> from pathlib import Path
        >>> pattern = YTIgnorePattern("*.pyc", Path("/project"))
        >>> pattern.matches(Path("/project/test.pyc"))
        True
        >>> pattern.matches(Path("/project/test.py"))
        False

        Directory pattern:

        >>> pattern = YTIgnorePattern("__pycache__/", Path("/project"))
        >>> pattern.matches(Path("/project/__pycache__/module.pyc"))
        True
        >>> pattern.matches(Path("/project/src/__pycache__/module.pyc"))
        True

        Recursive wildcard:

        >>> pattern = YTIgnorePattern("**/*.log", Path("/project"))
        >>> pattern.matches(Path("/project/src/debug.log"))
        True
        >>> pattern.matches(Path("/project/debug.log"))
        False  # Requires at least one directory level

        Rooted pattern:

        >>> pattern = YTIgnorePattern("/build", Path("/project"))
        >>> pattern.matches(Path("/project/build"))
        True
        >>> pattern.matches(Path("/project/src/build"))
        False  # Only matches at root

    """

    def __init__(self, pattern: str, base_dir: Path, is_negation: bool = False) -> None:  # noqa: FBT001,FBT002
        """Initialize a pattern.

        Args:
            pattern: The pattern string (without leading !)
            base_dir: Directory where the .ytignore file is located
            is_negation: Whether this is a negation pattern (!)

        """
        self.pattern = pattern
        self.base_dir = base_dir
        self.is_negation = is_negation
        self.is_directory = pattern.endswith("/")
        self.is_rooted = pattern.startswith("/")
        self._regex: re.Pattern[str] = re.compile(r"$^")

        if self.is_directory:
            self.pattern = pattern[:-1]

        if self.is_rooted:
            self.pattern = self.pattern.lstrip("/")

        # Convert to regex-like pattern for matching
        self._compile_pattern()

    def _translate_fnmatch_segment(self, part: str) -> str:
        if part == "*":
            return "[^/]*"
        part_regex = fnmatch.translate(part)
        part_regex = part_regex.lstrip("^").rstrip(r"\Z")
        return part_regex.replace(".*", "[^/]*")

    def _slash_separated_pattern_to_regex(self, pattern: str) -> str:
        parts = pattern.split("/")
        regex_parts = [self._translate_fnmatch_segment(part) for part in parts]
        return "^" + "/".join(regex_parts) + r"\Z"

    def _double_star_leading_to_regex(self, pattern: str) -> str:
        rest_pattern = pattern[3:]
        rest_regex = fnmatch.translate(rest_pattern)
        rest_regex = rest_regex.lstrip("^").rstrip("$").rstrip(r"\Z")
        return f"^.*/{rest_regex}$"

    def _double_star_generic_to_regex(self, pattern: str) -> str:
        marked = pattern.replace("**", "__RECURSIVE_WILDCARD__")
        translated = fnmatch.translate(marked)
        return translated.replace("__RECURSIVE_WILDCARD__", ".*")

    def _pattern_core_to_regex(self, pattern: str) -> str:
        if "**" in pattern:
            if pattern.startswith("**/"):
                return self._double_star_leading_to_regex(pattern)
            return self._double_star_generic_to_regex(pattern)
        if "/" in pattern:
            return self._slash_separated_pattern_to_regex(pattern)
        return fnmatch.translate(pattern)

    def _apply_rooted_anchor(self, pattern: str) -> str:
        stripped = pattern.lstrip("^").rstrip("$").rstrip(r"\Z")
        return f"^{stripped}$"

    def _compile_pattern(self) -> None:
        """Compile pattern into a matching function."""
        pattern = self._pattern_core_to_regex(self.pattern)
        if self.is_rooted:
            pattern = self._apply_rooted_anchor(pattern)
        self._regex = re.compile(pattern)

    def _relative_path_str(self, file_path: Path) -> str | None:
        try:
            rel_path = file_path.relative_to(self.base_dir)
        except ValueError:
            return None
        return str(rel_path).replace("\\", "/")

    def _rooted_blocks_match(self, path_str: str) -> bool:
        return bool(self.is_rooted and "/" in path_str)

    def _directory_segment_matches(self, parts: list[str], idx: int) -> bool:
        parent_path = "/".join(parts[: idx + 1])
        if self._regex.match(parent_path):
            return True
        if self.is_rooted or idx >= len(parts) - 1:
            return False
        return bool(self._regex.match(parts[idx]))

    def _directory_pattern_matches(self, path_str: str) -> bool:
        parts = path_str.split("/")
        return any(self._directory_segment_matches(parts, i) for i in range(len(parts)))

    def _file_pattern_matches(self, path_str: str) -> bool:
        filename_match = (
            "/" not in self.pattern
            and "**" not in self.pattern
            and bool(self._regex.match(Path(path_str).name))
        )
        return bool(self._regex.match(path_str)) or filename_match

    def matches(self, file_path: Path) -> bool:
        """Check if a file path matches this pattern.

        Args:
            file_path: Absolute or relative file path to check

        Returns:
            True if the path matches

        """
        path_str = self._relative_path_str(file_path)
        if path_str is None:
            return False

        if self._rooted_blocks_match(path_str):
            return False

        if self.is_directory:
            return self._directory_pattern_matches(path_str)

        return self._file_pattern_matches(path_str)


def _ytignore_pattern_from_line(line: str, base_dir: Path) -> YTIgnorePattern | None:
    if not line or line.startswith("#"):
        return None
    is_negation = line.startswith("!")
    pattern = line[1:].strip() if is_negation else line
    if not pattern:
        return None
    return YTIgnorePattern(pattern, base_dir, is_negation)


class YTIgnoreMatcher:
    r"""Matches file paths against .ytignore patterns.

    This class loads .ytignore files from the specified directory and all parent
    directories up to the filesystem root, then provides methods to check if files
    should be ignored based on the loaded patterns.

    Patterns follow .gitignore syntax:
    - `*.pyc` - matches any file ending in .pyc
    - `__pycache__/` - matches any __pycache__ directory
    - `**/test_*.py` - matches test_*.py in any subdirectory (but not root)
    - `!important.py` - negates a previous ignore pattern
    - `/build` - matches only at root level (not in subdirectories)
    - `src/*.log` - matches .log files in src/ but not src/subdir/

    Examples:
        Basic usage:

        >>> from pathlib import Path
        >>> matcher = YTIgnoreMatcher(Path("/project"))
        >>> matcher.should_ignore(Path("/project/test.pyc"))
        True
        >>> matcher.should_ignore(Path("/project/main.py"))
        False

        With custom .ytignore file:

        >>> # Create a .ytignore file
        >>> project_dir = Path("/tmp/myproject")
        >>> project_dir.mkdir(exist_ok=True)
        >>> (project_dir / ".ytignore").write_text("*.pyc\\n__pycache__/\\n")
        >>> matcher = YTIgnoreMatcher(project_dir)
        >>> matcher.should_ignore(project_dir / "module.pyc")
        True
        >>> matcher.should_ignore(project_dir / "module.py")
        False

        Negation patterns:

        >>> # Ignore all .log files except important.log
        >>> (project_dir / ".ytignore").write_text("*.log\\n!important.log\\n")
        >>> matcher = YTIgnoreMatcher(project_dir)
        >>> matcher.should_ignore(project_dir / "debug.log")
        True
        >>> matcher.should_ignore(project_dir / "important.log")
        False

    """

    def __init__(self, base_dir: Path) -> None:
        """Initialize matcher.

        Args:
            base_dir: Base directory for file matching

        """
        self.base_dir = base_dir.resolve()
        self.patterns: list[YTIgnorePattern] = []
        self._load_patterns()

    def _load_patterns(self) -> None:
        """Load patterns from .ytignore files in base_dir and parent directories."""
        # Walk up from base_dir to find all .ytignore files
        current_dir = self.base_dir
        found_ytignore_files = []

        # Collect .ytignore files from base_dir up to root
        while current_dir != current_dir.parent:
            ytignore_file = current_dir / ".ytignore"
            if ytignore_file.exists():
                found_ytignore_files.append((ytignore_file, current_dir))
            current_dir = current_dir.parent

        # Load patterns in order (root first, then subdirectories)
        # This ensures proper precedence (later patterns override earlier ones)
        for ytignore_file, pattern_base_dir in reversed(found_ytignore_files):
            self._parse_ytignore_file(ytignore_file, pattern_base_dir)

    def _parse_ytignore_file(self, ytignore_file: Path, base_dir: Path) -> None:
        """Parse a .ytignore file and add patterns.

        Args:
            ytignore_file: Path to .ytignore file
            base_dir: Base directory for patterns in this file

        """
        try:
            with ytignore_file.open(encoding="utf-8") as f:
                for raw_line in f:
                    ignore_pattern = _ytignore_pattern_from_line(
                        raw_line.strip(),
                        base_dir,
                    )
                    if ignore_pattern is not None:
                        self.patterns.append(ignore_pattern)
        except (OSError, UnicodeDecodeError, ValueError) as e:
            # Log error but don't fail - just skip this .ytignore file
            logger = logging.getLogger(__name__)
            logger.warning("Failed to parse .ytignore file %s: %s", ytignore_file, e)

    def should_ignore(self, file_path: Path) -> bool:
        """Check if a file should be ignored.

        Args:
            file_path: Path to the file (can be absolute or relative)

        Returns:
            True if the file should be ignored

        """
        # Always ignore .ytignore files themselves - they're only needed locally
        if file_path.name == ".ytignore":
            return True

        # Resolve to absolute path if relative
        if not file_path.is_absolute():
            file_path = (self.base_dir / file_path).resolve()

        # Check against all patterns in order
        # Later patterns (especially negations) can override earlier ones
        ignored = False
        for pattern in self.patterns:
            if pattern.matches(file_path):
                # Negation un-ignores; regular patterns ignore.
                ignored = not pattern.is_negation

        return ignored


def should_ignore_file(file_path: Path, base_dir: Path) -> bool:
    """Return whether ``file_path`` should be ignored under ``base_dir``.

    This is a shorthand for creating a YTIgnoreMatcher and checking a single file.
    For checking multiple files, create a YTIgnoreMatcher instance directly to
    avoid reloading .ytignore files for each check.

    Args:
        file_path: Path to the file to check
        base_dir: Base directory for .ytignore lookup

    Returns:
        True if the file should be ignored

    Examples:
        >>> from pathlib import Path
        >>> # Assuming .ytignore contains "*.pyc"
        >>> should_ignore_file(Path("/project/test.pyc"), Path("/project"))
        True
        >>> should_ignore_file(Path("/project/test.py"), Path("/project"))
        False

    """
    matcher = YTIgnoreMatcher(base_dir)
    return matcher.should_ignore(file_path)
