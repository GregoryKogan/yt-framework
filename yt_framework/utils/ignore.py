"""
.ytignore Parser and Matcher
============================

Supports .gitignore-like pattern matching for excluding files from YT uploads.
"""

import re
from pathlib import Path
from typing import List
import fnmatch


class YTIgnorePattern:
    """
    Represents a single .ytignore pattern.

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

    def __init__(self, pattern: str, base_dir: Path, is_negation: bool = False):
        """
        Initialize a pattern.

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

        if self.is_directory:
            self.pattern = pattern[:-1]

        if self.is_rooted:
            self.pattern = self.pattern.lstrip("/")

        # Convert to regex-like pattern for matching
        self._compile_pattern()

    def _compile_pattern(self):
        """Compile pattern into a matching function."""
        # Pattern is already stripped of leading slash and trailing slash
        pattern = self.pattern

        # Handle ** for recursive matching
        if "**" in pattern:
            # Convert ** to regex equivalent
            # ** matches zero or more directories
            # Special case: **/ at start means match in any subdirectory (requires at least one /)
            if pattern.startswith("**/"):
                # **/pattern should match pattern in subdirectories (not at root)
                rest_pattern = pattern[3:]  # Remove '**/'
                # Translate the rest
                rest_regex = fnmatch.translate(rest_pattern)
                # Remove the ^ and $ from rest_regex since we'll add our own
                rest_regex = rest_regex.lstrip("^").rstrip("$").rstrip(r"\Z")
                # Match in subdirectories only (requires at least one /)
                pattern = f"^.*/{rest_regex}$"
            else:
                # ** in middle or end - replace with .* (matches any path)
                pattern = pattern.replace("**", "__RECURSIVE_WILDCARD__")
                # Escape other special characters
                pattern = fnmatch.translate(pattern)
                # Replace our placeholder with regex for any path
                pattern = pattern.replace("__RECURSIVE_WILDCARD__", ".*")
        else:
            # Check if pattern contains path separator
            if "/" in pattern:
                # Pattern with explicit path - use fnmatch but ensure * doesn't match /
                # Split pattern by / and translate each part separately
                parts = pattern.split("/")
                regex_parts = []
                for part in parts:
                    if part == "*":
                        # * should not match path separators
                        regex_parts.append("[^/]*")
                    else:
                        # Use fnmatch for this part, but replace * with [^/]*
                        # to prevent matching across directories
                        part_regex = fnmatch.translate(part)
                        # Remove the anchors that fnmatch adds
                        part_regex = part_regex.lstrip("^").rstrip(r"\Z")
                        # Replace .* (from fnmatch's * translation) with [^/]*
                        # to prevent matching across path separators
                        part_regex = part_regex.replace(".*", "[^/]*")
                        regex_parts.append(part_regex)
                pattern = "^" + "/".join(regex_parts) + r"\Z"
            else:
                # Use fnmatch for standard wildcards
                pattern = fnmatch.translate(pattern)

        # If pattern was rooted (started with /), only match at root level
        # This means the path should NOT have any leading directory components
        if self.is_rooted:
            # Remove any existing anchor
            pattern = pattern.lstrip("^").rstrip("$").rstrip(r"\Z")
            # Anchors: pattern must match from start, and must not have leading dirs
            # Use negative lookahead to ensure no leading path components
            pattern = f"^{pattern}$"

        self._regex = re.compile(pattern)

    def matches(self, file_path: Path) -> bool:
        """
        Check if a file path matches this pattern.

        Args:
            file_path: Absolute or relative file path to check

        Returns:
            True if the path matches
        """
        # Patterns should always be matched relative to their own base_dir
        # (where the .ytignore file is located), not the upload base_dir
        try:
            rel_path = file_path.relative_to(self.base_dir)
        except ValueError:
            # file_path is not under pattern's base_dir, cannot match
            return False

        # Convert to string with forward slashes
        path_str = str(rel_path).replace("\\", "/")

        # If pattern is rooted, only match paths at root level (no subdirectories)
        if self.is_rooted and "/" in path_str:
            # Rooted patterns should not match in subdirectories
            return False

        # For directory patterns, only match if path is a directory
        # Since we're checking files, directory patterns match if any parent matches
        if self.is_directory:
            # Check if any parent directory matches
            parts = path_str.split("/")
            for i in range(len(parts)):
                parent_path = "/".join(parts[: i + 1])
                # Try matching the parent path
                if self._regex.match(parent_path):
                    return True
                # Also try matching just the directory name for non-rooted patterns
                # This allows "build/" to match "subdir/build/"
                if not self.is_rooted and i < len(parts) - 1:
                    dir_name = parts[i]
                    if self._regex.match(dir_name):
                        return True
            return False

        # For file patterns, check the full path
        if self._regex.match(path_str):
            return True

        # Also check just the filename, but only if pattern doesn't contain path separators
        # Patterns like **/*.pyc or subdir/*.py should only match full paths
        if "/" not in self.pattern and "**" not in self.pattern:
            if self._regex.match(Path(path_str).name):
                return True

        return False


class YTIgnoreMatcher:
    """
    Matches file paths against .ytignore patterns.

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

    def __init__(self, base_dir: Path):
        """
        Initialize matcher.

        Args:
            base_dir: Base directory for file matching
        """
        self.base_dir = base_dir.resolve()
        self.patterns: List[YTIgnorePattern] = []
        self._load_patterns()

    def _load_patterns(self):
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

    def _parse_ytignore_file(self, ytignore_file: Path, base_dir: Path):
        """
        Parse a .ytignore file and add patterns.

        Args:
            ytignore_file: Path to .ytignore file
            base_dir: Base directory for patterns in this file
        """
        try:
            with open(ytignore_file, "r", encoding="utf-8") as f:
                for line in f.readlines():
                    # Remove leading and trailing whitespace
                    line = line.strip()

                    # Skip empty lines and comments
                    if not line or line.startswith("#"):
                        continue

                    # Check for negation
                    is_negation = line.startswith("!")
                    if is_negation:
                        pattern = line[1:].strip()
                    else:
                        pattern = line

                    # Skip if pattern is empty after processing
                    if not pattern:
                        continue

                    # Create pattern object
                    ignore_pattern = YTIgnorePattern(pattern, base_dir, is_negation)
                    self.patterns.append(ignore_pattern)
        except Exception as e:
            # Log error but don't fail - just skip this .ytignore file
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to parse .ytignore file {ytignore_file}: {e}")

    def should_ignore(self, file_path: Path) -> bool:
        """
        Check if a file should be ignored.

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
                if pattern.is_negation:
                    # Negation pattern un-ignores
                    ignored = False
                else:
                    # Regular pattern ignores
                    ignored = True

        return ignored


def should_ignore_file(file_path: Path, base_dir: Path) -> bool:
    """
    Convenience function to check if a file should be ignored.

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
