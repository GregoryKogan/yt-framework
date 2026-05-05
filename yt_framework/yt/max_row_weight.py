"""
Max row weight helpers for YT operations and YQL queries.
"""

import re
from typing import Optional

# Cluster-side ceiling matches DEFAULT (explicit overrides must stay within this cap).
MAX_ALLOWED_ROW_WEIGHT = "128M"
DEFAULT_MAX_ROW_WEIGHT = MAX_ALLOWED_ROW_WEIGHT

_SUFFIX_MULT_KIB = {"k": 1024, "m": 1024**2, "g": 1024**3}

_MAX_ROW_WEIGHT_PRAGMA_RE = re.compile(
    r'^\s*PRAGMA\s+yt\.MaxRowWeight\s*=\s*["\'][^"\']+["\']\s*;\s*$',
    flags=re.IGNORECASE | re.MULTILINE,
)
_PRAGMA_MAX_ROW_WEIGHT_VALUE_RE = re.compile(
    r'PRAGMA\s+yt\.MaxRowWeight\s*=\s*["\']([^"\']+)["\']',
    flags=re.IGNORECASE,
)


def parse_max_row_weight_to_bytes(value: str) -> int:
    """Parse a max-row-weight token to a byte count.

    Suffixes ``K``, ``M``, and ``G`` are **binary** (1024-based): KiB, MiB, GiB.

    Args:
        value: Token such as ``128M``, ``64m``, or a bare decimal integer (bytes).

    Returns:
        Size in bytes.

    Raises:
        ValueError: If the format is not recognized.
    """
    s = value.strip()
    if not s:
        raise ValueError("max_row_weight must be non-empty")
    last = s[-1].lower()
    if last in _SUFFIX_MULT_KIB:
        num_part = s[:-1].strip()
        if not num_part.isdigit():
            raise ValueError(f"invalid max_row_weight numeric part in {value!r}")
        return int(num_part) * _SUFFIX_MULT_KIB[last]
    if s.isdigit():
        return int(s)
    raise ValueError(f"unrecognized max_row_weight format: {value!r}")


MAX_ALLOWED_BYTES = parse_max_row_weight_to_bytes(MAX_ALLOWED_ROW_WEIGHT)


def _canonical_max_row_weight_string(raw: str) -> str:
    s = raw.strip()
    last = s[-1].lower()
    if last in _SUFFIX_MULT_KIB:
        num = int(s[:-1].strip())
        return f"{num}{last.upper()}"
    return str(int(s))


def validate_max_row_weight(value: Optional[str]) -> str:
    """Validate max row weight against the cluster maximum and return canonical form.

    Args:
        value: Explicit override, or ``None`` for the project default.

    Returns:
        Canonical ``max_row_weight`` string (normalized suffix casing for ``K``/``M``/``G``).

    Raises:
        ValueError: If the value exceeds ``MAX_ALLOWED_ROW_WEIGHT`` or is malformed.
    """
    if value is None:
        return DEFAULT_MAX_ROW_WEIGHT
    resolved = value.strip()
    if not resolved:
        raise ValueError("max_row_weight override must be non-empty")
    n = parse_max_row_weight_to_bytes(resolved)
    if n > MAX_ALLOWED_BYTES:
        raise ValueError(
            f"max_row_weight {value!r} exceeds cluster maximum "
            f"{MAX_ALLOWED_ROW_WEIGHT} ({MAX_ALLOWED_BYTES} bytes)"
        )
    return _canonical_max_row_weight_string(resolved)


def resolve_max_row_weight(max_row_weight: Optional[str] = None) -> str:
    """Return explicit row weight value or project default (validated)."""
    return validate_max_row_weight(max_row_weight)


def build_max_row_weight_pragma(max_row_weight: Optional[str] = None) -> str:
    """Build YQL pragma for max row weight."""
    return f'PRAGMA yt.MaxRowWeight = "{resolve_max_row_weight(max_row_weight)}";'


def ensure_max_row_weight_pragma(
    query: str,
    max_row_weight: Optional[str] = None,
) -> str:
    """Ensure query has a single max-row-weight pragma.

    If query already contains ``PRAGMA yt.MaxRowWeight`` and no explicit override is
    requested, the embedded value is validated (invalid cluster values raise).

    If query already contains the pragma and an override is requested, the first
    matching pragma line is replaced with the validated override.
    """
    if max_row_weight is not None:
        validated = validate_max_row_weight(max_row_weight)
        match = _MAX_ROW_WEIGHT_PRAGMA_RE.search(query)
        if match:
            return _MAX_ROW_WEIGHT_PRAGMA_RE.sub(
                build_max_row_weight_pragma(validated),
                query,
                count=1,
            )
        return f"{build_max_row_weight_pragma(validated)}\n{query}"

    vm = _PRAGMA_MAX_ROW_WEIGHT_VALUE_RE.search(query)
    if vm:
        validate_max_row_weight(vm.group(1))
        return query
    return f"{build_max_row_weight_pragma(None)}\n{query}"
