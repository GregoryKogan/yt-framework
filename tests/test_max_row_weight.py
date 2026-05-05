"""Unit tests for max_row_weight validation and pragma helpers."""

import pytest

from yt_framework.yt.max_row_weight import (
    MAX_ALLOWED_BYTES,
    ensure_max_row_weight_pragma,
    parse_max_row_weight_to_bytes,
    validate_max_row_weight,
)


def test_parse_max_row_weight_to_bytes_uses_binary_m_suffix() -> None:
    assert parse_max_row_weight_to_bytes("1M") == 1024**2


def test_validate_max_row_weight_none_returns_default() -> None:
    assert validate_max_row_weight(None) == "128M"


def test_validate_max_row_weight_normalizes_m_suffix_casing() -> None:
    assert validate_max_row_weight("128m") == "128M"


def test_validate_max_row_weight_rejects_256m() -> None:
    with pytest.raises(ValueError, match="exceeds cluster maximum"):
        validate_max_row_weight("256M")


def test_validate_max_row_weight_rejects_129m() -> None:
    with pytest.raises(ValueError, match="exceeds cluster maximum"):
        validate_max_row_weight("129M")


def test_validate_max_row_weight_rejects_999g() -> None:
    with pytest.raises(ValueError, match="exceeds cluster maximum"):
        validate_max_row_weight("999G")


def test_validate_max_row_weight_accepts_64m() -> None:
    assert validate_max_row_weight("64M") == "64M"


def test_validate_max_row_weight_accepts_1m() -> None:
    assert validate_max_row_weight("1M") == "1M"


def test_validate_max_row_weight_accepts_bare_bytes_at_cap() -> None:
    assert validate_max_row_weight(str(MAX_ALLOWED_BYTES)) == str(MAX_ALLOWED_BYTES)


def test_ensure_max_row_weight_pragma_validates_embedded_value_when_no_override() -> (
    None
):
    with pytest.raises(ValueError, match="exceeds cluster maximum"):
        ensure_max_row_weight_pragma('PRAGMA yt.MaxRowWeight = "512M";\nSELECT 1')


def test_ensure_max_row_weight_pragma_leaves_valid_embedded_pragma_unchanged() -> None:
    q = 'PRAGMA yt.MaxRowWeight = "64M";\nSELECT 1'
    assert ensure_max_row_weight_pragma(q) == q
