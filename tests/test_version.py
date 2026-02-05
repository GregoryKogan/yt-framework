"""Tests for version consistency between package metadata and __version__."""

from importlib import metadata as importlib_metadata

import pytest

import yt_framework


def test_version_consistency():
    """Test that __version__ matches package metadata version."""
    try:
        metadata_version = importlib_metadata.version("yt_framework")
        assert (
            yt_framework.__version__ == metadata_version
        ), f"Version mismatch: __version__={yt_framework.__version__}, metadata={metadata_version}"
    except importlib_metadata.PackageNotFoundError:
        # In editable installs or when metadata isn't available, fallback is used
        # This is acceptable, so we skip the test
        pytest.skip("Package metadata not available (likely editable install)")


def test_version_format():
    """Test that __version__ is a non-empty string."""
    assert isinstance(yt_framework.__version__, str)
    assert len(yt_framework.__version__) > 0