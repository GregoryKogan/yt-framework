"""Tests for S3Client.parse_s3_uri."""

import pytest

from ytjobs.s3 import S3Client


def test_parse_s3_uri_ok() -> None:
    assert S3Client.parse_s3_uri("s3://my-bucket/path/to/key") == (
        "my-bucket",
        "path/to/key",
    )


def test_parse_s3_uri_bad() -> None:
    with pytest.raises(ValueError):
        S3Client.parse_s3_uri("https://example.com/x")
