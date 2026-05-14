"""Tests for yt_framework.yt.clients._client_split.dev_operation.DevOperation."""

from yt_framework.yt.clients._client_split.dev_operation import DevOperation


def test_dev_operation_get_error_returns_none_on_success() -> None:
    op = DevOperation(0)
    assert op.get_error() is None
