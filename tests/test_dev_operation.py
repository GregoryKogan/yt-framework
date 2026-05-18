"""Tests for yt_framework.yt.clients._client_split.dev_operation.DevOperation."""

from yt_framework.yt.clients._client_split.dev_operation import DevOperation


def test_dev_operation_get_error_returns_none_on_success() -> None:
    op = DevOperation(0)
    assert op.get_error() is None


def test_dev_operation_get_error_includes_leg_message_and_stderr_hint() -> None:
    op = DevOperation(3, "Stderr written to /tmp/x.log", leg_name="Reducer")
    assert op.get_error() == "Reducer exited with code 3. Stderr written to /tmp/x.log"


def test_dev_operation_get_error_returns_leg_message_when_no_stderr() -> None:
    op = DevOperation(2, leg_name="Mapper")
    assert op.get_error() == "Mapper exited with code 2"
