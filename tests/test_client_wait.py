"""Tests for yt_framework.yt.clients.client_wait.ClientOperationWaitMixin."""

import logging
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from yt_framework.yt.clients.client_wait import ClientOperationWaitMixin


class _WaitHarness(ClientOperationWaitMixin):
    def __init__(self) -> None:
        self.logger = logging.getLogger("tests.client_wait.harness")
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())


def test_wait_for_operation_returns_false_when_wait_raises() -> None:
    op = MagicMock()
    op.wait.side_effect = RuntimeError("boom")
    h = _WaitHarness()
    assert h.wait_for_operation(op) is False


def test_wait_for_operation_returns_false_when_state_not_completed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    op = MagicMock()
    op.get_state.return_value = "failed"
    op.get_error.return_value = "err"
    h = _WaitHarness()
    caplog.set_level(logging.ERROR)
    assert h.wait_for_operation(op) is False


def test_wait_for_operation_logs_operation_error_when_get_error_raises() -> None:
    op = MagicMock()
    op.get_state.return_value = "failed"
    op.get_error.side_effect = KeyError("missing")
    h = _WaitHarness()
    assert h.wait_for_operation(op) is False


def test_log_error_from_exception_logs_stderr_lines_when_present(
    caplog: pytest.LogCaptureFixture,
) -> None:
    h = _WaitHarness()
    caplog.set_level(logging.ERROR)

    class _FakeYtAttrsError(Exception):
        attributes: ClassVar[dict[str, object]] = {
            "stderrs": [{"error": {"attributes": {"stderr": "a\\nb"}}}],
        }

    h._log_error_from_exception(_FakeYtAttrsError())
    assert "Job stderr:" in caplog.text and "  a" in caplog.text


def test_log_error_from_exception_suppresses_secondary_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    h = _WaitHarness()
    caplog.set_level(logging.ERROR)
    with patch.object(
        h,
        "_log_stderr_lines_from_attributes",
        side_effect=RuntimeError("inner"),
    ):
        h._log_error_from_exception(ValueError("outer"))
    assert "Error:" in caplog.text
