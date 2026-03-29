"""Tests for ytjobs.logging.logger and ytjobs.logging.silencer."""

import io
import logging
import sys
from unittest.mock import patch

import pytest

from ytjobs.logging.logger import TextFormatter, get_logger, log_with_extra
from ytjobs.logging.silencer import (
    manage_output,
    redirect_stdout_to_stderr,
    suppress_all_output,
)


def test_text_formatter_includes_bracketed_name_level_and_message() -> None:
    formatter = TextFormatter()
    record = logging.LogRecord(
        name="pkg.mod",
        level=logging.WARNING,
        pathname="x.py",
        lineno=3,
        msg="watch",
        args=(),
        exc_info=None,
    )
    line = formatter.format(record)
    assert (
        "[pkg.mod]" in line and "WARNING: watch" in line and "202" in line
    ), "formatter should stamp name, level, and time"


def test_text_formatter_appends_extra_context_and_formats_collections() -> None:
    formatter = TextFormatter()
    record = logging.LogRecord(
        name="n",
        level=logging.INFO,
        pathname="x.py",
        lineno=1,
        msg="m",
        args=(),
        exc_info=None,
    )
    record.req_id = "a1"
    record.nested = {"k": (1, None)}
    line = formatter.format(record)
    assert (
        "req_id=a1" in line and "nested={" in line and "1" in line and "None" in line
    ), "tuple and None extras format via _format_value"


def test_text_formatter_truncates_long_string_extra_values() -> None:
    formatter = TextFormatter()
    record = logging.LogRecord(
        name="n",
        level=logging.INFO,
        pathname="x.py",
        lineno=1,
        msg="m",
        args=(),
        exc_info=None,
    )
    record.blob = "x" * 120
    line = formatter.format(record)
    assert line.count("x") < 120 and "..." in line, "long extra strings must truncate"


def test_text_formatter_appends_exception_text_when_exc_info_set() -> None:
    formatter = TextFormatter()
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="n",
            level=logging.ERROR,
            pathname="x.py",
            lineno=1,
            msg="fail",
            args=(),
            exc_info=exc_info,
        )
    line = formatter.format(record)
    assert "RuntimeError: boom" in line and "Traceback" in line


def test_get_logger_writes_to_stderr_and_stops_propagation() -> None:
    buf = io.StringIO()
    with patch.object(sys, "stderr", buf):
        log = get_logger("ytjobs_test_logger", level=logging.INFO)
        log.info("hello")
    text = buf.getvalue()
    assert (
        "hello" in text and "INFO" in text and log.propagate is False
    ), "get_logger should attach stderr handler and not propagate"


def test_log_with_extra_adds_fields_visible_through_text_formatter() -> None:
    buf = io.StringIO()
    with patch.object(sys, "stderr", buf):
        log = get_logger("ytjobs_extra", level=logging.DEBUG)
        log_with_extra(log, logging.INFO, "evt", stage="s1", n=2)
    assert (
        "evt" in buf.getvalue()
        and "stage=s1" in buf.getvalue()
        and "n=2" in (buf.getvalue())
    )


def test_manage_output_invalid_mode_raises_value_error() -> None:
    @manage_output(mode="invalid")  # type: ignore[arg-type]
    def _f() -> int:
        return 1

    with pytest.raises(ValueError, match="Invalid mode"):
        _f()


def test_manage_output_suppress_mode_runs_function_without_stdout_noise() -> None:
    seen: list[str] = []

    @manage_output(mode="suppress")
    def _quiet() -> None:
        seen.append("ran")

    buf = io.StringIO()
    with patch.object(sys, "stdout", buf):
        _quiet()
    assert seen == ["ran"] and buf.getvalue() == ""


def test_manage_output_redirect_mode_sends_stdout_to_stderr() -> None:
    @manage_output(mode="redirect")
    def _loud() -> None:
        print("via_redirect", flush=True)

    stderr_capture = io.StringIO()
    with patch.object(sys, "stderr", stderr_capture):
        _loud()
    assert "via_redirect" in stderr_capture.getvalue()


def test_redirect_stdout_to_stderr_routes_print_to_stderr_stream() -> None:
    stderr_capture = io.StringIO()
    with patch.object(sys, "stderr", stderr_capture):
        with redirect_stdout_to_stderr():
            print("marker", flush=True)
    assert "marker" in stderr_capture.getvalue()


def test_suppress_all_output_hides_stdout_print() -> None:
    buf = io.StringIO()
    with patch.object(sys, "stdout", buf):
        with suppress_all_output():
            print("silent")
    assert buf.getvalue() == "", "prints inside suppress must not reach patched stdout"
