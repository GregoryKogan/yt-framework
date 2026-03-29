"""Tests for yt_framework.utils.logging."""

import logging
import sys

import pytest

from yt_framework.utils.logging import (
    ColoredFormatter,
    log_config,
    log_header,
    log_operation,
    log_success,
    setup_logging,
)


def test_colored_formatter_prefixes_warning_level_with_icon() -> None:
    fmt = ColoredFormatter("%(levelname)s | %(message)s")
    record = logging.LogRecord("n", logging.WARNING, __file__, 0, "msg", (), None)
    text = fmt.format(record)
    assert (
        "⚠️" in text and "WARNING" in text
    ), "expected warning icon and level in output"


def test_colored_formatter_prefixes_error_level_with_icon() -> None:
    fmt = ColoredFormatter("%(levelname)s | %(message)s")
    record = logging.LogRecord("n", logging.ERROR, __file__, 0, "msg", (), None)
    text = fmt.format(record)
    assert "✗" in text and "ERROR" in text, "expected error icon and level in output"


def test_colored_formatter_leaves_info_level_without_icon_prefix() -> None:
    fmt = ColoredFormatter("%(levelname)s | %(message)s")
    record = logging.LogRecord("n", logging.INFO, __file__, 0, "msg", (), None)
    text = fmt.format(record)
    assert "INFO" in text and "🔍" not in text, "INFO must not use DEBUG icon"


def test_setup_logging_uses_colored_formatter_when_stdout_is_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    log = setup_logging(name="tests.logging.tty")
    handler = log.handlers[0]
    assert isinstance(handler.formatter, ColoredFormatter), "TTY should select colors"


def test_log_header_with_context_writes_bracketed_title(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = logging.getLogger("tests.logging.header")
    log.setLevel(logging.INFO)
    caplog.set_level(logging.INFO, logger="tests.logging.header")
    with caplog.at_level(logging.INFO, logger="tests.logging.header"):
        log_header(log, "Title", "ctx")
    assert "[Title] ctx" in caplog.text, "expected bracket title and context"


def test_log_operation_prefixes_arrow(caplog: pytest.LogCaptureFixture) -> None:
    log = logging.getLogger("tests.logging.op")
    log.setLevel(logging.INFO)
    caplog.set_level(logging.INFO, logger="tests.logging.op")
    with caplog.at_level(logging.INFO, logger="tests.logging.op"):
        log_operation(log, "do thing")
    assert "→ do thing" in caplog.text, "expected arrow-prefixed operation line"


def test_log_success_prefixes_checkmark(caplog: pytest.LogCaptureFixture) -> None:
    log = logging.getLogger("tests.logging.ok")
    log.setLevel(logging.INFO)
    caplog.set_level(logging.INFO, logger="tests.logging.ok")
    with caplog.at_level(logging.INFO, logger="tests.logging.ok"):
        log_success(log, "done")
    assert "✓ done" in caplog.text, "expected checkmark-prefixed success line"


def test_log_config_masks_key_like_column_in_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = logging.getLogger("tests.logging.cfgkey")
    log.setLevel(logging.INFO)
    caplog.set_level(logging.INFO, logger="tests.logging.cfgkey")
    with caplog.at_level(logging.INFO, logger="tests.logging.cfgkey"):
        log_config(log, {"api_key": "secret12345"}, title="Cfg")
    assert (
        "***2345" in caplog.text
    ), "sensitive key values should mask to last four chars"


def test_log_config_shows_not_set_for_empty_secret_value(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = logging.getLogger("tests.logging.cfgempty")
    log.setLevel(logging.INFO)
    caplog.set_level(logging.INFO, logger="tests.logging.cfgempty")
    with caplog.at_level(logging.INFO, logger="tests.logging.cfgempty"):
        log_config(log, {"my_secret": ""}, title="S")
    assert "(not set)" in caplog.text, "empty secret-like value should not leak"


def test_setup_logging_writes_plain_formatter_when_stdout_not_tty(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    log = setup_logging(name="tests.logging.notty")
    log.info("hello")
    captured = capsys.readouterr()
    assert (
        "hello" in captured.out and "\033[" not in captured.out
    ), "no ANSI when not a TTY"
