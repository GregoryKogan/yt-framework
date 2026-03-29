"""Tests for yt_framework.operations.s3."""

import logging
from unittest.mock import MagicMock

import pytest

from yt_framework.operations.s3 import list_s3_files, save_s3_paths_to_table
from yt_framework.yt.client_base import BaseYTClient


def _log(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


def test_list_s3_files_returns_keys_from_s3_client() -> None:
    s3 = MagicMock()
    s3.list_files.return_value = ["p1", "p2"]
    log = _log("t.s3.ops1")
    out = list_s3_files(s3, "buck", "pre/", log)
    assert out == ["p1", "p2"]
    s3.list_files.assert_called_once_with(
        bucket="buck", prefix="pre/", extension=None, max_files=None
    )


def test_list_s3_files_passes_extension_and_max_files() -> None:
    s3 = MagicMock()
    s3.list_files.return_value = []
    log = _log("t.s3.ops2")
    list_s3_files(s3, "b", "p", log, extension="txt", max_files=5)
    s3.list_files.assert_called_once_with(
        bucket="b", prefix="p", extension="txt", max_files=5
    )


def test_list_s3_files_logs_sample_paths_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    s3 = MagicMock()
    s3.list_files.return_value = ["a", "b"]
    caplog.set_level(logging.DEBUG, logger="t.s3.ops3")
    list_s3_files(s3, "buck", "", logging.getLogger("t.s3.ops3"))
    assert any("Sample paths" in r.message for r in caplog.records)


def test_list_s3_files_logs_ellipsis_when_more_than_three_paths(
    caplog: pytest.LogCaptureFixture,
) -> None:
    s3 = MagicMock()
    s3.list_files.return_value = ["w", "x", "y", "z"]
    caplog.set_level(logging.DEBUG, logger="t.s3.ops4")
    list_s3_files(s3, "buck", "", logging.getLogger("t.s3.ops4"))
    assert any("and 1 more" in r.message for r in caplog.records)


def test_save_s3_paths_to_table_writes_rows_via_yt_client() -> None:
    yt = MagicMock(spec=BaseYTClient)
    log = _log("t.s3.ops5")
    save_s3_paths_to_table(yt, "myb", ["k/a", "k/b"], "//out/t", log)
    yt.write_table.assert_called_once_with(
        table_path="//out/t",
        rows=[
            {"bucket": "myb", "path": "k/a"},
            {"bucket": "myb", "path": "k/b"},
        ],
        append=False,
    )
