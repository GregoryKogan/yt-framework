"""Tests for yt_framework.operations.table helpers."""

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

from yt_framework.operations.table import download_table, get_row_count, read_table
from yt_framework.yt.client_base import BaseYTClient

_LOG = logging.getLogger("tests.table")


def test_get_row_count_returns_client_count_and_logs_path() -> None:
    yt = MagicMock(spec=BaseYTClient)
    yt.row_count.return_value = 42
    assert get_row_count(yt, "//tmp/t", _LOG) == 42
    yt.row_count.assert_called_once_with("//tmp/t")


def test_read_table_returns_list_materialized_from_client_rows() -> None:
    yt = MagicMock(spec=BaseYTClient)
    yt.read_table.return_value = iter([{"a": 1}, {"a": 2}])
    rows = read_table(yt, "//tmp/t", _LOG)
    assert rows == [{"a": 1}, {"a": 2}]


def test_download_table_writes_jsonl_with_one_line_per_row(tmp_path: Path) -> None:
    yt = MagicMock(spec=BaseYTClient)
    yt.read_table.return_value = [{"x": 1}, {"x": 2}]
    out = tmp_path / "out.jsonl"
    download_table(yt, "//tmp/t", out, _LOG)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(line) for line in lines] == [{"x": 1}, {"x": 2}]
