"""Unit tests for yt_framework.yt.support._client_dev_runtime helpers."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from yt_framework.yt.support._client_dev_runtime import (
    dev_copy_output_to_table,
    dev_resolve_sort_keys,
    dev_sort_jsonl_file,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_dev_resolve_sort_keys_prefers_sort_by_when_set() -> None:
    keys = dev_resolve_sort_keys(reduce_by=["a"], sort_by=["b", "a"])
    assert keys == ["b", "a"], "sort_by wins when non-empty"


def test_dev_resolve_sort_keys_falls_back_to_reduce_by() -> None:
    keys = dev_resolve_sort_keys(reduce_by=["k"], sort_by=None)
    assert keys == ["k"], "reduce_by used when sort_by absent"


def test_dev_resolve_sort_keys_empty_sort_by_uses_reduce_by() -> None:
    keys = dev_resolve_sort_keys(reduce_by=["k"], sort_by=[])
    assert keys == ["k"], "empty sort_by list falls back to reduce_by"


def test_dev_sort_jsonl_file_missing_sort_key_sorts_before_present(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_text(
        '{"k":"b"}\n{}\n{"k":"a"}\n',
        encoding="utf-8",
    )
    dev_sort_jsonl_file(path, ["k"])
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [{}, {"k": "a"}, {"k": "b"}], (
        "rows missing sort key sort before rows that define the key"
    )


def test_dev_sort_jsonl_file_mixed_types_do_not_raise(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_text(
        '{"k": 2}\n{"k": "1"}\n{"k": 10}\n',
        encoding="utf-8",
    )
    dev_sort_jsonl_file(path, ["k"])
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [{"k": "1"}, {"k": 10}, {"k": 2}], (
        "mixed types sort by JSON canonical string without TypeError"
    )


def test_dev_sort_jsonl_file_multi_key_missing_secondary_key(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_text(
        '{"k":"a","v":2}\n{"k":"a"}\n{"k":"a","v":1}\n',
        encoding="utf-8",
    )
    dev_sort_jsonl_file(path, ["k", "v"])
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [{"k": "a"}, {"k": "a", "v": 1}, {"k": "a", "v": 2}], (
        "missing secondary sort key sorts before rows that define it"
    )


def test_dev_sort_jsonl_file_orders_rows_by_keys(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_text(
        '{"k":"b","v":1}\n{"k":"a","v":2}\n{"k":"a","v":1}\n',
        encoding="utf-8",
    )
    dev_sort_jsonl_file(path, ["k"])
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [r["k"] for r in rows] == ["a", "a", "b"], "stable sort by reduce key"


def test_dev_sort_jsonl_file_noop_when_sort_keys_empty(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    original = '{"k":"b"}\n{"k":"a"}\n'
    path.write_text(original, encoding="utf-8")
    dev_sort_jsonl_file(path, [])
    assert path.read_text(encoding="utf-8") == original


def test_dev_copy_output_to_table_skips_on_nonzero_returncode(tmp_path: Path) -> None:
    sandbox_output = tmp_path / "sand.jsonl"
    sandbox_output.write_text('{"x": 1}\n', encoding="utf-8")
    out_table = tmp_path / "out.jsonl"
    dev_copy_output_to_table(
        proc_returncode=1,
        sandbox_output=sandbox_output,
        output_table_local_path=out_table,
    )
    assert not out_table.exists(), "failed leg must not publish output"


def test_dev_sort_jsonl_file_uses_sort_by_columns(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_text(
        '{"k":"x","ord":2}\n{"k":"x","ord":1}\n',
        encoding="utf-8",
    )
    dev_sort_jsonl_file(path, ["ord"])
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [r["ord"] for r in rows] == [1, 2]
