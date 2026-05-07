"""Tests for ytjobs.mapper.mappers stdin-driven map paths (StringIO stdin)."""

import io
import json
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from ytjobs.mapper.mappers import BatchMapper, StreamMapper


def test_batch_mapper_init_stores_numeric_batch_size() -> None:
    assert BatchMapper(7).batch_size == 7


def test_batch_mapper_init_allows_none_batch_size() -> None:
    assert BatchMapper(None).batch_size is None


def test_stream_mapper_map_writes_json_results_for_valid_stdin_lines(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"a": 1}\n'))

    def _emit(row: Any, **_kw: Any) -> Iterator[dict[str, int]]:
        yield {"out": row["a"]}

    StreamMapper().map(_emit, redirect_processing_output=False)
    assert json.loads(capsys.readouterr().out.strip()) == {"out": 1}


def test_stream_mapper_map_raises_and_logs_json_error_when_processing_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"k": 1}\n'))

    def _boom(_row: Any, **_kw: Any) -> Iterator[dict[str, str]]:
        msg = "mapper boom"
        raise RuntimeError(msg)
        yield  # pragma: no cover

    with pytest.raises(RuntimeError, match="mapper boom"):
        StreamMapper().map(_boom, redirect_processing_output=False)
    err_line = capsys.readouterr().err.strip().splitlines()[-1]
    payload = json.loads(err_line)
    assert "Processing failed" in payload["error"]
    assert payload.get("row") == '{"k": 1}'


def test_batch_mapper_map_with_none_batch_size_passes_all_rows_to_processor(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"x": 1}\n{"x": 2}\n'))

    def _sum_batch(rows: list[dict[str, int]], **_kw: Any) -> Iterator[dict[str, int]]:
        assert rows == [{"x": 1}, {"x": 2}]
        yield {"n": len(rows)}

    BatchMapper(None).map(_sum_batch, redirect_processing_output=False)
    assert json.loads(capsys.readouterr().out.strip()) == {"n": 2}


def test_batch_mapper_map_with_batch_size_emits_one_output_line_per_batch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"i": 1}\n{"i": 2}\n'))

    def _each(rows: list[dict[str, int]], **_kw: Any) -> Iterator[dict[str, int]]:
        yield {"s": sum(x["i"] for x in rows)}

    BatchMapper(1).map(_each, redirect_processing_output=False)
    lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line) for line in lines] == [{"s": 1}, {"s": 2}]


def test_batch_mapper_map_skips_processor_when_stdin_has_only_blank_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[object] = []

    def _track(rows: Any, **_kw: Any) -> Iterator[Any]:
        seen.append(rows)
        yield from ()

    monkeypatch.setattr(sys, "stdin", io.StringIO("\n\n  \n"))
    BatchMapper(None).map(_track, redirect_processing_output=False)
    assert seen == [], "empty stdin must not invoke processing_func"


def test_stream_mapper_map_skips_stdin_lines_that_are_not_valid_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"a": 1}\nnot-json\n{"a": 2}\n'))

    def _passthrough(row: Any, **_kw: Any) -> Iterator[dict[str, int]]:
        yield {"a": row["a"]}

    StreamMapper().map(_passthrough, redirect_processing_output=False)
    lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line) for line in lines] == [{"a": 1}, {"a": 2}]


def test_stream_mapper_map_skips_whitespace_only_stdin_lines(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"a": 1}\n\n  \n{"a": 2}\n'))

    def _passthrough(row: Any, **_kw: Any) -> Iterator[dict[str, int]]:
        yield {"a": row["a"]}

    StreamMapper().map(_passthrough, redirect_processing_output=False)
    lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line) for line in lines] == [{"a": 1}, {"a": 2}]


def test_batch_mapper_map_in_batch_mode_skips_blank_and_invalid_json_lines(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO('{"i": 1}\n\nnot-json\n{"i": 2}\n'),
    )

    def _sum(rows: list[dict[str, int]], **_kw: Any) -> Iterator[dict[str, int]]:
        yield {"s": sum(r["i"] for r in rows)}

    BatchMapper(2).map(_sum, redirect_processing_output=False)
    assert json.loads(capsys.readouterr().out.strip()) == {"s": 3}


def test_batch_mapper_map_flushes_trailing_partial_batch_after_full_batches(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO('{"i": 1}\n{"i": 2}\n{"i": 3}\n'),
    )

    def _sum(rows: list[dict[str, int]], **_kw: Any) -> Iterator[dict[str, int]]:
        yield {"s": sum(r["i"] for r in rows)}

    BatchMapper(2).map(_sum, redirect_processing_output=False)
    lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line) for line in lines] == [{"s": 3}, {"s": 3}]


def test_batch_mapper_process_in_batches_raises_when_batch_size_not_set() -> None:
    mapper = BatchMapper(None)
    with pytest.raises(ValueError, match="Batch size must be set"):
        mapper._process_in_batches(lambda _rows, **_k: iter(()), True)


def test_batch_mapper_map_logs_batch_processing_failed_for_all_rows_mode(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"x": 1}\n'))

    def _boom(rows: list[dict[str, int]], **_kw: Any) -> Iterator[dict[str, str]]:
        msg = "all-rows boom"
        raise RuntimeError(msg)
        yield  # pragma: no cover

    with pytest.raises(RuntimeError, match="all-rows boom"):
        BatchMapper(None).map(_boom, redirect_processing_output=False)
    err_line = capsys.readouterr().err.strip().splitlines()[-1]
    assert "Batch processing failed" in json.loads(err_line)["error"]


def test_batch_mapper_map_logs_per_batch_failure_when_batch_size_set(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"i": 0}\n'))

    def _boom(rows: list[dict[str, int]], **_kw: Any) -> Iterator[dict[str, str]]:
        msg = "per-batch boom"
        raise RuntimeError(msg)
        yield  # pragma: no cover

    with pytest.raises(RuntimeError, match="per-batch boom"):
        BatchMapper(1).map(_boom, redirect_processing_output=False)
    err_line = capsys.readouterr().err.strip().splitlines()[-1]
    payload = json.loads(err_line)
    assert "Batch 0 processing failed" in payload["error"]
    assert payload["batch_size"] == 1
