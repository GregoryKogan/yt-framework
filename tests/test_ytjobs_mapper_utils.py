"""Tests for ytjobs.mapper.utils stream helpers, parse_json_line, and log_error."""

import io
import json
import sys

import pytest

from ytjobs.mapper.utils import (
    log_error,
    parse_json_line,
    process_and_write_results,
    read_input_rows,
)


def test_parse_json_line_returns_decoded_object() -> None:
    assert parse_json_line('{"a": 1}') == {"a": 1}


def test_parse_json_line_returns_none_on_invalid_json_and_writes_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert parse_json_line("not json") is None
    err = capsys.readouterr().err
    payload = json.loads(err.strip())
    assert "error" in payload and payload.get("row") == "not json"


def test_log_error_writes_json_line_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    log_error({"k": "v"})
    assert json.loads(capsys.readouterr().err.strip()) == {"k": "v"}


def test_read_input_rows_skips_blank_lines_and_yields_parsed_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO('{"a": 1}\n\n{"b": 2}\n'),
    )
    assert list(read_input_rows()) == [{"a": 1}, {"b": 2}]


def test_read_input_rows_writes_json_error_to_stderr_for_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO("not-json\n"))
    assert list(read_input_rows()) == []
    payload = json.loads(capsys.readouterr().err.strip())
    assert "error" in payload and payload.get("row") == "not-json"


def test_process_and_write_results_writes_each_yield_as_json_line_to_stdout(
    capsys: pytest.CaptureFixture[str],
) -> None:

    def _emit(data: str, **_kw: object) -> object:
        yield {"d": data}
        yield {"n": 2}

    process_and_write_results(_emit, "x", redirect_output=False)
    out_lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line) for line in out_lines] == [{"d": "x"}, {"n": 2}]


def test_process_and_write_results_sends_processing_prints_to_stderr_when_redirecting(
    capsys: pytest.CaptureFixture[str],
) -> None:

    def _noisy(_data: object, **_kw: object) -> object:
        print("mapper-side")
        yield {"ok": True}

    process_and_write_results(_noisy, None, redirect_output=True)
    captured = capsys.readouterr()
    assert "mapper-side" in captured.err and json.loads(captured.out.strip()) == {
        "ok": True
    }, "processing stdout must go to stderr while JSON rows use real stdout"
