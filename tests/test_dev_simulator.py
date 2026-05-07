"""Tests for yt_framework.yt.dev_simulator helpers and YQL conversion."""

import logging
from pathlib import Path

import pytest

from yt_framework.yt.dev_simulator import (
    DuckDBSimulator,
    extract_output_table,
    extract_table_references,
)


def test_extract_output_table_returns_path_from_insert_into() -> None:
    yql = "INSERT INTO `//tmp/home/out` WITH TRUNCATE SELECT * FROM `//tmp/home/in`;"
    assert extract_output_table(yql) == "//tmp/home/out"


def test_extract_output_table_returns_none_when_no_insert() -> None:
    assert extract_output_table("SELECT 1;") is None


def test_extract_table_references_omits_insert_target_table() -> None:
    yql = "INSERT INTO `//tmp/out` WITH TRUNCATE SELECT * FROM `//tmp/in/table`;"
    assert extract_table_references(yql) == ["//tmp/in/table"]


def test_yql_to_sql_substitutes_loaded_yt_paths() -> None:
    logger = logging.getLogger("tests.dev_simulator")
    logger.addHandler(logging.NullHandler())
    sim = DuckDBSimulator(Path("/tmp"), logger)
    sim.loaded_tables["//cluster/input"] = "yt_input"
    sql, output = sim.yql_to_sql(
        "INSERT INTO `//cluster/output` WITH TRUNCATE SELECT * FROM `//cluster/input`;"
    )
    assert output == "//cluster/output"
    assert "yt_input" in sql
    assert "`//cluster/input`" not in sql
    sim.close()


def _null_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


def test_yql_to_sql_appends_semicolon_when_transformed_sql_has_none(
    tmp_path: Path,
) -> None:
    sim = DuckDBSimulator(tmp_path, _null_logger("tests.dev_simulator.semi"))
    sql, out = sim.yql_to_sql("SELECT 1")
    assert out is None and sql.endswith(";"), "bare SELECT must end with semicolon"
    sim.close()


def test_load_table_registers_dummy_table_when_jsonl_missing(tmp_path: Path) -> None:
    sim = DuckDBSimulator(tmp_path, _null_logger("tests.dev_simulator.missing"))
    missing = tmp_path / "nope.jsonl"
    name = sim.load_table("//tmp/my_table", missing)
    assert name == "yt_my_table"
    assert sim.loaded_tables["//tmp/my_table"] == "yt_my_table"
    sim.close()


def test_load_table_loads_rows_from_valid_jsonl(tmp_path: Path) -> None:
    sim = DuckDBSimulator(tmp_path, _null_logger("tests.dev_simulator.jsonl"))
    data = tmp_path / "rows.jsonl"
    data.write_text('{"x": 1}\n{"x": 2}\n', encoding="utf-8")
    table = sim.load_table("//cluster/input", data)
    rows = sim.execute_query(f"SELECT * FROM {table} ORDER BY x")
    assert rows == [{"x": 1}, {"x": 2}]
    sim.close()


def test_load_table_raises_when_jsonl_is_not_valid_json(tmp_path: Path) -> None:
    sim = DuckDBSimulator(tmp_path, _null_logger("tests.dev_simulator.badjson"))
    bad = tmp_path / "bad.jsonl"
    bad.write_text("not-json\n", encoding="utf-8")
    with pytest.raises(Exception):
        sim.load_table("//tmp/bad", bad)
    sim.close()


def test_execute_query_raises_when_sql_is_invalid(tmp_path: Path) -> None:
    sim = DuckDBSimulator(tmp_path, _null_logger("tests.dev_simulator.badsql"))
    with pytest.raises(Exception):
        sim.execute_query("SELECT * FROM no_such_table_xyz;")
    sim.close()


def test_execute_query_returns_empty_list_when_select_matches_no_rows(
    tmp_path: Path,
) -> None:
    sim = DuckDBSimulator(tmp_path, _null_logger("tests.dev_simulator.empty"))
    data = tmp_path / "rows.jsonl"
    data.write_text('{"x": 1}\n', encoding="utf-8")
    table = sim.load_table("//tmp/t", data)
    assert sim.execute_query(f"SELECT * FROM {table} WHERE x > 99") == []
    sim.close()


def test_execute_yql_returns_select_results_and_output_path(tmp_path: Path) -> None:
    sim = DuckDBSimulator(tmp_path, _null_logger("tests.dev_simulator.yql"))
    data = tmp_path / "in.jsonl"
    data.write_text('{"k": "a"}\n', encoding="utf-8")
    sim.load_table("//tmp/in", data)
    yql = "INSERT INTO `//tmp/out` WITH TRUNCATE SELECT k FROM `//tmp/in`;"
    rows, out = sim.execute_yql(yql)
    assert (rows, out) == ([{"k": "a"}], "//tmp/out")
    sim.close()


def test_validated_table_identifier_rejects_non_whitelisted_names(
    tmp_path: Path,
) -> None:
    sim = DuckDBSimulator(tmp_path, _null_logger("tests.dev_simulator.ident"))
    with pytest.raises(ValueError, match="invalid DuckDB table identifier"):
        sim._validated_table_identifier("yt_; DROP TABLE")
    sim.close()
