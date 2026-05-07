"""Tests for yt_framework.operations.tar_command_wiring helpers."""

import logging

from yt_framework.operations.tar_command_wiring import (
    bootstrap_shell_run_wrapper,
    map_reduce_wrapper_names,
    reduce_wrapper_name,
    wrap_bootstrap_as_bash_c,
)

_LOG = logging.getLogger("tests.tar_wiring")


def test_bootstrap_shell_run_wrapper_extracts_archive_and_runs_script() -> None:
    cmd = bootstrap_shell_run_wrapper("code.tgz", "run.sh", _LOG)
    assert "tar -xzf code.tgz" in cmd and "./run.sh" in cmd, (
        "bootstrap should unpack archive then execute wrapper"
    )


def test_wrap_bootstrap_as_bash_c_escapes_single_quotes_for_shell() -> None:
    inner = "echo 'hi'"
    wrapped = wrap_bootstrap_as_bash_c(inner)
    assert wrapped.startswith("bash -c '")
    assert wrapped.endswith("'")
    assert "'\"'\"'" in wrapped, "single quotes must be escaped for bash -c"


def test_map_reduce_wrapper_names_use_stage_name_prefix() -> None:
    m, r = map_reduce_wrapper_names("train")
    assert m == "operation_wrapper_train_map_reduce_mapper.sh"
    assert r == "operation_wrapper_train_map_reduce_reducer.sh"


def test_reduce_wrapper_name_follows_reduce_convention() -> None:
    assert reduce_wrapper_name("train") == "operation_wrapper_train_reduce.sh"
