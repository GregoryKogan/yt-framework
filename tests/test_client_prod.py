"""Contract tests for yt_framework.yt.client_prod.YTProdClient (no cluster I/O)."""

import logging
from pathlib import Path
from typing import Any, List, Tuple
from unittest.mock import MagicMock, call

import pytest

from yt.wrapper import TypedJob  # pyright: ignore[reportMissingImports]
from yt.wrapper import schema as yt_schema  # pyright: ignore[reportMissingImports]

from yt_framework.yt.client_base import OperationResources
from yt_framework.yt.client_prod import (
    YTProdClient,
    _apply_command_leg_format,
    _apply_max_row_weight_to_spec_builder,
    _apply_spec_options_and_split_run_operation_kwargs,
)


def _null_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


def test_yt_prod_client_raises_when_yt_proxy_missing() -> None:
    with pytest.raises(ValueError, match="YT_PROXY is not set"):
        YTProdClient(
            _null_logger("tests.client_prod"),
            secrets={"YT_TOKEN": "tok"},
        )


def test_yt_prod_client_raises_when_yt_token_missing() -> None:
    with pytest.raises(ValueError, match="YT_TOKEN is not set"):
        YTProdClient(
            _null_logger("tests.client_prod.tok"),
            secrets={"YT_PROXY": "http://proxy"},
        )


class _FakeSpecBuilder:
    def __init__(self) -> None:
        self.calls: List[Tuple[str, Any]] = []

    def title(self, value: str) -> "_FakeSpecBuilder":
        self.calls.append(("title", value))
        return self


def test_apply_spec_options_splits_run_operation_kwargs_from_builder_chain() -> None:
    b = _FakeSpecBuilder()
    b2, run_op = _apply_spec_options_and_split_run_operation_kwargs(
        b,
        {"title": "my_job", "sync": True},
    )
    assert b2 is b and run_op == {"sync": True} and b.calls == [("title", "my_job")]


def test_apply_spec_options_raises_value_error_on_unknown_operation_kwarg() -> None:
    b = _FakeSpecBuilder()
    with pytest.raises(ValueError, match="Unknown YT operation option 'bad_kw'"):
        _apply_spec_options_and_split_run_operation_kwargs(b, {"bad_kw": 1})


class _LegBuilderWithFormat:
    def __init__(self) -> None:
        self.format_specs: list[Any] = []

    def format(self, spec: Any) -> "_LegBuilderWithFormat":
        self.format_specs.append(spec)
        return self


class _BuilderWithTableWriter:
    def __init__(self) -> None:
        self.payload: Any = None

    def table_writer(self, payload: Any) -> "_BuilderWithTableWriter":
        self.payload = payload
        return self


class _BuilderWithJobIo:
    def __init__(self) -> None:
        self.payload: Any = None

    def job_io(self, payload: Any) -> "_BuilderWithJobIo":
        self.payload = payload
        return self


class _BuilderWithTableWriterAndJobIo:
    def __init__(self) -> None:
        self.table_writer_payload: Any = None
        self.job_io_payload: Any = None

    def table_writer(self, payload: Any) -> "_BuilderWithTableWriterAndJobIo":
        self.table_writer_payload = payload
        return self

    def job_io(self, payload: Any) -> "_BuilderWithTableWriterAndJobIo":
        self.job_io_payload = payload
        return self


def _chain_returns_self(mock_obj: MagicMock, method_names: tuple[str, ...]) -> None:
    for name in method_names:
        getattr(mock_obj, name).return_value = mock_obj


def _prod_client_with_run_operation_id(
    monkeypatch: pytest.MonkeyPatch,
    logger_name: str,
    op_id: str,
) -> tuple[YTProdClient, MagicMock]:
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = op_id
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger(logger_name),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    return client, fake_inner


def _stub_split_run_operation_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "yt_framework.yt.client_prod._apply_spec_options_and_split_run_operation_kwargs",
        lambda sb, kw: (sb, {}),
    )


def test_apply_command_leg_format_sets_json_format_for_string_command() -> None:
    b = _LegBuilderWithFormat()
    assert _apply_command_leg_format(b, "python3 mapper.py") is b
    assert len(b.format_specs) == 1 and b.format_specs[0].encode_utf8 is False


def test_apply_command_leg_format_skips_format_for_typed_job_leg() -> None:
    b = _LegBuilderWithFormat()
    assert _apply_command_leg_format(b, TypedJob()) is b and b.format_specs == []


def test_apply_max_row_weight_to_spec_builder_writes_bytes_to_table_writer() -> None:
    b = _BuilderWithTableWriter()
    out = _apply_max_row_weight_to_spec_builder(b, "128M")
    assert out is b and b.payload == {"max_row_weight": 134217728}


def test_apply_max_row_weight_to_spec_builder_uses_job_io_fallback_with_bytes() -> None:
    b = _BuilderWithJobIo()
    out = _apply_max_row_weight_to_spec_builder(b, "64M")
    assert out is b and b.payload == {"table_writer": {"max_row_weight": 67108864}}


def test_apply_max_row_weight_to_spec_builder_prefers_table_writer_over_job_io() -> (
    None
):
    b = _BuilderWithTableWriterAndJobIo()
    out = _apply_max_row_weight_to_spec_builder(b, "64M")
    assert (
        out is b
        and b.table_writer_payload == {"max_row_weight": 67108864}
        and b.job_io_payload is None
    )


def test_yt_prod_client_run_map_configures_pool_tree_docker_and_secure_vault(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prod run_map wires pool_trees, mapper docker_image, and secure_vault when set."""
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "yt-op-docker"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )

    spec = MagicMock()
    mapper = MagicMock()
    for name in (
        "pool",
        "resource_limits",
        "max_failed_job_count",
        "job_count",
        "input_table_paths",
        "output_table_paths",
        "pool_trees",
        "secure_vault",
    ):
        getattr(spec, name).return_value = spec
    spec.begin_mapper.return_value = mapper
    for name in (
        "command",
        "file_paths",
        "environment",
        "memory_limit",
        "cpu_limit",
        "gpu_limit",
        "format",
        "docker_image",
    ):
        getattr(mapper, name).return_value = mapper
    mapper.end_mapper.return_value = spec

    monkeypatch.setattr("yt_framework.yt.client_prod.MapSpecBuilder", lambda: spec)

    client = YTProdClient(
        _null_logger("tests.client_prod.run_map_docker"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    resources = OperationResources(
        pool_tree="heavy_tree",
        docker_image="registry.example/img:1",
    )
    op = client.run_map(
        "python3 mapper.py",
        "//tmp/in",
        "//tmp/out",
        [],
        resources,
        {},
        docker_auth={"user": "x", "password": "y"},
    )
    assert (
        op.id == "yt-op-docker"
        and spec.pool_trees.call_args == call(["heavy_tree"])
        and mapper.docker_image.call_args == call("registry.example/img:1")
        and spec.secure_vault.call_args
        == call({"docker_auth": {"user": "x", "password": "y"}})
    ), "pool_tree, docker_image, and secure_vault must be applied for docker map jobs"


def test_yt_prod_client_run_map_partitions_env_into_vault_and_wraps_string_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Secrets go to secure_vault; allowlisted keys stay in mapper.environment."""
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "yt-op-env-vault"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )

    spec = MagicMock()
    mapper = MagicMock()
    for name in (
        "pool",
        "resource_limits",
        "max_failed_job_count",
        "job_count",
        "input_table_paths",
        "output_table_paths",
        "secure_vault",
    ):
        getattr(spec, name).return_value = spec
    spec.begin_mapper.return_value = mapper
    for name in (
        "command",
        "file_paths",
        "environment",
        "memory_limit",
        "cpu_limit",
        "gpu_limit",
        "format",
        "end_mapper",
    ):
        getattr(mapper, name).return_value = mapper
    mapper.end_mapper.return_value = spec
    monkeypatch.setattr("yt_framework.yt.client_prod.MapSpecBuilder", lambda: spec)
    monkeypatch.setattr(
        "yt_framework.yt.client_prod._apply_spec_options_and_split_run_operation_kwargs",
        lambda sb, kw: (sb, {}),
    )

    client = YTProdClient(
        _null_logger("tests.client_prod.map_env_vault"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    client.run_map(
        "python3 mapper.py",
        "//tmp/in",
        "//tmp/out",
        [],
        OperationResources(),
        {"YT_TOKEN": "secret", "YT_STAGE_NAME": "st"},
    )
    mapper.environment.assert_called_once_with({"YT_STAGE_NAME": "st"})
    cmd_arg = mapper.command.call_args[0][0]
    assert isinstance(cmd_arg, str)
    assert cmd_arg.startswith("python3 -c ")
    assert spec.secure_vault.call_args == call({"YT_TOKEN": "secret"})


def test_yt_prod_client_run_map_returns_operation_from_client_run_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "yt-op-1"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.run_map"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    op = client.run_map(
        "python3 -c 'print(1)'",
        "//tmp/input_table",
        "//tmp/output_table",
        [],
        OperationResources(),
        {},
    )
    assert op.id == "yt-op-1"
    fake_inner.run_operation.assert_called_once()


def test_yt_prod_client_run_map_applies_default_max_row_weight_when_not_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _prod_client_with_run_operation_id(
        monkeypatch,
        "tests.client_prod.map_default_mrw",
        "yt-op-default-row-weight-map",
    )

    spec = MagicMock()
    mapper = MagicMock()
    _chain_returns_self(
        spec,
        (
            "pool",
            "resource_limits",
            "max_failed_job_count",
            "job_count",
            "input_table_paths",
            "output_table_paths",
            "table_writer",
        ),
    )
    spec.begin_mapper.return_value = mapper
    _chain_returns_self(
        mapper,
        (
            "command",
            "file_paths",
            "environment",
            "memory_limit",
            "cpu_limit",
            "gpu_limit",
            "format",
            "end_mapper",
        ),
    )
    mapper.end_mapper.return_value = spec
    monkeypatch.setattr("yt_framework.yt.client_prod.MapSpecBuilder", lambda: spec)
    _stub_split_run_operation_kwargs(monkeypatch)
    client.run_map(
        "python3 mapper.py", "//tmp/in", "//tmp/out", [], OperationResources(), {}
    )
    assert spec.table_writer.call_args == call({"max_row_weight": 134217728})


def test_yt_prod_client_run_map_applies_custom_max_row_weight_as_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _prod_client_with_run_operation_id(
        monkeypatch,
        "tests.client_prod.map_custom_mrw",
        "yt-op-custom-row-weight-map",
    )

    spec = MagicMock()
    mapper = MagicMock()
    _chain_returns_self(
        spec,
        (
            "pool",
            "resource_limits",
            "max_failed_job_count",
            "job_count",
            "input_table_paths",
            "output_table_paths",
            "table_writer",
        ),
    )
    spec.begin_mapper.return_value = mapper
    _chain_returns_self(
        mapper,
        (
            "command",
            "file_paths",
            "environment",
            "memory_limit",
            "cpu_limit",
            "gpu_limit",
            "format",
            "end_mapper",
        ),
    )
    mapper.end_mapper.return_value = spec
    monkeypatch.setattr("yt_framework.yt.client_prod.MapSpecBuilder", lambda: spec)
    _stub_split_run_operation_kwargs(monkeypatch)
    client.run_map(
        "python3 mapper.py",
        "//tmp/in",
        "//tmp/out",
        [],
        OperationResources(),
        {},
        max_row_weight="64M",
    )
    assert spec.table_writer.call_args == call({"max_row_weight": 67108864})


def test_yt_prod_client_run_map_sets_output_table_path_append_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "yt-op-append-map"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )

    spec = MagicMock()
    mapper = MagicMock()
    for name in (
        "pool",
        "resource_limits",
        "max_failed_job_count",
        "job_count",
        "input_table_paths",
        "output_table_paths",
    ):
        getattr(spec, name).return_value = spec
    spec.begin_mapper.return_value = mapper
    for name in (
        "command",
        "file_paths",
        "environment",
        "memory_limit",
        "cpu_limit",
        "gpu_limit",
        "format",
        "end_mapper",
    ):
        getattr(mapper, name).return_value = mapper
    mapper.end_mapper.return_value = spec

    captured: List[Any] = []

    def _otp(paths: Any) -> Any:
        captured[:] = list(paths)
        return spec

    spec.output_table_paths.side_effect = _otp
    monkeypatch.setattr("yt_framework.yt.client_prod.MapSpecBuilder", lambda: spec)

    monkeypatch.setattr(
        "yt_framework.yt.client_prod._apply_spec_options_and_split_run_operation_kwargs",
        lambda sb, kw: (sb, {}),
    )

    client = YTProdClient(
        _null_logger("tests.client_prod.map_append"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    client.run_map(
        "python3 mapper.py",
        "//tmp/in",
        "//tmp/out",
        [],
        OperationResources(),
        {},
        append=True,
    )
    assert len(captured) == 1 and captured[0].append is True


def _prod_client_with_stub_run_operation(monkeypatch: pytest.MonkeyPatch) -> Any:
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "yt-op-stub"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.stub"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    return client, fake_inner


def test_yt_prod_client_run_vanilla_returns_operation_from_client_run_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_stub_run_operation(monkeypatch)
    op = client.run_vanilla(
        "bash -c true",
        [],
        {},
        "my_task",
        OperationResources(),
    )
    assert op.id == "yt-op-stub"
    fake_inner.run_operation.assert_called_once()


def test_yt_prod_client_run_vanilla_applies_default_max_row_weight_when_not_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _prod_client_with_run_operation_id(
        monkeypatch,
        "tests.client_prod.vanilla_default_mrw",
        "yt-op-default-row-weight-vanilla",
    )

    spec = MagicMock()
    task = MagicMock()
    _chain_returns_self(
        spec,
        ("pool", "resource_limits", "max_failed_job_count", "table_writer"),
    )
    spec.begin_task.return_value = task
    _chain_returns_self(
        task,
        (
            "command",
            "file_paths",
            "environment",
            "memory_limit",
            "cpu_limit",
            "gpu_limit",
            "job_count",
            "end_task",
        ),
    )
    monkeypatch.setattr("yt_framework.yt.client_prod.VanillaSpecBuilder", lambda: spec)
    _stub_split_run_operation_kwargs(monkeypatch)
    client.run_vanilla("bash -c true", [], {}, "task", OperationResources())
    assert spec.table_writer.call_args == call({"max_row_weight": 134217728})


def test_yt_prod_client_run_vanilla_configures_pool_tree_docker_description_and_vault(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "yt-vanilla-docker"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )

    spec = MagicMock()
    task = MagicMock()
    for name in (
        "pool",
        "resource_limits",
        "max_failed_job_count",
        "description",
        "pool_trees",
        "secure_vault",
    ):
        getattr(spec, name).return_value = spec
    spec.begin_task.return_value = task
    for name in (
        "command",
        "file_paths",
        "environment",
        "memory_limit",
        "cpu_limit",
        "gpu_limit",
        "job_count",
        "docker_image",
        "end_task",
    ):
        getattr(task, name).return_value = task

    monkeypatch.setattr("yt_framework.yt.client_prod.VanillaSpecBuilder", lambda: spec)

    client = YTProdClient(
        _null_logger("tests.client_prod.vanilla_docker"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    resources = OperationResources(
        pool_tree="vanilla_tree",
        docker_image="registry.example/vanilla:2",
    )
    op = client.run_vanilla(
        "bash -c true",
        [],
        {},
        "vtask",
        resources,
        docker_auth={"registry_user": "u"},
        operation_description={"title": "vanilla-op"},
    )
    assert (
        op.id == "yt-vanilla-docker"
        and spec.description.call_args == call({"title": "vanilla-op"})
        and spec.pool_trees.call_args == call(["vanilla_tree"])
        and task.docker_image.call_args == call("registry.example/vanilla:2")
        and spec.secure_vault.call_args == call({"docker_auth": {"registry_user": "u"}})
    ), "vanilla spec should set description, pool_trees, task docker_image, and vault"


def test_yt_prod_client_run_map_reduce_returns_operation_from_client_run_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_stub_run_operation(monkeypatch)
    op = client.run_map_reduce(
        "python3 mapper.py",
        "python3 reducer.py",
        "//tmp/in",
        "//tmp/out",
        ["id"],
        [],
        OperationResources(),
        {},
    )
    assert op.id == "yt-op-stub"
    fake_inner.run_operation.assert_called_once()


def test_yt_prod_client_run_map_reduce_applies_default_max_row_weight_when_not_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _prod_client_with_run_operation_id(
        monkeypatch,
        "tests.client_prod.mr_default_mrw",
        "yt-op-default-row-weight-mr",
    )

    spec = MagicMock()
    mapper = MagicMock()
    reducer = MagicMock()
    _chain_returns_self(
        spec,
        (
            "input_table_paths",
            "output_table_paths",
            "pool",
            "max_failed_job_count",
            "reduce_by",
            "table_writer",
        ),
    )
    spec.begin_mapper.return_value = mapper
    spec.begin_reducer.return_value = reducer
    _chain_returns_self(
        mapper,
        (
            "command",
            "file_paths",
            "environment",
            "memory_limit",
            "cpu_limit",
            "gpu_limit",
            "format",
            "end_mapper",
            "end_reducer",
        ),
    )
    _chain_returns_self(
        reducer,
        (
            "command",
            "file_paths",
            "environment",
            "memory_limit",
            "cpu_limit",
            "gpu_limit",
            "format",
            "end_mapper",
            "end_reducer",
        ),
    )
    mapper.end_mapper.return_value = spec
    reducer.end_reducer.return_value = spec
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.MapReduceSpecBuilder", lambda: spec
    )
    _stub_split_run_operation_kwargs(monkeypatch)
    client.run_map_reduce(
        "python3 mapper.py",
        "python3 reducer.py",
        "//tmp/in",
        "//tmp/out",
        ["k"],
        [],
        OperationResources(),
        {},
    )
    assert spec.table_writer.call_args == call({"max_row_weight": 134217728})


def test_yt_prod_client_run_reduce_returns_operation_from_client_run_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_stub_run_operation(monkeypatch)
    op = client.run_reduce(
        "python3 reducer.py",
        "//tmp/in",
        "//tmp/out",
        ["id"],
        [],
        OperationResources(),
        {},
    )
    assert op.id == "yt-op-stub"
    fake_inner.run_operation.assert_called_once()


def test_yt_prod_client_run_reduce_applies_default_max_row_weight_when_not_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _prod_client_with_run_operation_id(
        monkeypatch,
        "tests.client_prod.reduce_default_mrw",
        "yt-op-default-row-weight-reduce",
    )

    spec = MagicMock()
    reducer = MagicMock()
    _chain_returns_self(
        spec,
        (
            "input_table_paths",
            "output_table_paths",
            "pool",
            "max_failed_job_count",
            "reduce_by",
            "begin_reducer",
            "table_writer",
        ),
    )
    spec.begin_reducer.return_value = reducer
    _chain_returns_self(
        reducer,
        (
            "command",
            "file_paths",
            "environment",
            "memory_limit",
            "cpu_limit",
            "gpu_limit",
            "format",
            "end_reducer",
        ),
    )
    reducer.end_reducer.return_value = spec
    monkeypatch.setattr("yt_framework.yt.client_prod.ReduceSpecBuilder", lambda: spec)
    _stub_split_run_operation_kwargs(monkeypatch)
    client.run_reduce(
        "python3 reducer.py",
        "//tmp/in",
        "//tmp/out",
        ["k"],
        [],
        OperationResources(),
        {},
    )
    assert spec.table_writer.call_args == call({"max_row_weight": 134217728})


def test_yt_prod_client_run_map_reduce_wires_pool_tree_slots_description_sort_map_job_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MapReduceSpecBuilder receives pool_trees, resource_limits, description, sort_by, map_job_count."""
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "mr-wired"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )

    spec = MagicMock()
    mapper = MagicMock()
    reducer = MagicMock()
    for name in (
        "input_table_paths",
        "output_table_paths",
        "pool",
        "max_failed_job_count",
        "pool_trees",
        "resource_limits",
        "description",
        "reduce_by",
        "sort_by",
        "map_job_count",
    ):
        getattr(spec, name).return_value = spec
    spec.begin_mapper.return_value = mapper
    spec.begin_reducer.return_value = reducer
    for name in (
        "command",
        "file_paths",
        "environment",
        "memory_limit",
        "cpu_limit",
        "gpu_limit",
        "format",
    ):
        getattr(mapper, name).return_value = mapper
        getattr(reducer, name).return_value = reducer
    mapper.end_mapper.return_value = spec
    reducer.end_reducer.return_value = spec

    monkeypatch.setattr(
        "yt_framework.yt.client_prod.MapReduceSpecBuilder", lambda: spec
    )

    client = YTProdClient(
        _null_logger("tests.client_prod.mr_opts"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    resources = OperationResources(pool_tree="mr_pool_tree", user_slots=9)
    op = client.run_map_reduce(
        "python3 mapper.py",
        "python3 reducer.py",
        "//tmp/in",
        "//tmp/out",
        ["id"],
        [],
        resources,
        {},
        sort_by=["ts"],
        map_job_count=4,
        operation_description={"title": "mr-title"},
    )
    assert (
        op.id == "mr-wired"
        and spec.pool_trees.call_args == call(["mr_pool_tree"])
        and spec.resource_limits.call_args == call({"user_slots": 9})
        and spec.description.call_args == call({"title": "mr-title"})
        and spec.sort_by.call_args == call(["ts"])
        and spec.map_job_count.call_args == call(4)
    ), "map-reduce must forward pool tree, slots, description, sort_by, map_job_count"


def test_yt_prod_client_run_map_reduce_applies_docker_secure_vault_on_mapper_and_reducer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mapper and reducer legs get docker_image; spec gets secure_vault with docker_auth."""
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "mr-docker"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )

    spec = MagicMock()
    mapper = MagicMock()
    reducer = MagicMock()
    for name in (
        "input_table_paths",
        "output_table_paths",
        "pool",
        "max_failed_job_count",
        "secure_vault",
        "reduce_by",
    ):
        getattr(spec, name).return_value = spec
    spec.begin_mapper.return_value = mapper
    spec.begin_reducer.return_value = reducer
    for name in (
        "command",
        "file_paths",
        "environment",
        "memory_limit",
        "cpu_limit",
        "gpu_limit",
        "format",
        "docker_image",
    ):
        getattr(mapper, name).return_value = mapper
        getattr(reducer, name).return_value = reducer
    mapper.end_mapper.return_value = spec
    reducer.end_reducer.return_value = spec

    monkeypatch.setattr(
        "yt_framework.yt.client_prod.MapReduceSpecBuilder", lambda: spec
    )

    client = YTProdClient(
        _null_logger("tests.client_prod.mr_docker"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    resources = OperationResources(docker_image="registry.example/mr:3")
    op = client.run_map_reduce(
        "python3 map.py",
        "python3 red.py",
        "//tmp/in",
        "//tmp/out",
        ["k"],
        [],
        resources,
        {},
        docker_auth={"registry_user": "u1"},
    )
    assert (
        op.id == "mr-docker"
        and mapper.docker_image.call_args == call("registry.example/mr:3")
        and reducer.docker_image.call_args == call("registry.example/mr:3")
        and spec.secure_vault.call_args
        == call({"docker_auth": {"registry_user": "u1"}})
    ), "map-reduce docker legs and vault must match OperationResources.docker_image"


def test_yt_prod_client_run_reduce_wires_pool_tree_slots_description_docker_vault(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ReduceSpecBuilder receives pool_trees, resource_limits, description, reducer docker + vault."""
    fake_inner = MagicMock()
    fake_op = MagicMock()
    fake_op.id = "red-wired"
    fake_inner.run_operation.return_value = fake_op
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )

    spec = MagicMock()
    reducer = MagicMock()
    for name in (
        "input_table_paths",
        "output_table_paths",
        "pool",
        "max_failed_job_count",
        "pool_trees",
        "resource_limits",
        "description",
        "secure_vault",
        "begin_reducer",
        "reduce_by",
    ):
        getattr(spec, name).return_value = spec
    spec.begin_reducer.return_value = reducer
    for name in (
        "command",
        "file_paths",
        "environment",
        "memory_limit",
        "cpu_limit",
        "gpu_limit",
        "format",
        "docker_image",
        "end_reducer",
    ):
        getattr(reducer, name).return_value = reducer
    reducer.end_reducer.return_value = spec

    monkeypatch.setattr("yt_framework.yt.client_prod.ReduceSpecBuilder", lambda: spec)

    client = YTProdClient(
        _null_logger("tests.client_prod.red_full"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    resources = OperationResources(
        pool_tree="reduce_tree",
        user_slots=2,
        docker_image="registry.example/reduce:1",
    )
    op = client.run_reduce(
        "python3 reducer.py",
        "//tmp/r_in",
        "//tmp/r_out",
        ["id"],
        [],
        resources,
        {},
        docker_auth={"user": "a", "password": "b"},
        operation_description={"title": "reduce-op"},
    )
    assert (
        op.id == "red-wired"
        and spec.pool_trees.call_args == call(["reduce_tree"])
        and spec.resource_limits.call_args == call({"user_slots": 2})
        and spec.description.call_args == call({"title": "reduce-op"})
        and reducer.docker_image.call_args == call("registry.example/reduce:1")
        and spec.secure_vault.call_args
        == call({"docker_auth": {"user": "a", "password": "b"}})
    ), "reduce must wire pool tree, slots, description, docker_image, and secure_vault"


def test_yt_prod_client_run_sort_calls_client_run_sort_with_sort_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.run_sort"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    client.run_sort("//tmp/sorted", ["a", "b"], pool="pool1", pool_tree="tree1")
    fake_inner.run_sort.assert_called_once()
    args, kwargs = fake_inner.run_sort.call_args
    assert args[0] == "//tmp/sorted"
    cols = kwargs["sort_by"]
    assert (
        len(cols) == 2
        and all(isinstance(c, yt_schema.SortColumn) for c in cols)
        and [c.name for c in cols] == ["a", "b"]
    )
    spec = kwargs.get("spec") or {}
    assert spec.get("pool") == "pool1" and spec.get("pool_tree") == "tree1"


def test_yt_prod_client_write_table_creates_and_writes_via_stub_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_inner.exists.return_value = False
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.write"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    client.write_table("//tmp/out/t", [{"k": 1}], append=False)
    fake_inner.exists.assert_called()
    assert fake_inner.create.call_count == 2, "parent map_node + table node"
    fake_inner.write_table.assert_called_once()


def test_yt_prod_client_read_table_returns_materialized_rows_from_stub_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_inner.read_table.return_value = iter([{"z": 2}])
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.read"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    assert client.read_table("//tmp/in") == [{"z": 2}]


def test_yt_prod_client_exists_delegates_to_stub_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_inner.exists.return_value = True
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.exists"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    assert client.exists("//tmp/x") is True
    fake_inner.exists.assert_called_once_with("//tmp/x")


def test_yt_prod_client_row_count_delegates_to_stub_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_inner.row_count.return_value = 42
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.rc"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    assert client.row_count("//tmp/t") == 42


def test_yt_prod_client_create_path_calls_client_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.cp"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    client.create_path("//tmp/new_node", node_type="map_node")
    fake_inner.create.assert_called_once_with(
        "map_node", "//tmp/new_node", recursive=True, ignore_existing=True
    )


def test_yt_prod_client_create_path_raises_after_logging_when_create_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_inner = MagicMock()
    fake_inner.create.side_effect = OSError("yt create denied")
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    caplog.set_level(logging.ERROR)
    client = YTProdClient(
        _null_logger("tests.client_prod.cp_err"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    with pytest.raises(OSError, match="yt create denied"):
        client.create_path("//tmp/bad")
    assert "Failed to create path" in caplog.text


def test_yt_prod_client_exists_raises_after_logging_when_exists_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_inner = MagicMock()
    fake_inner.exists.side_effect = RuntimeError("network down")
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    caplog.set_level(logging.ERROR)
    client = YTProdClient(
        _null_logger("tests.client_prod.exists_err"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    with pytest.raises(RuntimeError, match="network down"):
        client.exists("//tmp/x")
    assert "Failed to check if path exists" in caplog.text


def test_yt_prod_client_upload_directory_propagates_after_upload_file_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    root = tmp_path / "src"
    root.mkdir()
    (root / "a.txt").write_text("a", encoding="utf-8")
    fake_inner.write_file.side_effect = OSError("write_file failed")
    with pytest.raises(OSError, match="write_file failed"):
        client.upload_directory(root, "//yt/out")


def test_yt_prod_client_run_map_raises_runtime_error_when_run_operation_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_inner.run_operation.return_value = None
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.run_map_none"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    with pytest.raises(RuntimeError, match="run_operation returned None"):
        client.run_map(
            "python3 -c 'print(1)'",
            "//tmp/in",
            "//tmp/out",
            [],
            OperationResources(),
            {},
        )


def _prod_client_with_fake_inner(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, MagicMock]:
    fake_inner = MagicMock()
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.fake_inner"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    return client, fake_inner


def test_yt_prod_client_run_yql_logs_success_when_query_state_completed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    qobj = MagicMock()
    qobj.id = "yql-q-1"
    qobj.get_state.return_value = "completed"
    fake_inner.run_query.return_value = qobj
    client.run_yql("SELECT 1", pool="heavy")
    submitted_query = fake_inner.run_query.call_args.kwargs["query"]
    fake_inner.run_query.assert_called_once_with(
        engine="yql",
        query=submitted_query,
        settings={"pool": "heavy"},
    )
    assert submitted_query.startswith('PRAGMA yt.MaxRowWeight = "128M";')
    assert submitted_query.endswith("SELECT 1")
    qobj.get_state.assert_called_once()


def test_yt_prod_client_run_yql_uses_override_for_max_row_weight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    qobj = MagicMock()
    qobj.id = "yql-q-override"
    qobj.get_state.return_value = "completed"
    fake_inner.run_query.return_value = qobj
    client.run_yql("SELECT 1", max_row_weight="64M")
    submitted_query = fake_inner.run_query.call_args.kwargs["query"]
    assert submitted_query.startswith('PRAGMA yt.MaxRowWeight = "64M";')


def test_yt_prod_client_run_yql_raises_when_max_row_weight_exceeds_cluster_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    with pytest.raises(ValueError, match="exceeds cluster maximum"):
        client.run_yql("SELECT 1", max_row_weight="256M")
    fake_inner.run_query.assert_not_called()


def test_yt_prod_client_run_yql_does_not_duplicate_existing_max_row_weight_pragma(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    qobj = MagicMock()
    qobj.id = "yql-q-no-dup"
    qobj.get_state.return_value = "completed"
    fake_inner.run_query.return_value = qobj
    client.run_yql('PRAGMA yt.MaxRowWeight = "64M";\nSELECT 1;')
    submitted_query = fake_inner.run_query.call_args.kwargs["query"]
    assert submitted_query.count("PRAGMA yt.MaxRowWeight") == 1
    assert '"64M"' in submitted_query


def test_yt_prod_client_run_yql_raises_runtime_error_when_query_not_completed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    qobj = MagicMock()
    qobj.id = "yql-q-bad"
    qobj.get_state.return_value = "failed"
    qobj.get_error.return_value = "boom"
    fake_inner.run_query.return_value = qobj
    with pytest.raises(RuntimeError, match="Query failed with state failed"):
        client.run_yql("SELECT 1")


def test_yt_prod_client_run_yql_propagates_exception_from_run_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.run_query.side_effect = OSError("yt unreachable")
    with pytest.raises(OSError, match="yt unreachable"):
        client.run_yql("SELECT 1")


def test_yt_prod_client_get_table_columns_returns_names_from_yt_schema_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {
        "schema": [{"name": "id"}, {"name": "_internal_yql"}],
    }
    assert client._get_table_columns("//tmp/t") == ["id"]


def test_yt_prod_client_get_table_columns_uses_first_row_when_schema_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": []}
    fake_inner.read_table.return_value = iter([{"visible": 1, "_hidden": 2}])
    assert client._get_table_columns("//tmp/t") == ["visible"]


def test_yt_prod_client_get_table_columns_raises_when_table_has_no_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {}
    fake_inner.read_table.return_value = iter([])
    with pytest.raises(ValueError, match="empty, cannot determine columns"):
        client._get_table_columns("//tmp/empty")


def test_yt_prod_client_get_table_columns_raises_wrapped_error_on_read_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {}
    fake_inner.read_table.side_effect = RuntimeError("read broke")
    with pytest.raises(ValueError, match="Failed to get table columns from //tmp/bad"):
        client._get_table_columns("//tmp/bad")


def test_yt_prod_client_upload_file_calls_write_file_with_local_bytes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    local = tmp_path / "blob.bin"
    local.write_bytes(b"\x00\xff")
    client.upload_file(local, "//yt/cluster/blob.bin")
    fake_inner.write_file.assert_called_once()
    args, kwargs = fake_inner.write_file.call_args
    assert args[0] == "//yt/cluster/blob.bin"
    assert kwargs.get("force_create") is True and kwargs.get("compute_md5") is True


def test_yt_prod_client_upload_file_create_parent_dir_calls_create_for_parent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    f = tmp_path / "one.txt"
    f.write_text("x", encoding="utf-8")
    client.upload_file(f, "//yt/parent/child.txt", create_parent_dir=True)
    fake_inner.create.assert_called_with(
        "map_node",
        "//yt/parent",
        recursive=True,
        ignore_existing=True,
    )


def test_yt_prod_client_upload_directory_uploads_each_file_and_returns_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    root = tmp_path / "src"
    root.mkdir()
    (root / "a.txt").write_text("a", encoding="utf-8")
    sub = root / "sub"
    sub.mkdir()
    (sub / "b.txt").write_text("b", encoding="utf-8")
    out = sorted(client.upload_directory(root, "//yt/out"))
    assert out == ["//yt/out/a.txt", "//yt/out/sub/b.txt"]
    assert fake_inner.write_file.call_count == 2


def _attach_completed_yql_query(fake_inner: MagicMock) -> MagicMock:
    qobj = MagicMock()
    qobj.id = "yql-stub"
    qobj.get_state.return_value = "completed"
    fake_inner.run_query.return_value = qobj
    return qobj


def test_yt_prod_client_join_tables_runs_yql_when_not_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    _attach_completed_yql_query(fake_inner)
    client.join_tables("//left/t", "//right/t", "//out/t", "id", dry_run=False)
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert (
        "//left/t" in q and "//right/t" in q and "//out/t" in q
    ), "join_tables should execute built YQL referencing all three paths"


def test_yt_prod_client_filter_table_runs_yql_using_resolved_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": [{"name": "status"}]}
    _attach_completed_yql_query(fake_inner)
    client.filter_table(
        "//tmp/in_tbl",
        "//tmp/out_tbl",
        "status = 'active'",
        dry_run=False,
    )
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert (
        "WHERE" in q and "//tmp/in_tbl" in q
    ), "filter_table should run YQL with WHERE after _get_table_columns"


def test_yt_prod_client_union_tables_runs_yql_when_not_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": [{"name": "id"}]}
    _attach_completed_yql_query(fake_inner)
    client.union_tables(["//tmp/a", "//tmp/b"], "//tmp/u_out", dry_run=False)
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert "//tmp/a" in q and "//tmp/b" in q and "//tmp/u_out" in q


def test_yt_prod_client_select_columns_runs_yql_when_not_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    _attach_completed_yql_query(fake_inner)
    client.select_columns(
        "//tmp/s_in",
        "//tmp/s_out",
        ["id", "name"],
        dry_run=False,
    )
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert "//tmp/s_in" in q and "//tmp/s_out" in q


def test_yt_prod_client_group_by_aggregate_runs_yql_when_not_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    _attach_completed_yql_query(fake_inner)
    client.group_by_aggregate(
        "//tmp/g_in",
        "//tmp/g_out",
        "region",
        {"n": "count"},
        dry_run=False,
    )
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert "//tmp/g_in" in q and "//tmp/g_out" in q


def test_yt_prod_client_distinct_runs_yql_when_not_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    _attach_completed_yql_query(fake_inner)
    client.distinct("//tmp/d_in", "//tmp/d_out", columns=["k"], dry_run=False)
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert "//tmp/d_in" in q and "//tmp/d_out" in q


def test_yt_prod_client_sort_table_runs_yql_using_resolved_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": [{"name": "ts"}, {"name": "id"}]}
    _attach_completed_yql_query(fake_inner)
    client.sort_table(
        "//tmp/sort_in",
        "//tmp/sort_out",
        order_by="ts",
        dry_run=False,
    )
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert "//tmp/sort_in" in q and "//tmp/sort_out" in q


def test_yt_prod_client_limit_table_runs_yql_using_resolved_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": [{"name": "id"}]}
    _attach_completed_yql_query(fake_inner)
    client.limit_table("//tmp/l_in", "//tmp/l_out", limit=10, dry_run=False)
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert "//tmp/l_in" in q and "//tmp/l_out" in q and "LIMIT" in q.upper()


def test_yt_prod_client_get_table_columns_uses_yql_schema_inference_on_decode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)

    def _get_side_effect(path: str, attributes: Any = None) -> dict[str, Any]:
        if "temp_schema_" in path:
            return {"schema": [{"name": "inferred_col"}]}
        return {}

    fake_inner.get.side_effect = _get_side_effect
    fake_inner.read_table.side_effect = RuntimeError("Failed to decode string column")
    _attach_completed_yql_query(fake_inner)

    assert client._get_table_columns("//tmp/binaryish") == ["inferred_col"]
    fake_inner.run_query.assert_called_once()
    q = fake_inner.run_query.call_args.kwargs["query"]
    assert "PRAGMA yt.InferSchema" in q and "//tmp/binaryish" in q
    removed = [c.args[0] for c in fake_inner.remove.call_args_list]
    assert any("temp_schema_" in str(p) for p in removed)


def test_yt_prod_client_get_table_columns_raises_binary_help_when_inference_schema_only_internal_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)

    def _get_side_effect(path: str, attributes: Any = None) -> dict[str, Any]:
        if "temp_schema_" in path:
            return {"schema": [{"name": "_yql_column_0"}]}
        return {}

    fake_inner.get.side_effect = _get_side_effect
    fake_inner.read_table.side_effect = RuntimeError("Failed to decode string column")
    _attach_completed_yql_query(fake_inner)

    with pytest.raises(ValueError, match="binary columns that cannot be decoded"):
        client._get_table_columns("//tmp/internal_only")


def test_yt_prod_client_get_table_columns_raises_binary_help_when_yql_inference_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {}
    fake_inner.read_table.side_effect = RuntimeError("encoding error in row")
    fake_inner.run_query.side_effect = OSError("cluster yql unavailable")

    with pytest.raises(ValueError, match="binary columns that cannot be decoded"):
        client._get_table_columns("//tmp/yql_fail")


def test_yt_prod_client_get_table_columns_returns_inferred_columns_when_temp_remove_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Temp-table cleanup swallows remove errors so callers still get inferred columns."""

    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)

    def _get_side_effect(path: str, attributes: Any = None) -> dict[str, Any]:
        if "temp_schema_" in path:
            return {"schema": [{"name": "inferred_col"}]}
        return {}

    fake_inner.get.side_effect = _get_side_effect
    fake_inner.read_table.side_effect = RuntimeError("Failed to decode string column")
    fake_inner.remove.side_effect = OSError("yt remove failed")
    _attach_completed_yql_query(fake_inner)

    assert client._get_table_columns("//tmp/rm_fail_ok") == ["inferred_col"]


def test_yt_prod_client_get_table_columns_raises_binary_help_when_temp_remove_raises_after_internal_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup after failed inference swallows remove errors; binary helper ValueError still raises."""

    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)

    def _get_side_effect(path: str, attributes: Any = None) -> dict[str, Any]:
        if "temp_schema_" in path:
            return {"schema": [{"name": "_yql_column_0"}]}
        return {}

    fake_inner.get.side_effect = _get_side_effect
    fake_inner.read_table.side_effect = RuntimeError("Failed to decode string column")
    fake_inner.remove.side_effect = OSError("yt remove failed")
    _attach_completed_yql_query(fake_inner)

    with pytest.raises(ValueError, match="binary columns that cannot be decoded"):
        client._get_table_columns("//tmp/rm_fail_internal")


def test_yt_prod_client_get_table_columns_raises_binary_help_when_remove_raises_after_yql_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {}
    fake_inner.read_table.side_effect = RuntimeError("encoding error in row")
    fake_inner.run_query.side_effect = OSError("cluster yql unavailable")
    fake_inner.remove.side_effect = OSError("yt remove failed")

    with pytest.raises(ValueError, match="binary columns that cannot be decoded"):
        client._get_table_columns("//tmp/rm_fail_yql_ex")


def test_yt_prod_client_init_warns_when_proxy_discovery_config_unusable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BadCfg:
        def __contains__(self, _item: object) -> bool:
            raise RuntimeError("config unreadable")

    fake_inner = MagicMock()
    fake_inner.config = _BadCfg()
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    caplog.set_level(logging.WARNING)
    YTProdClient(
        _null_logger("tests.client_prod.init_proxy_warn"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    assert "Could not disable proxy discovery" in caplog.text


def test_yt_prod_client_write_table_overwrite_removes_existing_table_before_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.exists.return_value = True
    client.write_table("//tmp/exists", [{"n": 1}], append=False)
    fake_inner.remove.assert_called_once_with("//tmp/exists", force=True)


def test_yt_prod_client_write_table_append_skips_table_recreate_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    client.write_table("//tmp/t", [{"k": 1}], append=True)
    table_ops = [c for c in fake_inner.create.call_args_list if c[0][0] == "table"]
    assert table_ops == [], "append mode must not create/overwrite the table node"


def test_yt_prod_client_write_table_make_parents_false_skips_map_node_parents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.exists.return_value = False
    client.write_table("//ns/table", [{"a": 1}], make_parents=False)
    map_nodes = [c for c in fake_inner.create.call_args_list if c[0][0] == "map_node"]
    assert map_nodes == [], "make_parents=False must not create parent map_nodes"


def test_yt_prod_client_write_table_raises_after_log_on_client_write_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.exists.return_value = False
    fake_inner.write_table.side_effect = OSError("yt write_table failed")
    caplog.set_level(logging.ERROR)
    with pytest.raises(OSError, match="yt write_table failed"):
        client.write_table("//tmp/one", [{"x": 1}])
    assert "Failed to write table" in caplog.text


def test_yt_prod_client_row_count_raises_after_log_on_client_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.row_count.side_effect = RuntimeError("count failed")
    caplog.set_level(logging.ERROR)
    with pytest.raises(RuntimeError, match="count failed"):
        client.row_count("//tmp/t")
    assert "Failed to get row count" in caplog.text


def test_yt_prod_client_get_table_columns_uses_read_path_when_schema_get_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.side_effect = OSError("schema unavailable")
    fake_inner.read_table.return_value = iter([{"col_a": 1}])
    assert client._get_table_columns("//tmp/fallback_read") == ["col_a"]


def test_yt_prod_client_get_table_columns_keeps_underscore_only_names_from_read_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": []}
    fake_inner.read_table.return_value = iter([{"_internal": 1}])
    assert client._get_table_columns("//tmp/underscore_only") == ["_internal"]


def test_yt_prod_client_join_tables_dry_run_returns_query_without_run_yql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    q = client.join_tables("//l", "//r", "//o", "id", dry_run=True)
    assert (
        isinstance(q, str) and "JOIN" in q.upper() and not fake_inner.run_query.called
    )


def test_yt_prod_client_filter_table_dry_run_returns_query_without_run_yql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": [{"name": "status"}]}
    q = client.filter_table("//i", "//o", "status = 1", dry_run=True)
    assert "WHERE" in q and not fake_inner.run_query.called


def test_yt_prod_client_select_columns_dry_run_returns_query_without_run_yql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    q = client.select_columns("//i", "//o", ["a", "b"], dry_run=True)
    assert "//i" in q and "//o" in q and not fake_inner.run_query.called


def test_yt_prod_client_group_by_aggregate_dry_run_returns_query_without_run_yql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    q = client.group_by_aggregate("//i", "//o", "g", {"n": "count"}, dry_run=True)
    assert "GROUP BY" in q.upper() and not fake_inner.run_query.called


def test_yt_prod_client_union_tables_dry_run_returns_query_without_run_yql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": [{"name": "id"}]}
    q = client.union_tables(["//a", "//b"], "//u", dry_run=True)
    assert "UNION" in q.upper() and not fake_inner.run_query.called


def test_yt_prod_client_distinct_dry_run_returns_query_without_run_yql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    q = client.distinct("//d_in", "//d_out", columns=["k"], dry_run=True)
    assert "//d_in" in q and not fake_inner.run_query.called


def test_yt_prod_client_sort_table_dry_run_returns_query_without_run_yql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": [{"name": "ts"}]}
    q = client.sort_table("//s_in", "//s_out", order_by="ts", dry_run=True)
    assert "//s_in" in q and not fake_inner.run_query.called


def test_yt_prod_client_limit_table_dry_run_returns_query_without_run_yql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    fake_inner.get.return_value = {"schema": [{"name": "id"}]}
    q = client.limit_table("//l_in", "//l_out", limit=3, dry_run=True)
    assert "LIMIT" in q.upper() and not fake_inner.run_query.called


@pytest.mark.parametrize(
    ("method_name", "kwargs"),
    [
        (
            "join_tables",
            {
                "left_table": "//l",
                "right_table": "//r",
                "output_table": "//o",
                "on": "id",
            },
        ),
        (
            "filter_table",
            {
                "input_table": "//i",
                "output_table": "//o",
                "condition": "id > 0",
            },
        ),
        (
            "select_columns",
            {
                "input_table": "//i",
                "output_table": "//o",
                "columns": ["id"],
            },
        ),
        (
            "group_by_aggregate",
            {
                "input_table": "//i",
                "output_table": "//o",
                "group_by": "id",
                "aggregations": {"n": "count"},
            },
        ),
        (
            "union_tables",
            {"tables": ["//a", "//b"], "output_table": "//o"},
        ),
        (
            "distinct",
            {"input_table": "//i", "output_table": "//o", "columns": ["id"]},
        ),
        (
            "sort_table",
            {"input_table": "//i", "output_table": "//o", "order_by": "id"},
        ),
        (
            "limit_table",
            {"input_table": "//i", "output_table": "//o", "limit": 5},
        ),
    ],
)
def test_yt_prod_yql_helpers_forward_max_row_weight_to_run_yql(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    kwargs: dict[str, Any],
) -> None:
    client, _ = _prod_client_with_fake_inner(monkeypatch)
    monkeypatch.setattr(client, "_get_table_columns", lambda _path: ["id"])
    run_yql = MagicMock()
    monkeypatch.setattr(client, "run_yql", run_yql)
    method = getattr(client, method_name)
    method(max_row_weight="64M", **kwargs)
    assert run_yql.call_args.kwargs.get("max_row_weight") == "64M"


def test_yt_prod_client_upload_directory_skips_ytignored_files_and_logs_count(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, fake_inner = _prod_client_with_fake_inner(monkeypatch)
    root = tmp_path / "udir"
    root.mkdir()
    (root / ".ytignore").write_text("skipme.txt\n", encoding="utf-8")
    (root / "keep.txt").write_text("a", encoding="utf-8")
    (root / "skipme.txt").write_text("b", encoding="utf-8")
    caplog.set_level(logging.INFO)
    out = sorted(client.upload_directory(root, "//yt/out"))
    assert out == ["//yt/out/keep.txt"] and "Ignored" in caplog.text


def test_yt_prod_client_run_vanilla_raises_when_run_operation_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_inner.run_operation.return_value = None
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.v_none"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    with pytest.raises(RuntimeError, match="run_operation returned None"):
        client.run_vanilla("true", [], {}, "t", OperationResources())


def test_yt_prod_client_run_map_reduce_raises_when_run_operation_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_inner.run_operation.return_value = None
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.mr_none"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    with pytest.raises(RuntimeError, match="Failed to submit map-reduce"):
        client.run_map_reduce(
            "m",
            "r",
            "//i",
            "//o",
            ["k"],
            [],
            OperationResources(),
            {},
        )


def test_yt_prod_client_run_reduce_raises_when_run_operation_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_inner = MagicMock()
    fake_inner.run_operation.return_value = None
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    client = YTProdClient(
        _null_logger("tests.client_prod.red_none"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    with pytest.raises(RuntimeError, match="Failed to submit reduce"):
        client.run_reduce("r", "//i", "//o", ["k"], [], OperationResources(), {})


def test_yt_prod_client_run_vanilla_propagates_submit_exception_after_error_log(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_inner = MagicMock()
    fake_inner.run_operation.side_effect = ValueError("bad spec")
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    caplog.set_level(logging.ERROR)
    client = YTProdClient(
        _null_logger("tests.client_prod.v_exc"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    with pytest.raises(ValueError, match="bad spec"):
        client.run_vanilla("true", [], {}, "t", OperationResources())
    assert "Failed to submit vanilla operation" in caplog.text


def test_yt_prod_client_run_sort_raises_after_log_on_client_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_inner = MagicMock()
    fake_inner.run_sort.side_effect = OSError("sort cluster error")
    monkeypatch.setattr(
        "yt_framework.yt.client_prod.YtClient", lambda *a, **k: fake_inner
    )
    caplog.set_level(logging.ERROR)
    client = YTProdClient(
        _null_logger("tests.client_prod.sort_exc"),
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    with pytest.raises(OSError, match="sort cluster error"):
        client.run_sort("//tmp/t", ["c"])
    assert "Failed to sort table" in caplog.text
