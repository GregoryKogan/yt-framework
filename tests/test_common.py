"""Tests for yt_framework.operations.common helpers."""

import logging
from pathlib import Path

from omegaconf import OmegaConf

from yt_framework.operations.common import (
    build_environment,
    collect_passthrough_kwargs,
    extract_docker_auth_from_operation_config,
    extract_max_failed_jobs,
    extract_operation_resources,
    prepare_docker_auth,
)

_LOG = logging.getLogger("tests.common")


def test_prepare_docker_auth_returns_dict_when_all_parts_present() -> None:
    out = prepare_docker_auth("img:latest", "user", "secret")
    assert out == {"username": "user", "password": "secret"}


def test_prepare_docker_auth_returns_none_when_password_missing() -> None:
    assert prepare_docker_auth("img:latest", "user", None) is None


def test_prepare_docker_auth_returns_none_when_image_missing() -> None:
    assert prepare_docker_auth(None, "user", "secret") is None


def test_extract_operation_resources_reads_top_level_keys() -> None:
    cfg = OmegaConf.create({"pool": "heavy", "memory_limit_gb": 16, "cpu_limit": 8})
    res = extract_operation_resources(cfg, _LOG)
    assert res.pool == "heavy" and res.memory_gb == 16 and res.cpu_limit == 8


def test_extract_operation_resources_reads_nested_resources_block() -> None:
    cfg = OmegaConf.create({"resources": {"pool": "nest", "job_count": 3}})
    res = extract_operation_resources(cfg, _LOG)
    assert res.pool == "nest" and res.job_count == 3


def test_collect_passthrough_kwargs_skips_reserved_and_none() -> None:
    cfg = OmegaConf.create(
        {"resources": {"pool": "p"}, "extra": 1, "skip_none": None, "tag": "x"}
    )
    out = collect_passthrough_kwargs(cfg, reserved_keys={"resources"})
    assert out == {"extra": 1, "tag": "x"}


def test_collect_passthrough_kwargs_resolves_dict_node_to_plain_dict() -> None:
    cfg = OmegaConf.create({"meta": {"a": 1}})
    out = collect_passthrough_kwargs(cfg, reserved_keys=set())
    assert out == {"meta": {"a": 1}}


def test_extract_docker_auth_from_operation_config_uses_env_credentials() -> None:
    cfg = OmegaConf.create({"resources": {"docker_image": "reg/img:v1"}})
    env = {"DOCKER_AUTH_USERNAME": "u", "DOCKER_AUTH_PASSWORD": "p"}
    auth = extract_docker_auth_from_operation_config(cfg, env)
    assert auth == {"username": "u", "password": "p"}


def test_extract_max_failed_jobs_default_when_missing() -> None:
    cfg = OmegaConf.create({})
    assert extract_max_failed_jobs(cfg, _LOG) == 1


def test_extract_max_failed_jobs_reads_config_value() -> None:
    cfg = OmegaConf.create({"max_failed_job_count": 5})
    assert extract_max_failed_jobs(cfg, _LOG) == 5


def test_build_environment_loads_secrets_from_configs_dir(tmp_path: Path) -> None:
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "secrets.env").write_text("YT_TOKEN=abc\n")
    env = build_environment(cfg_dir, _LOG)
    assert env.get("YT_TOKEN") == "abc"
