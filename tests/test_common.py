"""Tests for yt_framework.operations.common helpers."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

from omegaconf import OmegaConf

from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.stage import StageContext
from yt_framework.operations.common import (
    build_environment,
    build_operation_environment,
    collect_passthrough_kwargs,
    extract_docker_auth_from_operation_config,
    extract_max_failed_jobs,
    extract_operation_resources,
    prepare_docker_auth,
)
from yt_framework.yt.client_base import BaseYTClient

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


def test_extract_max_failed_jobs_uses_default_when_config_value_is_none() -> None:
    cfg = OmegaConf.create({"max_failed_job_count": None})
    assert extract_max_failed_jobs(cfg, _LOG) == 1


def test_extract_operation_resources_uses_default_when_nested_value_is_none() -> None:
    cfg = OmegaConf.create({"resources": {"pool": None}})
    res = extract_operation_resources(cfg, _LOG)
    assert res.pool == "default"


class _ConfigThatRaisesOnAccess:
    """Stand-in config object that forces the generic fallback in _get_config_value_with_default."""

    def __contains__(self, _key: object) -> bool:
        raise RuntimeError("config access failed")

    def get(self, _key: str) -> None:
        raise RuntimeError("config access failed")


def test_extract_max_failed_jobs_falls_back_to_default_on_config_access_error() -> None:
    assert extract_max_failed_jobs(_ConfigThatRaisesOnAccess(), _LOG) == 1


def _stage_context_with_configs(
    tmp_path: Path, secrets: str = "K=secret\n"
) -> StageContext:
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "secrets.env").write_text(secrets, encoding="utf-8")
    fake_yt = MagicMock(spec=BaseYTClient)
    deps = PipelineStageDependencies(
        yt_client=fake_yt,
        pipeline_config=OmegaConf.create({}),
        configs_dir=cfg_dir,
    )
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    return StageContext(
        name="train",
        config=OmegaConf.create({}),
        stage_dir=stage_dir,
        logger=_LOG,
        deps=deps,
    )


def test_build_operation_environment_merges_secrets_env_and_stage_name(
    tmp_path: Path,
) -> None:
    ctx = _stage_context_with_configs(tmp_path)
    op_cfg = OmegaConf.create({"env": {"EXTRA": "x", "SKIP": None}})
    env = build_operation_environment(
        ctx,
        op_cfg,
        _LOG,
        include_tokenizer_artifact=False,
        include_stage_name=True,
    )
    assert (env.get("K"), env.get("EXTRA"), env.get("YT_STAGE_NAME")) == (
        "secret",
        "x",
        "train",
    )


def test_build_operation_environment_omits_stage_name_when_disabled(
    tmp_path: Path,
) -> None:
    ctx = _stage_context_with_configs(tmp_path)
    env = build_operation_environment(
        ctx,
        OmegaConf.create({}),
        _LOG,
        include_tokenizer_artifact=False,
        include_stage_name=False,
    )
    assert "YT_STAGE_NAME" not in env


def test_build_operation_environment_sets_tokenizer_env_when_artifact_configured(
    tmp_path: Path,
) -> None:
    ctx = _stage_context_with_configs(tmp_path)
    op_cfg = OmegaConf.create(
        {
            "tokenizer_artifact": {
                "artifact_base": "//home/artifacts",
                "artifact_name": "tok",
            }
        }
    )
    with patch("yt_framework.operations.common.init_tokenizer_artifact_directory"):
        env = build_operation_environment(
            ctx,
            op_cfg,
            _LOG,
            include_tokenizer_artifact=True,
            include_stage_name=False,
        )
    assert (
        env.get("TOKENIZER_ARTIFACT_FILE"),
        env.get("TOKENIZER_ARTIFACT_DIR"),
        env.get("TOKENIZER_ARTIFACT_NAME"),
    ) == ("tok.tar.gz", "tokenizer_artifacts/tok", "tok")


def test_build_environment_loads_secrets_from_configs_dir(tmp_path: Path) -> None:
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "secrets.env").write_text("YT_TOKEN=abc\n")
    env = build_environment(cfg_dir, _LOG)
    assert env.get("YT_TOKEN") == "abc"
