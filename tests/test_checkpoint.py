"""Tests for yt_framework.operations.checkpoint.init_checkpoint_directory."""

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf

from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.stage import StageContext
from yt_framework.operations.checkpoint import init_checkpoint_directory
from yt_framework.yt.client_base import BaseYTClient

_LOG = logging.getLogger("tests.checkpoint")
_LOG.addHandler(logging.NullHandler())


def _ctx(
    tmp_path: Path,
    *,
    job_section: dict | None = None,
) -> tuple[StageContext, MagicMock]:
    stage_dir = tmp_path / "st"
    stage_dir.mkdir()
    configs = tmp_path / "cfg"
    configs.mkdir()
    fake_yt = MagicMock(spec=BaseYTClient)
    cfg: dict = {}
    if job_section is not None:
        cfg["job"] = job_section
    deps = PipelineStageDependencies(
        yt_client=fake_yt,
        pipeline_config=OmegaConf.create({"pipeline": {"build_folder": "//bf"}}),
        configs_dir=configs,
    )
    ctx = StageContext(
        name="ckpt_stage",
        config=OmegaConf.create(cfg),
        stage_dir=stage_dir,
        logger=_LOG,
        deps=deps,
    )
    return ctx, fake_yt


def test_init_checkpoint_directory_no_op_when_checkpoint_base_missing(
    tmp_path: Path,
) -> None:
    ctx, yt = _ctx(tmp_path)
    init_checkpoint_directory(ctx, OmegaConf.create({}))
    yt.create_path.assert_not_called()


def test_init_checkpoint_directory_creates_yt_path_when_base_set(
    tmp_path: Path,
) -> None:
    ctx, yt = _ctx(tmp_path)
    init_checkpoint_directory(
        ctx,
        OmegaConf.create({"checkpoint_base": "//yt/checkpoints"}),
    )
    yt.create_path.assert_called_once_with("//yt/checkpoints", node_type="map_node")


def test_init_checkpoint_directory_uploads_local_when_missing_on_yt(
    tmp_path: Path,
) -> None:
    local = tmp_path / "weights.bin"
    local.write_bytes(b"x")
    ctx, yt = _ctx(tmp_path)
    yt.exists.return_value = False
    init_checkpoint_directory(
        ctx,
        OmegaConf.create(
            {
                "checkpoint_base": "//yt/cp",
                "local_checkpoint_path": str(local),
            }
        ),
    )
    yt.upload_file.assert_called_once_with(
        local, "//yt/cp/weights.bin", create_parent_dir=True
    )


def test_init_checkpoint_directory_skips_upload_when_checkpoint_exists_on_yt(
    tmp_path: Path,
) -> None:
    local = tmp_path / "weights.bin"
    local.write_bytes(b"x")
    ctx, yt = _ctx(tmp_path)
    yt.exists.return_value = True
    init_checkpoint_directory(
        ctx,
        OmegaConf.create(
            {
                "checkpoint_base": "//yt/cp",
                "local_checkpoint_path": str(local),
            }
        ),
    )
    yt.upload_file.assert_not_called()


def test_init_checkpoint_directory_warns_when_local_checkpoint_missing(
    tmp_path: Path,
) -> None:
    ctx, yt = _ctx(tmp_path)
    init_checkpoint_directory(
        ctx,
        OmegaConf.create(
            {
                "checkpoint_base": "//yt/cp",
                "local_checkpoint_path": str(tmp_path / "nope.bin"),
            }
        ),
    )
    yt.upload_file.assert_not_called()


def test_init_checkpoint_directory_raises_when_required_model_checkpoint_missing(
    tmp_path: Path,
) -> None:
    ctx, yt = _ctx(tmp_path, job_section={"model_name": "m.bin"})
    yt.exists.return_value = False
    with pytest.raises(FileNotFoundError, match="Required checkpoint not found"):
        init_checkpoint_directory(
            ctx,
            OmegaConf.create({"checkpoint_base": "//yt/cp"}),
        )


def test_init_checkpoint_directory_verifies_model_checkpoint_when_present(
    tmp_path: Path,
) -> None:
    ctx, yt = _ctx(tmp_path, job_section={"model_name": "m.bin"})

    def exists_side(path: str) -> bool:
        return path == "//yt/cp/m.bin"

    yt.exists.side_effect = exists_side
    init_checkpoint_directory(
        ctx,
        OmegaConf.create({"checkpoint_base": "//yt/cp"}),
    )
    assert yt.exists.call_args_list[-1][0][0] == "//yt/cp/m.bin"


def test_init_checkpoint_directory_propagates_create_path_failure(
    tmp_path: Path,
) -> None:
    ctx, yt = _ctx(tmp_path)
    yt.create_path.side_effect = RuntimeError("yt down")
    with pytest.raises(RuntimeError, match="yt down"):
        init_checkpoint_directory(
            ctx,
            OmegaConf.create({"checkpoint_base": "//yt/cp"}),
        )


def test_init_checkpoint_directory_propagates_upload_failure(
    tmp_path: Path,
) -> None:
    local = tmp_path / "w.bin"
    local.write_bytes(b"z")
    ctx, yt = _ctx(tmp_path)
    yt.exists.return_value = False
    yt.upload_file.side_effect = OSError("upload failed")
    with pytest.raises(OSError, match="upload failed"):
        init_checkpoint_directory(
            ctx,
            OmegaConf.create(
                {
                    "checkpoint_base": "//yt/cp",
                    "local_checkpoint_path": str(local),
                }
            ),
        )
