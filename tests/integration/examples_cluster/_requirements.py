"""Gating for optional ``examples_cluster`` subprocess runs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import yaml

from yt_framework.utils.env import load_secrets

if TYPE_CHECKING:
    from pathlib import Path


def s3_example_has_config_and_secrets(
    repo_root: Path,
    *,
    env_opt_in: bool,
    cluster_secrets: dict[str, str],
) -> tuple[bool, str]:
    """Return (ok, reason_skip) for running ``06_s3_integration`` on a cell."""
    if not env_opt_in:
        return (
            False,
            "set YT_FRAMEWORK_EXAMPLE_S3=1 to run the S3 example on the cluster",
        )
    stage_cfg = (
        repo_root
        / "examples"
        / "06_s3_integration"
        / "stages"
        / "list_s3"
        / "config.yaml"
    )
    raw = yaml.safe_load(stage_cfg.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return False, "invalid list_s3 config.yaml"
    client = raw.get("client", {})
    bucket = str(client.get("input_bucket") or "").strip()
    prefix = str(client.get("input_prefix") or "").strip()
    if not bucket or not prefix:
        return (
            False,
            "set client.input_bucket and client.input_prefix in "
            "examples/06_s3_integration/stages/list_s3/config.yaml",
        )
    secrets_dir = repo_root / "examples" / "06_s3_integration" / "configs"
    sec = {**cluster_secrets, **load_secrets(secrets_dir)}
    for k in ("S3_ENDPOINT", "S3_DOWNLOAD_ACCESS_KEY", "S3_DOWNLOAD_SECRET_KEY"):
        if not str(sec.get(k, "")).strip():
            return (
                False,
                f"missing {k} in yt-cluster-test.env or "
                "examples/06_s3_integration/configs/secrets.env",
            )
    return True, ""


def docker_example_opt_in(*, env_opt_in: bool) -> tuple[bool, str]:
    """Return (ok, reason_skip) for running ``07_custom_docker`` on a cell."""
    if not env_opt_in:
        return (
            False,
            "set YT_FRAMEWORK_EXAMPLE_DOCKER=1 to run the custom Docker example",
        )
    return True, ""
