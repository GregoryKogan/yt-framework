"""Pack tokenizer/processor tarballs, upload to Cypress, and expose sandbox env vars."""

from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from yt_framework.core.stage import StageContext


def resolve_tokenizer_artifact_name(
    stage_config: DictConfig,
    tokenizer_artifact_config: DictConfig,
) -> str | None:
    """Resolve logical tokenizer artifact name from config."""
    explicit = tokenizer_artifact_config.get("artifact_name")
    if explicit and str(explicit).strip():
        return str(explicit).strip()

    if "job" in stage_config:
        tokenizer_name = stage_config.job.get("tokenizer_name")
        if tokenizer_name and str(tokenizer_name).strip():
            return str(tokenizer_name).strip()

        model_name = stage_config.job.get("model_name")
        if model_name and str(model_name).strip():
            return str(model_name).strip().split("/")[-1]

    local_path = tokenizer_artifact_config.get("local_artifact_path")
    if local_path and str(local_path).strip():
        return Path(str(local_path)).name.replace(".tar.gz", "")

    return None


def resolve_tokenizer_archive_name(artifact_name: str) -> str:
    """Convert logical artifact name to mounted tar filename."""
    if artifact_name.endswith(".tar.gz"):
        return artifact_name
    return f"{artifact_name}.tar.gz"


def resolve_tokenizer_artifact_yt_path(
    stage_config: DictConfig,
    tokenizer_artifact_config: DictConfig,
) -> str | None:
    """Resolve full YT file path for tokenizer artifact tarball."""
    artifact_base = tokenizer_artifact_config.get("artifact_base")
    if not artifact_base:
        return None
    artifact_name = resolve_tokenizer_artifact_name(
        stage_config=stage_config,
        tokenizer_artifact_config=tokenizer_artifact_config,
    )
    if not artifact_name:
        return None
    archive_name = resolve_tokenizer_archive_name(artifact_name)
    return f"{artifact_base}/{archive_name}"


def _tar_directory(source_dir: Path, target_tar_gz: Path) -> None:
    """Create tar.gz from directory contents (without parent dir wrapper)."""
    with tarfile.open(target_tar_gz, "w:gz") as tar:
        for path in source_dir.rglob("*"):
            if path.is_file():
                tar.add(path, arcname=path.relative_to(source_dir), recursive=False)


def _prepare_local_archive(local_artifact_path: Path, artifact_name: str) -> Path:
    """Prepare local tar.gz path from `local_artifact_path`.

    - If source is a directory, pack it to a temporary `.tar.gz`.
    - If source is `.tar.gz`, use it directly.
    """
    if local_artifact_path.is_dir():
        _fd, tmp_name = tempfile.mkstemp(prefix=f"{artifact_name}_", suffix=".tar.gz")
        Path(tmp_name).unlink(missing_ok=True)
        tmp_archive = Path(tmp_name)
        _tar_directory(local_artifact_path, tmp_archive)
        return tmp_archive

    if local_artifact_path.is_file() and local_artifact_path.name.endswith(".tar.gz"):
        return local_artifact_path

    msg = (
        "local_artifact_path must point to a directory or to a .tar.gz file, "
        f"got: {local_artifact_path}"
    )
    raise ValueError(msg)


def init_tokenizer_artifact_directory(
    context: StageContext,
    tokenizer_artifact_config: DictConfig,
) -> None:
    """Initialize tokenizer artifact in YT (if configured).

    Behavior:
    - creates `artifact_base` if needed;
    - uploads local artifact from `local_artifact_path` if provided and missing in YT;
    - validates artifact presence in YT.
    """
    artifact_base = tokenizer_artifact_config.get("artifact_base")
    if not artifact_base:
        return

    artifact_name = resolve_tokenizer_artifact_name(
        stage_config=context.config,
        tokenizer_artifact_config=tokenizer_artifact_config,
    )
    if not artifact_name:
        msg = (
            "tokenizer_artifact is configured but artifact_name cannot be resolved. "
            "Set tokenizer_artifact.artifact_name or job.tokenizer_name/model_name."
        )
        raise ValueError(msg)

    archive_name = resolve_tokenizer_archive_name(artifact_name)
    yt_artifact_path = f"{artifact_base}/{archive_name}"
    local_artifact_path = tokenizer_artifact_config.get("local_artifact_path")

    context.deps.yt_client.create_path(artifact_base, node_type="map_node")
    context.logger.info("Tokenizer artifact directory ready: %s", artifact_base)

    temp_archive: Path | None = None
    try:
        if local_artifact_path:
            source = Path(str(local_artifact_path))
            if not source.exists():
                context.logger.warning(
                    "tokenizer_artifact.local_artifact_path does not exist: %s", source
                )
            elif context.deps.yt_client.exists(yt_artifact_path):
                context.logger.info(
                    "Tokenizer artifact already exists in YT: %s (skipping upload)",
                    yt_artifact_path,
                )
            else:
                archive_local_path = _prepare_local_archive(source, artifact_name)
                if archive_local_path != source:
                    temp_archive = archive_local_path
                context.logger.info(
                    "Uploading tokenizer artifact: %s -> %s",
                    archive_local_path,
                    yt_artifact_path,
                )
                context.deps.yt_client.upload_file(
                    archive_local_path, yt_artifact_path, create_parent_dir=True
                )

        if not context.deps.yt_client.exists(yt_artifact_path):
            msg = (
                f"Tokenizer artifact not found in YT: {yt_artifact_path}. "
                "Provide tokenizer_artifact.local_artifact_path or upload manually."
            )
            raise FileNotFoundError(msg)

        context.logger.info("Tokenizer artifact verified: %s", yt_artifact_path)
    finally:
        if temp_archive and temp_archive.exists():
            temp_archive.unlink(missing_ok=True)
