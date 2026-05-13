"""Pack tokenizer/processor tarballs, upload to Cypress, and expose sandbox env vars."""

from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from yt_framework.operations.stage_contracts import StageContext


def _nonempty_str(value: object) -> str | None:
    if value and str(value).strip():
        return str(value).strip()
    return None


def _artifact_name_from_job(stage_config: DictConfig) -> str | None:
    if "job" not in stage_config:
        return None
    tokenizer_name = stage_config.job.get("tokenizer_name")
    t = _nonempty_str(tokenizer_name)
    if t is not None:
        return t
    model_name = stage_config.job.get("model_name")
    m = _nonempty_str(model_name)
    if m is None:
        return None
    return m.split("/")[-1]


def resolve_tokenizer_artifact_name(
    stage_config: DictConfig,
    tokenizer_artifact_config: DictConfig,
) -> str | None:
    """Resolve logical tokenizer artifact name from config."""
    explicit = tokenizer_artifact_config.get("artifact_name")
    e = _nonempty_str(explicit)
    if e is not None:
        return e

    from_job = _artifact_name_from_job(stage_config)
    if from_job is not None:
        return from_job

    local_path = tokenizer_artifact_config.get("local_artifact_path")
    lp = _nonempty_str(local_path)
    if lp is not None:
        return Path(lp).name.replace(".tar.gz", "")

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


def _tokenizer_artifact_base_str(tokenizer_artifact_config: DictConfig) -> str | None:
    raw = tokenizer_artifact_config.get("artifact_base")
    if isinstance(raw, str) and raw.strip():
        return str(raw).strip()
    return None


def _upload_tokenizer_if_missing(
    context: StageContext,
    *,
    local_artifact_path: str,
    yt_artifact_path: str,
    artifact_name: str,
) -> Path | None:
    """Upload local tarball when path exists and YT object missing. Returns temp archive to delete."""
    source = Path(str(local_artifact_path))
    if not source.exists():
        context.logger.warning(
            "tokenizer_artifact.local_artifact_path does not exist: %s",
            source,
        )
        return None
    if context.deps.yt_client.exists(yt_artifact_path):
        context.logger.info(
            "Tokenizer artifact already exists in YT: %s (skipping upload)",
            yt_artifact_path,
        )
        return None
    archive_local_path = _prepare_local_archive(source, artifact_name)
    context.logger.info(
        "Uploading tokenizer artifact: %s -> %s",
        archive_local_path,
        yt_artifact_path,
    )
    context.deps.yt_client.upload_file(
        archive_local_path,
        yt_artifact_path,
        create_parent_dir=True,
    )
    return archive_local_path if archive_local_path != source else None


def tokenizer_artifact_name_or_raise(
    stage_config: DictConfig,
    tokenizer_artifact_config: DictConfig,
) -> str:
    artifact_name = resolve_tokenizer_artifact_name(
        stage_config=stage_config,
        tokenizer_artifact_config=tokenizer_artifact_config,
    )
    if artifact_name:
        return artifact_name
    msg = (
        "tokenizer_artifact is configured but artifact_name cannot be resolved. "
        "Set tokenizer_artifact.artifact_name or job.tokenizer_name/model_name."
    )
    raise ValueError(msg)


def _local_artifact_path_option(tokenizer_artifact_config: DictConfig) -> str | None:
    local_raw = tokenizer_artifact_config.get("local_artifact_path")
    return str(local_raw) if isinstance(local_raw, str) and local_raw else None


def verify_tokenizer_path_or_raise(
    context: StageContext,
    yt_artifact_path: str,
) -> None:
    if not context.deps.yt_client.exists(yt_artifact_path):
        msg = (
            f"Tokenizer artifact not found in YT: {yt_artifact_path}. "
            "Provide tokenizer_artifact.local_artifact_path or upload manually."
        )
        raise FileNotFoundError(msg)
    context.logger.info("Tokenizer artifact verified: %s", yt_artifact_path)


def _tokenizer_try_upload_local(
    context: StageContext,
    *,
    local_artifact_path: str | None,
    yt_artifact_path: str,
    artifact_name: str,
) -> Path | None:
    if not local_artifact_path:
        return None
    return _upload_tokenizer_if_missing(
        context,
        local_artifact_path=local_artifact_path,
        yt_artifact_path=yt_artifact_path,
        artifact_name=artifact_name,
    )


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
    artifact_base = _tokenizer_artifact_base_str(tokenizer_artifact_config)
    if not artifact_base:
        return

    artifact_name = tokenizer_artifact_name_or_raise(
        stage_config=context.config,
        tokenizer_artifact_config=tokenizer_artifact_config,
    )

    archive_name = resolve_tokenizer_archive_name(artifact_name)
    yt_artifact_path = f"{artifact_base}/{archive_name}"
    local_artifact_path = _local_artifact_path_option(tokenizer_artifact_config)

    context.deps.yt_client.create_path(artifact_base, node_type="map_node")
    context.logger.info("Tokenizer artifact directory ready: %s", artifact_base)

    temp_archive: Path | None = None
    try:
        maybe_temp = _tokenizer_try_upload_local(
            context,
            local_artifact_path=local_artifact_path,
            yt_artifact_path=yt_artifact_path,
            artifact_name=artifact_name,
        )
        if maybe_temp is not None:
            temp_archive = maybe_temp

        verify_tokenizer_path_or_raise(context, yt_artifact_path)
    finally:
        if temp_archive and temp_archive.exists():
            temp_archive.unlink(missing_ok=True)
