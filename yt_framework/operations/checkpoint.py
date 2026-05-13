"""Upload or reuse single-file model checkpoints and wire them into operation specs."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from omegaconf import DictConfig

if TYPE_CHECKING:
    from yt_framework.operations.stage_contracts import StageContext


def _raise_file_not_found(message: str) -> None:
    raise FileNotFoundError(message)


def _checkpoint_base_from_config(checkpoint_config: DictConfig) -> str | None:
    raw = checkpoint_config.get("checkpoint_base")
    if isinstance(raw, str) and raw.strip():
        return str(raw)
    return None


def _local_checkpoint_path_from_config(checkpoint_config: DictConfig) -> str | None:
    raw = checkpoint_config.get("local_checkpoint_path")
    if isinstance(raw, str):
        return str(raw)
    return None


def _resolve_model_name(context: StageContext) -> str | None:
    job_cfg = context.config.get("job")
    if not isinstance(job_cfg, DictConfig):
        return None
    model_name = job_cfg.get("model_name")
    if isinstance(model_name, str) and model_name.strip():
        return model_name
    return None


def _upload_local_checkpoint_if_needed(
    context: StageContext,
    checkpoint_base: str,
    local_checkpoint_path: str | None,
) -> None:
    if not local_checkpoint_path:
        return

    local_path = Path(local_checkpoint_path)
    if not local_path.exists():
        context.logger.warning(
            "Local checkpoint path does not exist: %s",
            local_path,
        )
        return

    checkpoint_name = local_path.name
    yt_checkpoint_path = f"{checkpoint_base}/{checkpoint_name}"
    if context.deps.yt_client.exists(yt_checkpoint_path):
        context.logger.info(
            "Checkpoint already exists in YT: %s (skipping upload)",
            yt_checkpoint_path,
        )
        return

    context.logger.info(
        "Uploading local checkpoint: %s → %s",
        local_path,
        yt_checkpoint_path,
    )
    context.deps.yt_client.upload_file(
        local_path,
        yt_checkpoint_path,
        create_parent_dir=True,
    )
    context.logger.debug("Checkpoint uploaded: %s", yt_checkpoint_path)


def _validate_required_checkpoint(
    context: StageContext,
    checkpoint_base: str,
    model_name: str | None,
) -> None:
    if not model_name:
        context.logger.debug("No model_name specified, skipping checkpoint validation")
        return

    yt_checkpoint_path = f"{checkpoint_base}/{model_name}"
    if context.deps.yt_client.exists(yt_checkpoint_path):
        context.logger.debug("Required checkpoint verified: %s", yt_checkpoint_path)
        return

    error_msg = (
        f"Required checkpoint not found in YT: {yt_checkpoint_path}\n"
        "Please upload the checkpoint using local_checkpoint_path in config, "
        f"or manually upload {model_name} to {checkpoint_base}"
    )
    context.logger.error(error_msg)
    _raise_file_not_found(error_msg)


def init_checkpoint_directory(
    context: StageContext,
    checkpoint_config: DictConfig,
) -> None:
    """Initialize checkpoint directory in YTsaurus if it doesn't exist.

    Uses checkpoint_base from checkpoint_config. Also uploads local checkpoint if specified.
    Validates that required checkpoint exists in YT before proceeding.

    Args:
        context: Stage context (provides deps, logger)
        checkpoint_config: Checkpoint-specific config (from client.operations.<op>.checkpoint)

    Returns:
        None

    Raises:
        FileNotFoundError: If required checkpoint not found in YT
        Exception: If checkpoint initialization fails

    """
    checkpoint_base = _checkpoint_base_from_config(checkpoint_config)
    local_checkpoint_path = _local_checkpoint_path_from_config(checkpoint_config)

    model_name = _resolve_model_name(context)

    if not checkpoint_base:
        context.logger.warning(
            "No checkpoint_base specified in checkpoint config, skipping checkpoint initialization",
        )
        return

    try:
        # Create checkpoint directory in YT
        context.deps.yt_client.create_path(checkpoint_base, node_type="map_node")
        context.logger.info("Checkpoint directory ready: %s", checkpoint_base)

        _upload_local_checkpoint_if_needed(
            context=context,
            checkpoint_base=checkpoint_base,
            local_checkpoint_path=local_checkpoint_path,
        )
        _validate_required_checkpoint(
            context=context,
            checkpoint_base=checkpoint_base,
            model_name=model_name,
        )

    except FileNotFoundError:
        raise  # Re-raise checkpoint validation errors
    except Exception:
        context.logger.exception(
            "Could not initialize checkpoint directory %s",
            checkpoint_base,
        )
        raise
