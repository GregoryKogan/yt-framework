"""Upload or reuse single-file model checkpoints and wire them into operation specs."""

from pathlib import Path
from typing import TYPE_CHECKING

from omegaconf import DictConfig

if TYPE_CHECKING:
    from yt_framework.core.stage import StageContext


def init_checkpoint_directory(
    context: "StageContext",
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
    # Get checkpoint-related config values from checkpoint_config
    checkpoint_base = checkpoint_config.get("checkpoint_base")
    local_checkpoint_path = checkpoint_config.get("local_checkpoint_path")

    # model_name is still accessed from stage config job section
    model_name = None
    if "job" in context.config and context.config.job.get("model_name"):
        model_name = context.config.job.model_name

    if not checkpoint_base:
        context.logger.warning(
            "No checkpoint_base specified in checkpoint config, skipping checkpoint initialization"
        )
        return

    try:
        # Create checkpoint directory in YT
        context.deps.yt_client.create_path(checkpoint_base, node_type="map_node")
        context.logger.info("Checkpoint directory ready: %s", checkpoint_base)

        # Upload local checkpoint if specified and exists (only if not already in YT)
        if local_checkpoint_path:
            local_path = Path(local_checkpoint_path)
            if local_path.exists():
                checkpoint_name = local_path.name
                yt_checkpoint_path = f"{checkpoint_base}/{checkpoint_name}"

                # Check if checkpoint already exists in YT
                if context.deps.yt_client.exists(yt_checkpoint_path):
                    context.logger.info(
                        "Checkpoint already exists in YT: %s (skipping upload)",
                        yt_checkpoint_path,
                    )
                else:
                    context.logger.info(
                        "Uploading local checkpoint: %s → %s",
                        local_path,
                        yt_checkpoint_path,
                    )
                    try:
                        context.deps.yt_client.upload_file(
                            local_path, yt_checkpoint_path, create_parent_dir=True
                        )
                        context.logger.debug(
                            "Checkpoint uploaded: %s", yt_checkpoint_path
                        )
                    except Exception:
                        context.logger.exception("Failed to upload checkpoint")
                        raise
            else:
                context.logger.warning(
                    "Local checkpoint path does not exist: %s", local_path
                )

        # Validate that required checkpoint exists in YT
        if model_name:
            checkpoint_name = model_name
            yt_checkpoint_path = f"{checkpoint_base}/{checkpoint_name}"

            if not context.deps.yt_client.exists(yt_checkpoint_path):
                error_msg = (
                    f"Required checkpoint not found in YT: {yt_checkpoint_path}\n"
                    f"Please upload the checkpoint using local_checkpoint_path in config, "
                    f"or manually upload {checkpoint_name} to {checkpoint_base}"
                )
                context.logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            context.logger.debug("Required checkpoint verified: %s", yt_checkpoint_path)
        else:
            context.logger.debug(
                "No model_name specified, skipping checkpoint validation"
            )

    except FileNotFoundError:
        raise  # Re-raise checkpoint validation errors
    except Exception:
        context.logger.exception(
            "Could not initialize checkpoint directory %s", checkpoint_base
        )
        raise
