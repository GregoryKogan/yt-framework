"""Checkpoint Utilities for YTsaurus.
==================================

Utilities for saving and loading checkpoints in YTsaurus Cypress file system.
Works without internet access - uses YT's internal distributed file system.
"""

import json
import logging
import os
from typing import Any

import yt.wrapper as yt


def get_checkpoint_path(checkpoint_name: str, base_path: str | None = None) -> str:
    """Get full YT path for a checkpoint.

    Args:
        checkpoint_name: Name of checkpoint file
        base_path: Base YT path (defaults to user's checkpoints folder)

    Returns:
        Full YT path to checkpoint

    """
    if base_path is None:
        # Default to user's checkpoints folder
        # Extract username from YT_PROXY or use default
        username = os.environ.get("YT_USER", "default")
        base_path = f"//home/{username}/checkpoints"

    return f"{base_path}/{checkpoint_name}"


def save_checkpoint(
    data: bytes,
    checkpoint_name: str,
    metadata: dict[str, Any] | None = None,
    base_path: str | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Save checkpoint to YTsaurus Cypress file system.

    Args:
        data: Checkpoint data as bytes
        checkpoint_name: Name of checkpoint file
        metadata: Optional metadata dictionary
        base_path: Base YT path for checkpoints
        logger: Optional logger

    Returns:
        Full YT path to saved checkpoint

    """
    log = logger or logging.getLogger(__name__)

    checkpoint_path = get_checkpoint_path(checkpoint_name, base_path)

    # Ensure directory exists
    base_dir = "/".join(checkpoint_path.split("/")[:-1])
    try:
        yt.create("map_node", base_dir, recursive=True, ignore_existing=True)
    except Exception as e:
        log.warning("Could not create checkpoint directory %s: %s", base_dir, e)

    # Save checkpoint
    try:
        yt.write_file(checkpoint_path, data, force_create=True, compute_md5=True)
        log.info("Saved checkpoint: %s (%s bytes)", checkpoint_path, len(data))
    except Exception:
        log.exception("Failed to save checkpoint %s", checkpoint_path)
        raise

    # Save metadata if provided
    if metadata:
        metadata_path = f"{checkpoint_path}.meta"
        try:
            metadata_json = json.dumps(metadata, indent=2)
            yt.write_file(
                metadata_path,
                metadata_json.encode("utf-8"),
                force_create=True,
                compute_md5=True,
            )
            log.debug("Saved checkpoint metadata: %s", metadata_path)
        except Exception as e:
            log.warning("Failed to save checkpoint metadata: %s", e)

    return checkpoint_path


def load_checkpoint(
    checkpoint_name: str,
    base_path: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[bytes | None, dict[str, Any] | None]:
    """Load checkpoint and metadata from YTsaurus Cypress.

    Args:
        checkpoint_name: Name of checkpoint file
        base_path: Base YT path for checkpoints
        logger: Optional logger

    Returns:
        Tuple of (checkpoint_data, metadata_dict) or (None, None) if not found

    """
    log = logger or logging.getLogger(__name__)

    checkpoint_path = get_checkpoint_path(checkpoint_name, base_path)

    if not yt.exists(checkpoint_path):
        log.debug("Checkpoint not found: %s", checkpoint_path)
        return None, None

    try:
        # Load checkpoint data
        data = yt.read_file(checkpoint_path).read()
        log.info("Loaded checkpoint: %s (%s bytes)", checkpoint_path, len(data))

        # Load metadata if exists
        metadata = None
        metadata_path = f"{checkpoint_path}.meta"
        if yt.exists(metadata_path):
            try:
                metadata_json = yt.read_file(metadata_path).read().decode("utf-8")
                metadata = json.loads(metadata_json)
                log.debug("Loaded checkpoint metadata: %s", metadata_path)
            except Exception as e:
                log.warning("Failed to load checkpoint metadata: %s", e)

        return data, metadata

    except Exception:
        log.exception("Failed to load checkpoint %s", checkpoint_path)
        return None, None


def list_checkpoints(
    base_path: str | None = None,
    pattern: str | None = None,
    logger: logging.Logger | None = None,
) -> list[str]:
    """List available checkpoints in YTsaurus.

    Args:
        base_path: Base YT path for checkpoints
        pattern: Optional pattern to filter checkpoint names
        logger: Optional logger

    Returns:
        List of checkpoint names (without .meta files)

    """
    log = logger or logging.getLogger(__name__)

    checkpoint_dir = get_checkpoint_path("", base_path).rstrip("/")

    if not yt.exists(checkpoint_dir):
        log.debug("Checkpoint directory does not exist: %s", checkpoint_dir)
        return []

    try:
        files = yt.list(checkpoint_dir)
        # Filter out metadata files and apply pattern if provided
        checkpoints = []
        for file in files:
            if file.endswith(".meta"):
                continue
            if pattern and pattern not in file:
                continue
            checkpoints.append(file)

        log.debug("Found %s checkpoints in %s", len(checkpoints), checkpoint_dir)
        return sorted(checkpoints)

    except Exception:
        log.exception("Failed to list checkpoints in %s", checkpoint_dir)
        return []


def delete_checkpoint(
    checkpoint_name: str,
    base_path: str | None = None,
    logger: logging.Logger | None = None,
) -> bool:
    """Delete checkpoint and its metadata from YTsaurus.

    Args:
        checkpoint_name: Name of checkpoint file
        base_path: Base YT path for checkpoints
        logger: Optional logger

    Returns:
        True if deleted successfully, False otherwise

    """
    log = logger or logging.getLogger(__name__)

    checkpoint_path = get_checkpoint_path(checkpoint_name, base_path)

    try:
        if yt.exists(checkpoint_path):
            yt.remove(checkpoint_path, force=True)
            log.info("Deleted checkpoint: %s", checkpoint_path)

        # Delete metadata if exists
        metadata_path = f"{checkpoint_path}.meta"
        if yt.exists(metadata_path):
            yt.remove(metadata_path, force=True)
            log.debug("Deleted checkpoint metadata: %s", metadata_path)

        return True

    except Exception:
        log.exception("Failed to delete checkpoint %s", checkpoint_path)
        return False


def save_processing_state(
    state: dict[str, Any],
    state_name: str = "processing_state",
    base_path: str | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Save processing state (e.g., processed video list, iteration count) to checkpoint.

    Convenience function for saving JSON-serializable state dictionaries.

    Args:
        state: Dictionary with processing state
        state_name: Name for the state checkpoint
        base_path: Base YT path for checkpoints
        logger: Optional logger

    Returns:
        Full YT path to saved checkpoint

    """
    log = logger or logging.getLogger(__name__)

    state_json = json.dumps(state, indent=2)
    checkpoint_name = f"{state_name}.json"

    return save_checkpoint(
        data=state_json.encode("utf-8"),
        checkpoint_name=checkpoint_name,
        metadata={"type": "processing_state", "state_name": state_name},
        base_path=base_path,
        logger=log,
    )


def load_processing_state(
    state_name: str = "processing_state",
    base_path: str | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any] | None:
    """Load processing state from checkpoint.

    Convenience function for loading JSON-serializable state dictionaries.

    Args:
        state_name: Name of the state checkpoint
        base_path: Base YT path for checkpoints
        logger: Optional logger

    Returns:
        State dictionary or None if not found

    """
    log = logger or logging.getLogger(__name__)

    checkpoint_name = f"{state_name}.json"
    data, _metadata = load_checkpoint(checkpoint_name, base_path, log)

    if data is None:
        return None

    try:
        state = json.loads(data.decode("utf-8"))
        log.debug("Loaded processing state: %s", state_name)
        return state
    except Exception:
        log.exception("Failed to parse processing state %s", state_name)
        return None
