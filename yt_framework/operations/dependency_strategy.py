"""
Dependency Strategy Pattern
============================

Strategy pattern for building dependencies in deployment mode.

This module provides a clean separation between deployment strategy and operation
preparation logic, following SOLID principles.
"""

import logging
from pathlib import Path
from typing import Protocol, Tuple, List, Optional, Literal

from omegaconf import DictConfig


class DependencyBuilder(Protocol):
    """
    Protocol for dependency building strategies.

    Defines the interface for building dependencies in different deployment modes.
    Each concrete implementation handles a specific deployment strategy.
    """

    def build_dependencies(
        self,
        operation_type: Literal["map", "vanilla"],
        stage_dir: Path,
        build_folder: str,
        operation_config: DictConfig,
        stage_config: DictConfig,
        logger: logging.Logger,
    ) -> Tuple[str, List[Tuple[str, str]], Optional[str]]:
        """
        Build dependencies for an operation.

        Args:
            operation_type: Type of operation ('map' or 'vanilla')
            stage_dir: Path to stage directory
            build_folder: YT build folder path
            operation_config: Operation-specific config (from client.operations.map/vanilla)
            stage_config: Full stage config (for accessing job section)
            logger: Logger instance

        Returns:
            Tuple of (script_path, dependencies, command)
            - script_path: Path to script in YT (or placeholder)
            - dependencies: List of (yt_path, local_path) tuples
            - command: Optional command to execute (for tar mode)
        """
        ...


class TarArchiveDependencyBuilder:
    """
    Tar archive deployment strategy.

    Uploads code as a single tar.gz archive and generates wrapper scripts
    that extract the archive and execute the appropriate script.

    This strategy works for both map and vanilla operations using unified wrapper scripts.
    """

    def build_dependencies(
        self,
        operation_type: Literal["map", "vanilla"],
        stage_dir: Path,
        build_folder: str,
        operation_config: DictConfig,
        stage_config: DictConfig,
        logger: logging.Logger,
    ) -> Tuple[str, List[Tuple[str, str]], Optional[str]]:
        """Build dependencies using tar archive strategy."""

        from yt_framework.utils import log_header

        log_header(
            logger,
            f"{operation_type.title()} Operation",
            "Preparing (Tar Archive Mode)",
        )

        stage_name = stage_dir.name

        # Create bootstrap command that extracts archive and runs wrapper
        bootstrap_command = self._create_bootstrap_command(
            stage_name=stage_name,
            operation_type=operation_type,
            logger=logger,
        )

        # Script path is a placeholder (not used when command is provided)
        if operation_type == "map":
            script_path = f"{build_folder}/stages/{stage_name}/src/mapper.py"
        else:  # vanilla
            script_path = f"{build_folder}/stages/{stage_name}/src/vanilla.py"

        # Dependencies: only the tar archive and optional checkpoint
        dependencies: List[Tuple[str, str]] = []

        # Add tar archive as dependency
        archive_yt_path = f"{build_folder}/code.tar.gz"
        dependencies.append((archive_yt_path, "code.tar.gz"))
        logger.info(f"Added tar archive dependency: {archive_yt_path}")

        # Add checkpoint if configured (for map operations with models)
        if operation_type == "map":
            # Get model_name from stage_config.job.model_name (job is at stage level)
            model_name = None
            if "job" in stage_config and stage_config.job.get("model_name"):
                model_name = stage_config.job.model_name

            # Get checkpoint_base from operation_config.checkpoint.checkpoint_base
            checkpoint_base = None
            if "checkpoint" in operation_config and operation_config.checkpoint.get(
                "checkpoint_base"
            ):
                checkpoint_base = operation_config.checkpoint.checkpoint_base

            if model_name and checkpoint_base:
                checkpoint_file_path = f"{checkpoint_base}/{model_name}"
                dependencies.append((checkpoint_file_path, model_name))
                logger.info(f"Added checkpoint dependency: {checkpoint_file_path}")

        logger.info(f"Total dependencies: {len(dependencies)} files")

        # Escape single quotes in command for bash -c
        escaped_command = bootstrap_command.replace("'", "'\"'\"'")
        command = f"bash -c '{escaped_command}'"

        return script_path, dependencies, command

    def _create_bootstrap_command(
        self,
        stage_name: str,
        operation_type: Literal["map", "vanilla"],
        logger: logging.Logger,
    ) -> str:
        """
        Create bootstrap command for tar archive mode.

        The bootstrap command:
        1. Extracts code.tar.gz archive
        2. Runs the operation-specific wrapper script

        Args:
            stage_name: Name of the stage (e.g., "run_map")
            operation_type: Type of operation ('map' or 'vanilla')
            logger: Logger instance

        Returns:
            Bash command string to execute
        """
        logger.debug(f"Creating bootstrap command for {operation_type} operation")

        # Unified wrapper script naming: operation_wrapper_{stage_name}_{type}.sh
        wrapper_name = f"operation_wrapper_{stage_name}_{operation_type}.sh"

        bootstrap_command = f"""set -e
tar -xzf code.tar.gz
./{wrapper_name}
"""

        return bootstrap_command
