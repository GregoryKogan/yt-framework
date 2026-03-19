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
        operation_type: Literal["map", "vanilla", "map_reduce", "reduce"],
        stage_dir: Path,
        archive_name: str,
        build_folder: str,
        operation_config: DictConfig,
        stage_config: DictConfig,
        logger: logging.Logger,
    ) -> Tuple[str, List[Tuple[str, str]], Optional[str]]:
        """
        Build dependencies for an operation.

        Args:
            operation_type: Type of operation ('map', 'vanilla', 'map_reduce', or 'reduce')
            stage_dir: Path to stage directory
            archive_name: Name of the archive (e.g., "source.tar.gz")
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


class TarArchiveDependencyBuilder:
    """
    Tar archive deployment strategy.

    Uploads code as a single tar.gz archive and generates wrapper scripts
    that extract the archive and execute the appropriate script.

    This strategy works for both map and vanilla operations using unified wrapper scripts.
    """

    def build_dependencies(
        self,
        operation_type: Literal["map", "vanilla", "map_reduce", "reduce"],
        stage_dir: Path,
        archive_name: str,
        build_folder: str,
        operation_config: DictConfig,
        stage_config: DictConfig,
        logger: logging.Logger,
    ) -> Tuple[str, List[Tuple[str, str]], Optional[str]]:
        """Build dependencies using tar archive strategy.
        
        Creates a tar archive deployment where all code is packaged into a single
        tar.gz file. Generates a bootstrap command that extracts the archive
        and executes the appropriate wrapper script.
        
        Args:
            operation_type: Type of operation ('map', 'vanilla', 'map_reduce', or 'reduce').
            stage_dir: Path to stage directory containing src/ folder.
            build_folder: YT build folder path where archive will be stored.
            operation_config: Operation-specific config (from client.operations.map/vanilla).
            stage_config: Full stage config (for accessing job.model_name for checkpoints).
            logger: Logger instance for logging dependency preparation.
            
        Returns:
            Tuple containing:
            - script_path: Placeholder path to script in YT (not used when command provided).
            - dependencies: List of (yt_path, local_path) tuples including:
              * tar.gz archive
              * Optional checkpoint file if configured
            - command: Bootstrap command string that extracts archive and runs wrapper.
        """

        from yt_framework.utils import log_header

        effective_type = "map" if operation_type in ("map_reduce", "reduce") else operation_type
        log_header(
            logger,
            f"{operation_type.replace('_', ' ').title()} Operation",
            "Preparing (Tar Archive Mode)",
        )

        stage_name = stage_dir.name

        # For map_reduce/reduce we only need dependencies (no command); archive same as map
        if operation_type in ("map_reduce", "reduce"):
            bootstrap_command = ""
        else:
            bootstrap_command = self._create_bootstrap_command(
                stage_name=stage_name,
                operation_type=operation_type,
                archive_name=archive_name,
                logger=logger,
            )

        # Script path is a placeholder (not used when command is provided)
        if effective_type == "map":
            script_path = f"{build_folder}/stages/{stage_name}/src/mapper.py"
        else:
            script_path = f"{build_folder}/stages/{stage_name}/src/vanilla.py"

        # Dependencies: tar archive and optional checkpoint / extra file_paths
        dependencies: List[Tuple[str, str]] = []

        # Add tar archive as dependency
        archive_yt_path = f"{build_folder}/{archive_name}"
        dependencies.append((archive_yt_path, archive_name))
        logger.info(f"Added tar archive dependency: {archive_yt_path}")

        # Add extra file_paths from operation_config (e.g. secrets, extra files)
        for item in operation_config.get("file_paths") or []:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                yt_path, local_path = item[0], item[1]
                dependencies.append((yt_path, local_path))
                logger.info(f"Added file dependency: {yt_path}")
            elif isinstance(item, str):
                dependencies.append((item, item.split("/")[-1]))
                logger.info(f"Added file dependency: {item}")

        # Add checkpoint if configured (for map and map_reduce operations with models)
        if effective_type in ("map", "map_reduce"):
            model_name = None
            if "job" in stage_config and stage_config.job.get("model_name"):
                model_name = stage_config.job.model_name
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

        if operation_type in ("map_reduce", "reduce"):
            command = None
        else:
            escaped_command = bootstrap_command.replace("'", "'\"'\"'")
            command = f"bash -c '{escaped_command}'"

        return script_path, dependencies, command

    def _create_bootstrap_command(
        self,
        stage_name: str,
        operation_type: Literal["map", "vanilla"],
        archive_name: str,
        logger: logging.Logger,
    ) -> str:
        """
        Create bootstrap command for tar archive mode.

        The bootstrap command:
        1. Extracts tar.gz archive
        2. Runs the operation-specific wrapper script

        Args:
            stage_name: Name of the stage (e.g., "run_map")
            operation_type: Type of operation ('map', 'vanilla', 'map_reduce', or 'reduce')
            archive_name: Name of the archive (e.g., "source.tar.gz")
            logger: Logger instance

        Returns:
            Bash command string to execute
        """
        logger.debug(f"Creating bootstrap command for {operation_type} operation")

        # Unified wrapper script naming: operation_wrapper_{stage_name}_{type}.sh
        wrapper_name = f"operation_wrapper_{stage_name}_{operation_type}.sh"

        bootstrap_command = f"""set -e
tar -xzf {archive_name}
./{wrapper_name}
"""

        return bootstrap_command
