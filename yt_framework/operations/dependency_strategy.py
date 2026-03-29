"""
Dependency Strategy Pattern
============================

Strategy pattern for building dependencies in deployment mode.

This module provides a clean separation between deployment strategy and operation
preparation logic, following SOLID principles.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Tuple, List, Optional, Literal

from omegaconf import DictConfig, ListConfig

from yt_framework.operations.job_command import (
    require_consistent_map_reduce_legs,
    map_reduce_leg_kind,
)
from yt_framework.operations.tar_command_wiring import (
    bootstrap_shell_run_wrapper,
    map_reduce_wrapper_names,
    reduce_wrapper_name,
    wrap_bootstrap_as_bash_c,
)
from yt_framework.operations.tokenizer_artifact import (
    resolve_tokenizer_archive_name,
    resolve_tokenizer_artifact_name,
)


@dataclass
class DependencyBuildResult:
    """Result of tar-dependency preparation for an operation."""

    script_path: str
    dependencies: List[Tuple[str, str]]
    command: Optional[str]
    """Bootstrap command for single-leg map/vanilla (``bash -c '...'``)."""
    mapper_command: Optional[str] = None
    """When set, map-reduce mapper leg should use this string (tar + wrapper)."""
    reducer_command: Optional[str] = None
    """Map-reduce reducer or reduce-only leg string (tar + wrapper) when set."""


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
        *,
        mapper: Any = None,
        reducer: Any = None,
    ) -> DependencyBuildResult:
        """
        Build dependencies for an operation.

        Optional ``mapper`` / ``reducer`` are used for map_reduce / reduce tar command
        bootstrap when ``tar_command_bootstrap`` is enabled in ``operation_config``.
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
        *,
        mapper: Any = None,
        reducer: Any = None,
    ) -> DependencyBuildResult:
        """Build dependencies using tar archive strategy."""
        from yt_framework.utils import log_header

        effective_type = (
            "map" if operation_type in ("map_reduce", "reduce") else operation_type
        )
        log_header(
            logger,
            f"{operation_type.replace('_', ' ').title()} Operation",
            "Preparing (Tar Archive Mode)",
        )

        stage_name = stage_dir.name

        tar_bootstrap_flag = bool(operation_config.get("tar_command_bootstrap", False))

        mapper_command: Optional[str] = None
        reducer_command: Optional[str] = None

        if operation_type == "map_reduce":
            if mapper is not None and reducer is not None:
                require_consistent_map_reduce_legs(mapper, reducer)
            if tar_bootstrap_flag and mapper is not None and reducer is not None:
                if map_reduce_leg_kind(mapper) == "command":
                    w_m, w_r = map_reduce_wrapper_names(stage_name)
                    inner_m = bootstrap_shell_run_wrapper(archive_name, w_m, logger)
                    inner_r = bootstrap_shell_run_wrapper(archive_name, w_r, logger)
                    mapper_command = wrap_bootstrap_as_bash_c(inner_m)
                    reducer_command = wrap_bootstrap_as_bash_c(inner_r)
                    logger.info(
                        "tar_command_bootstrap enabled: map-reduce legs use tar extract + "
                        f"{w_m} / {w_r}"
                    )
            bootstrap_command = ""
        elif operation_type == "reduce":
            if tar_bootstrap_flag and reducer is not None and isinstance(reducer, str):
                w = reduce_wrapper_name(stage_name)
                inner = bootstrap_shell_run_wrapper(archive_name, w, logger)
                reducer_command = wrap_bootstrap_as_bash_c(inner)
                logger.info(
                    f"tar_command_bootstrap enabled: reduce leg uses tar extract + {w}"
                )
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
            if isinstance(item, (list, tuple, ListConfig)) and len(item) >= 2:
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

        # Add tokenizer artifact if configured (mounted as a .tar.gz file)
        tokenizer_cfg = operation_config.get("tokenizer_artifact")
        if tokenizer_cfg and tokenizer_cfg.get("artifact_base"):
            artifact_name = resolve_tokenizer_artifact_name(
                stage_config=stage_config,
                tokenizer_artifact_config=tokenizer_cfg,
            )
            if artifact_name:
                archive_name = resolve_tokenizer_archive_name(artifact_name)
                artifact_path = f"{tokenizer_cfg.artifact_base}/{archive_name}"
                dependencies.append((artifact_path, archive_name))
                logger.info(f"Added tokenizer artifact dependency: {artifact_path}")
            else:
                logger.warning(
                    "tokenizer_artifact configured but artifact_name cannot be resolved; "
                    "skipping dependency mount"
                )

        logger.info(f"Total dependencies: {len(dependencies)} files")

        if operation_type in ("map", "vanilla"):
            escaped_command = bootstrap_command.replace("'", "'\"'\"'")
            command: Optional[str] = f"bash -c '{escaped_command}'"
        else:
            command = None

        return DependencyBuildResult(
            script_path=script_path,
            dependencies=dependencies,
            command=command,
            mapper_command=mapper_command,
            reducer_command=reducer_command,
        )

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
            operation_type: Type of operation ('map' or 'vanilla')
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
