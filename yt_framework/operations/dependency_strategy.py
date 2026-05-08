"""Dependency strategies for deployment-mode operation preparation.

Separates deployment concerns from operation preparation using the strategy
pattern (SOLID).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol

from omegaconf import DictConfig, ListConfig

from yt_framework.operations.job_command import (
    map_reduce_leg_kind,
    require_consistent_map_reduce_legs,
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
from yt_framework.utils import log_header

if TYPE_CHECKING:
    import logging
    from pathlib import Path

_FILE_PATH_PAIR_MIN_LEN = 2


@dataclass
class DependencyBuildResult:
    """Result of tar-dependency preparation for an operation."""

    script_path: str
    dependencies: list[tuple[str, str]]
    command: str | None
    """Bootstrap command for single-leg map/vanilla (``bash -c '...'``)."""
    mapper_command: str | None = None
    """When set, map-reduce mapper leg should use this string (tar + wrapper)."""
    reducer_command: str | None = None
    """Map-reduce reducer or reduce-only leg string (tar + wrapper) when set."""


class DependencyBuilder(Protocol):
    """Protocol for dependency building strategies.

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
        mapper: object = None,
        reducer: object = None,
    ) -> DependencyBuildResult:
        """Build dependencies for an operation.

        Optional ``mapper`` / ``reducer`` are used for map_reduce / reduce tar command
        bootstrap when ``tar_command_bootstrap`` is enabled in ``operation_config``.
        """


class TarArchiveDependencyBuilder:
    """Tar archive deployment strategy.

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
        mapper: object = None,
        reducer: object = None,
    ) -> DependencyBuildResult:
        """Build dependencies using tar archive strategy."""
        effective_type = (
            "map" if operation_type in ("map_reduce", "reduce") else operation_type
        )
        log_header(
            logger,
            f"{operation_type.replace('_', ' ').title()} Operation",
            "Preparing (Tar Archive Mode)",
        )

        stage_name = stage_dir.name

        bootstrap_command, mapper_command, reducer_command = (
            self._resolve_operation_commands(
                operation_type=operation_type,
                stage_name=stage_name,
                archive_name=archive_name,
                operation_config=operation_config,
                logger=logger,
                mapper=mapper,
                reducer=reducer,
            )
        )

        # Script path is a placeholder (not used when command is provided)
        if effective_type == "map":
            script_path = f"{build_folder}/stages/{stage_name}/src/mapper.py"
        else:
            script_path = f"{build_folder}/stages/{stage_name}/src/vanilla.py"

        dependencies = self._build_dependencies(
            effective_type=effective_type,
            build_folder=build_folder,
            archive_name=archive_name,
            operation_config=operation_config,
            stage_config=stage_config,
            logger=logger,
        )

        logger.info("Total dependencies: %s files", len(dependencies))

        if operation_type in ("map", "vanilla"):
            escaped_command = bootstrap_command.replace("'", "'\"'\"'")
            command: str | None = f"bash -c '{escaped_command}'"
        else:
            command = None

        return DependencyBuildResult(
            script_path=script_path,
            dependencies=dependencies,
            command=command,
            mapper_command=mapper_command,
            reducer_command=reducer_command,
        )

    def _resolve_operation_commands(
        self,
        operation_type: Literal["map", "vanilla", "map_reduce", "reduce"],
        stage_name: str,
        archive_name: str,
        operation_config: DictConfig,
        logger: logging.Logger,
        *,
        mapper: object,
        reducer: object,
    ) -> tuple[str, str | None, str | None]:
        tar_bootstrap_flag = bool(operation_config.get("tar_command_bootstrap", False))
        if operation_type == "map_reduce":
            return self._map_reduce_commands(
                stage_name=stage_name,
                archive_name=archive_name,
                logger=logger,
                tar_bootstrap_flag=tar_bootstrap_flag,
                mapper=mapper,
                reducer=reducer,
            )
        if operation_type == "reduce":
            return self._reduce_commands(
                stage_name=stage_name,
                archive_name=archive_name,
                logger=logger,
                tar_bootstrap_flag=tar_bootstrap_flag,
                reducer=reducer,
            )
        bootstrap_command = self._create_bootstrap_command(
            stage_name=stage_name,
            operation_type=operation_type,
            archive_name=archive_name,
            logger=logger,
        )
        return bootstrap_command, None, None

    def _map_reduce_commands(
        self,
        stage_name: str,
        archive_name: str,
        logger: logging.Logger,
        *,
        tar_bootstrap_flag: bool,
        mapper: object,
        reducer: object,
    ) -> tuple[str, str | None, str | None]:
        mapper_command: str | None = None
        reducer_command: str | None = None
        if mapper is not None and reducer is not None:
            require_consistent_map_reduce_legs(mapper, reducer)
        if (
            tar_bootstrap_flag
            and mapper is not None
            and reducer is not None
            and map_reduce_leg_kind(mapper) == "command"
        ):
            w_m, w_r = map_reduce_wrapper_names(stage_name)
            inner_m = bootstrap_shell_run_wrapper(archive_name, w_m, logger)
            inner_r = bootstrap_shell_run_wrapper(archive_name, w_r, logger)
            mapper_command = wrap_bootstrap_as_bash_c(inner_m)
            reducer_command = wrap_bootstrap_as_bash_c(inner_r)
            logger.info(
                "tar_command_bootstrap enabled: map-reduce legs use tar extract + %s / %s",
                w_m,
                w_r,
            )
        return "", mapper_command, reducer_command

    def _reduce_commands(
        self,
        stage_name: str,
        archive_name: str,
        logger: logging.Logger,
        *,
        tar_bootstrap_flag: bool,
        reducer: object,
    ) -> tuple[str, str | None, str | None]:
        if tar_bootstrap_flag and reducer is not None and isinstance(reducer, str):
            wrapper = reduce_wrapper_name(stage_name)
            inner = bootstrap_shell_run_wrapper(archive_name, wrapper, logger)
            reducer_command = wrap_bootstrap_as_bash_c(inner)
            logger.info(
                "tar_command_bootstrap enabled: reduce leg uses tar extract + %s",
                wrapper,
            )
            return "", None, reducer_command
        return "", None, None

    def _build_dependencies(
        self,
        *,
        effective_type: Literal["map", "vanilla"],
        build_folder: str,
        archive_name: str,
        operation_config: DictConfig,
        stage_config: DictConfig,
        logger: logging.Logger,
    ) -> list[tuple[str, str]]:
        dependencies: list[tuple[str, str]] = []
        self._append_tar_archive_dependency(
            dependencies=dependencies,
            build_folder=build_folder,
            archive_name=archive_name,
            logger=logger,
        )
        self._append_file_path_dependencies(
            dependencies=dependencies,
            operation_config=operation_config,
            logger=logger,
        )
        self._append_checkpoint_dependency(
            dependencies=dependencies,
            effective_type=effective_type,
            operation_config=operation_config,
            stage_config=stage_config,
            logger=logger,
        )
        self._append_tokenizer_dependency(
            dependencies=dependencies,
            operation_config=operation_config,
            stage_config=stage_config,
            logger=logger,
        )
        return dependencies

    def _append_tar_archive_dependency(
        self,
        *,
        dependencies: list[tuple[str, str]],
        build_folder: str,
        archive_name: str,
        logger: logging.Logger,
    ) -> None:
        archive_yt_path = f"{build_folder}/{archive_name}"
        dependencies.append((archive_yt_path, archive_name))
        logger.info("Added tar archive dependency: %s", archive_yt_path)

    def _append_file_path_dependencies(
        self,
        *,
        dependencies: list[tuple[str, str]],
        operation_config: DictConfig,
        logger: logging.Logger,
    ) -> None:
        for item in operation_config.get("file_paths") or []:
            if (
                isinstance(item, (list, tuple, ListConfig))
                and len(item) >= _FILE_PATH_PAIR_MIN_LEN
            ):
                yt_path, local_path = item[0], item[1]
                dependencies.append((yt_path, local_path))
                logger.info("Added file dependency: %s", yt_path)
                continue
            if isinstance(item, str):
                dependencies.append((item, item.split("/")[-1]))
                logger.info("Added file dependency: %s", item)

    def _append_checkpoint_dependency(
        self,
        *,
        dependencies: list[tuple[str, str]],
        effective_type: Literal["map", "vanilla"],
        operation_config: DictConfig,
        stage_config: DictConfig,
        logger: logging.Logger,
    ) -> None:
        if effective_type != "map":
            return
        model_name = None
        if "job" in stage_config and stage_config.job.get("model_name"):
            model_name = stage_config.job.model_name
        checkpoint_base = None
        if "checkpoint" in operation_config and operation_config.checkpoint.get(
            "checkpoint_base",
        ):
            checkpoint_base = operation_config.checkpoint.checkpoint_base
        if not (model_name and checkpoint_base):
            return
        checkpoint_file_path = f"{checkpoint_base}/{model_name}"
        dependencies.append((checkpoint_file_path, model_name))
        logger.info("Added checkpoint dependency: %s", checkpoint_file_path)

    def _append_tokenizer_dependency(
        self,
        *,
        dependencies: list[tuple[str, str]],
        operation_config: DictConfig,
        stage_config: DictConfig,
        logger: logging.Logger,
    ) -> None:
        tokenizer_cfg = operation_config.get("tokenizer_artifact")
        if not (tokenizer_cfg and tokenizer_cfg.get("artifact_base")):
            return
        artifact_name = resolve_tokenizer_artifact_name(
            stage_config=stage_config,
            tokenizer_artifact_config=tokenizer_cfg,
        )
        if not artifact_name:
            logger.warning(
                "tokenizer_artifact configured but artifact_name cannot be resolved; "
                "skipping dependency mount",
            )
            return
        artifact_archive = resolve_tokenizer_archive_name(artifact_name)
        artifact_path = f"{tokenizer_cfg.artifact_base}/{artifact_archive}"
        dependencies.append((artifact_path, artifact_archive))
        logger.info("Added tokenizer artifact dependency: %s", artifact_path)

    def _create_bootstrap_command(
        self,
        stage_name: str,
        operation_type: Literal["map", "vanilla"],
        archive_name: str,
        logger: logging.Logger,
    ) -> str:
        """Create bootstrap command for tar archive mode.

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
        logger.debug("Creating bootstrap command for %s operation", operation_type)

        # Unified wrapper script naming: operation_wrapper_{stage_name}_{type}.sh
        wrapper_name = f"operation_wrapper_{stage_name}_{operation_type}.sh"

        return f"""set -e
tar -xzf {archive_name}
./{wrapper_name}
"""
