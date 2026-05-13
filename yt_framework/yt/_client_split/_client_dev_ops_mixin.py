"""Upload and map/reduce/vanilla/sort methods for dev YT client."""

# pyright: reportAttributeAccessIssue=false, reportUnusedImport=false

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, cast

from omegaconf import DictConfig, ListConfig, OmegaConf

from yt_framework.job_command import is_typed_job, resolve_aliased_job
from yt_framework.yt._client_dev_runtime import (
    dev_apply_first_stage_checkpoint_fallback,
    dev_find_checkpoint_in_config,
    dev_pythonpath_entries_for_dev_env,
    dev_resolve_ytjobs_source,
    dev_rewrite_build_path_in_command,
    dev_run_map_subprocess,
    dev_run_vanilla_subprocess,
    dev_scan_stages_dir_for_checkpoint,
    dev_try_upload_one_dependency,
)
from yt_framework.yt._client_split.dev_operation import DevOperation
from yt_framework.yt.operation_secure_env import pop_secure_env_client_kwargs

if TYPE_CHECKING:
    from yt.wrapper import Operation
    from yt.wrapper.schema import TableSchema

    from yt_framework.yt.client_base import OperationResources

_DEV_BUILD_SPLIT_PARTS = 2


class ClientDevOpsMixin:
    """Mixin providing dev-mode uploads and job subprocess runners."""

    def upload_file(
        self,
        local_path: Path,
        yt_path: str,
        *,
        create_parent_dir: bool = False,
    ) -> None:
        """Upload a file to YT (no-op in dev mode).

        Args:
            local_path: Local file path to upload
            yt_path: YT destination path
            create_parent_dir: If True, create parent directory if it doesn't exist (default: False)

        """
        self.logger.debug("Dev: upload_file no-op %s → %s", local_path.name, yt_path)

    def upload_directory(
        self,
        local_dir: Path,
        yt_dir: str,
        pattern: str = "*",
    ) -> list[str]:
        """Upload a directory to YT (no-op in dev mode).

        Args:
            local_dir: Local directory path to upload.
            yt_dir: YT destination directory path.
            pattern: File pattern to match (not used in dev mode).

        Returns:
            List[str]: Empty list in dev mode.

        """
        self.logger.debug("Dev: upload_directory no-op %s → %s", local_dir, yt_dir)
        return []

    def run_map(
        self,
        command: object,
        input_table: str,
        output_table: str,
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: TableSchema | None = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: object = None,
        *,
        append: bool = False,
        **kwargs: object,
    ) -> Operation:
        """Run a map operation locally using subprocess.

        In dev mode, executes the mapper script locally with input/output tables
        as JSONL files. The command is executed in a temporary sandbox directory
        with all dependencies available.

        Args:
            command: Mapper job (command string in dev mode).
            input_table: Input YT table path (read from local JSONL).
            output_table: Output YT table path (written to local JSONL).
            files: List of (yt_path, local_path) tuples for dependencies.
            resources: Operation resource configuration (not fully used in dev mode).
            env: Environment variables dictionary.
            output_schema: Optional output table schema (not used in dev mode).
            max_failed_jobs: Maximum failed jobs allowed (not used in dev mode).
            docker_auth: Optional Docker authentication (not used in dev mode).
            job: Mapper command string when set; otherwise ``command`` is used.
            append: If True and output JSONL exists, append mapper stdout lines to it.
            **kwargs: Additional arguments (not used in dev mode).

        Returns:
            Operation: Mock operation object that simulates YT operation.

        Example:
            >>> op = client.run_map(
            ...     command="python3 mapper.py",
            ...     input_table="//tmp/input",
            ...     output_table="//tmp/output",
            ...     files=[],
            ...     resources=OperationResources(),
            ...     env={}
            ... )

        """
        self._pipeline_dir_or_raise()
        _kw = dict(kwargs)
        pop_secure_env_client_kwargs(_kw)

        self.logger.info("Submitting map operation")
        self.logger.info("  Input: %s", input_table)
        self.logger.info("  Output: %s", output_table)
        mapper_job = job if job is not None else command
        self.logger.info("  Command: %s", mapper_job)
        if not isinstance(mapper_job, str):
            msg = (
                "Dev mode run_map supports only string commands; "
                "TypedJob mappers are supported in prod mode."
            )
            raise NotImplementedError(msg)

        # Prepare sandbox and input/output files
        sandbox_dir, sandbox_input, sandbox_output = self._prepare_map_sandbox(
            input_table,
            output_table,
        )

        # Copy files to sandbox
        self._upload_files(files, sandbox_dir)

        # Setup environment
        env_merged = self._setup_map_environment(env)

        logs_path = self._dev_dir() / f"{self._table_basename(output_table)}.log"
        rc, err_hint = dev_run_map_subprocess(
            mapper_job=mapper_job,
            sandbox_dir=sandbox_dir,
            sandbox_input=sandbox_input,
            sandbox_output=sandbox_output,
            env_merged=env_merged,
            logs_path=logs_path,
            append=append,
            output_table_local_path=self._table_local_path(output_table),
        )
        return cast("Operation", DevOperation(rc, err_hint))

    def run_vanilla(
        self,
        command: object,
        files: list[tuple[str, str]],
        env: dict[str, str],
        task_name: str = "main",
        job: object = None,
        **kwargs: object,
    ) -> Operation | None:
        """Run a vanilla operation locally using subprocess.

        In dev mode, executes the vanilla script locally in a temporary sandbox
        directory with all dependencies available. No input/output tables are involved.

        Args:
            command: Command to execute (typically bash command with script path).
            files: List of (yt_path, local_path) tuples for dependencies.
            env: Environment variables dictionary.
            task_name: Task name for logging (default: "main").
            job: Command string when set; otherwise ``command`` is executed.
            **kwargs: Additional arguments (not used in dev mode).

        Returns:
            Operation: Mock operation object that simulates YT operation.

        """
        self.logger.info("Submitting vanilla operation")
        _kw = dict(kwargs)
        pop_secure_env_client_kwargs(_kw)
        vanilla_job = str(job) if job is not None else str(command)
        self.logger.info("  Command: %s", vanilla_job)
        self.logger.info("  Task: %s", task_name)

        self._pipeline_dir_or_raise()
        self._dev_dir().mkdir(parents=True, exist_ok=True)

        sandbox_dir = self._dev_dir() / f"{task_name}_sandbox"
        sandbox_dir.mkdir(parents=True, exist_ok=True)
        self._upload_files(files, sandbox_dir)

        # Copy config.yaml to the correct location in sandbox if it exists
        # config.yaml dependency has local_name="config.yaml" but should be at stages/{task_name}/config.yaml
        pd = self._pipeline_dir_or_raise()
        stage_config_source = pd / "stages" / task_name / "config.yaml"
        if stage_config_source.exists():
            stage_config_dest = sandbox_dir / "stages" / task_name / "config.yaml"
            stage_config_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(stage_config_source, stage_config_dest)
            self.logger.debug("  Dev: copied config.yaml to %s", stage_config_dest)

        # Convert YT paths in command to local sandbox paths
        local_command = dev_rewrite_build_path_in_command(
            vanilla_job,
            build_split_parts=_DEV_BUILD_SPLIT_PARTS,
            logger=self.logger,
        )
        if local_command != vanilla_job:
            self.logger.debug(
                "  Dev: converted command: %s -> %s",
                vanilla_job,
                local_command,
            )

        logs_path = self._dev_dir() / f"{task_name}.log"

        # Set up environment with JOB_CONFIG_PATH pointing to the config file in sandbox
        env_merged = self._build_env(env)
        config_path_in_sandbox = sandbox_dir / "stages" / task_name / "config.yaml"
        if config_path_in_sandbox.exists():
            env_merged["JOB_CONFIG_PATH"] = str(config_path_in_sandbox)
            self.logger.debug("  Dev: JOB_CONFIG_PATH=%s", config_path_in_sandbox)
        else:
            self.logger.warning(
                "  Dev: config file not found at %s",
                config_path_in_sandbox,
            )

        self.logger.info("  Dev: sandbox=%s", sandbox_dir)
        self.logger.info("  Dev: stderr=%s", logs_path)
        rc, err_hint = dev_run_vanilla_subprocess(
            local_command=local_command,
            sandbox_dir=sandbox_dir,
            env_merged=env_merged,
            logs_path=logs_path,
        )
        return cast("Operation", DevOperation(rc, err_hint))

    def run_map_reduce(
        self,
        mapper: object,
        reducer: object,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        sort_by: list[str] | None = None,
        output_schema: TableSchema | None = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        map_job: object = None,
        reduce_job: object = None,
        **kwargs: object,
    ) -> Operation:
        """Dev: no-op; copy input table to output table."""
        _kw = dict(kwargs)
        pop_secure_env_client_kwargs(_kw)
        mapper_leg = resolve_aliased_job(
            legacy_name="mapper",
            legacy_value=mapper,
            preferred_name="map_job",
            preferred_value=map_job,
        )
        reducer_leg = resolve_aliased_job(
            legacy_name="reducer",
            legacy_value=reducer,
            preferred_name="reduce_job",
            preferred_value=reduce_job,
        )

        def _leg_desc(obj: object) -> str:
            if is_typed_job(obj):
                return "TypedJob"
            if isinstance(obj, str):
                return "command (prod uses JsonFormat on this leg)"
            return f"invalid leg type {type(obj).__name__} (expected TypedJob or str)"

        self.logger.info(
            "Dev: map-reduce mapper leg: %s; reducer leg: %s",
            _leg_desc(mapper_leg),
            _leg_desc(reducer_leg),
        )
        self.logger.info("Dev: map-reduce no-op (copying input -> output)")
        self._pipeline_dir_or_raise()
        self._dev_dir().mkdir(parents=True, exist_ok=True)
        input_path = self._table_local_path(input_table)
        output_path = self._table_local_path(output_table)
        if input_path.exists():
            shutil.copy2(input_path, output_path)
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("")
        return cast("Operation", DevOperation(0))

    def run_reduce(
        self,
        reducer: object,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: TableSchema | None = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: object = None,
        **kwargs: object,
    ) -> Operation:
        """Dev: no-op; copy input table to output table."""
        _kw = dict(kwargs)
        pop_secure_env_client_kwargs(_kw)
        reducer_leg = resolve_aliased_job(
            legacy_name="reducer",
            legacy_value=reducer,
            preferred_name="job",
            preferred_value=job,
        )
        if is_typed_job(reducer_leg):
            rdesc = "TypedJob"
        elif isinstance(reducer_leg, str):
            rdesc = "command (prod uses JsonFormat on this leg)"
        else:
            rdesc = f"invalid leg type {type(reducer_leg).__name__} (expected TypedJob or str)"
        self.logger.info("Dev: reduce leg: %s", rdesc)
        self.logger.info("Dev: reduce no-op (copying input -> output)")
        self._pipeline_dir_or_raise()
        self._dev_dir().mkdir(parents=True, exist_ok=True)
        input_path = self._table_local_path(input_table)
        output_path = self._table_local_path(output_table)
        if input_path.exists():
            shutil.copy2(input_path, output_path)
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("")
        return cast("Operation", DevOperation(0))

    def run_sort(
        self,
        table_path: str,
        sort_by: list[str],
        pool: str | None = None,
        pool_tree: str | None = None,
        **kwargs: object,
    ) -> None:
        """Dev: no-op (table unchanged)."""
        self.logger.info("Dev: run_sort no-op for %s by %s", table_path, sort_by)

    def _build_env(self, env: dict[str, str]) -> dict[str, str]:
        """Build environment variables for subprocess."""
        env_merged = {**os.environ, **(env or {})}
        parts = dev_pythonpath_entries_for_dev_env(
            self._pipeline_dir_or_raise(),
            env_merged,
        )
        env_merged["PYTHONPATH"] = os.pathsep.join(parts)
        return env_merged

    def _try_copy_checkpoint_file(
        self,
        *,
        yt_path: str,
        local_name: str,
        sandbox_dir: Path,
        local_checkpoint_path: str | None,
    ) -> bool:
        if not local_checkpoint_path:
            return False
        checkpoint_filename = Path(local_checkpoint_path).name
        yt_filename = Path(yt_path).name
        if checkpoint_filename not in (yt_filename, local_name):
            return False
        checkpoint_path = Path(local_checkpoint_path)
        if not checkpoint_path.exists():
            self.logger.warning(
                "  Dev: checkpoint path does not exist: %s",
                checkpoint_path,
            )
            return False
        dest_file = sandbox_dir / local_name
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(
            "  Dev: copying checkpoint %s -> %s",
            checkpoint_path,
            dest_file,
        )
        shutil.copy2(checkpoint_path, dest_file)
        return True

    def _try_copy_tarball_from_build(
        self,
        *,
        yt_path: str,
        local_name: str,
        sandbox_dir: Path,
    ) -> bool:
        if not yt_path.endswith(".tar.gz"):
            return False
        local_build = self._pipeline_dir_or_raise() / ".build"
        if not local_build.exists():
            return False
        source_file = local_build / Path(yt_path).name
        if not source_file.exists():
            return False
        dest_file = sandbox_dir / local_name
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger.debug("  Dev: copying %s -> %s", source_file, dest_file)
        shutil.copy2(source_file, dest_file)
        return True

    def _copy_file_to_sandbox(
        self,
        source_file: Path,
        sandbox_file: Path,
        *,
        source_label: str | None = None,
    ) -> None:
        sandbox_file.parent.mkdir(parents=True, exist_ok=True)
        if source_label:
            self.logger.debug(
                "  Dev: copying %s %s -> %s",
                source_label,
                source_file,
                sandbox_file,
            )
        else:
            self.logger.debug("  Dev: copying %s -> %s", source_file, sandbox_file)
        shutil.copy2(source_file, sandbox_file)

    def _resolve_installed_ytjobs_file(self, local_name: str) -> Path | None:
        return dev_resolve_ytjobs_source(local_name)

    def _try_copy_regular_file(
        self,
        *,
        local_name: str,
        sandbox_dir: Path,
    ) -> bool:
        source_file = self._pipeline_dir_or_raise() / local_name
        sandbox_file = sandbox_dir / local_name
        if source_file.exists():
            self._copy_file_to_sandbox(source_file, sandbox_file)
            return True
        ytjobs_file = self._resolve_installed_ytjobs_file(local_name)
        if ytjobs_file is None:
            return False
        self._copy_file_to_sandbox(
            ytjobs_file,
            sandbox_file,
            source_label="ytjobs",
        )
        return True

    def _upload_files(self, files: list[tuple[str, str]], sandbox_dir: Path) -> None:
        """Upload files to sandbox directory."""
        self._pipeline_dir_or_raise()

        # Try to get local checkpoint path from stage config for checkpoint files
        local_checkpoint_path = self._get_local_checkpoint_path()
        if local_checkpoint_path:
            self.logger.debug(
                "  Dev: local_checkpoint_path available: %s",
                local_checkpoint_path,
            )

        for yt_path, local_name in files:
            dev_try_upload_one_dependency(
                yt_path=yt_path,
                local_name=local_name,
                sandbox_dir=sandbox_dir,
                local_checkpoint_path=local_checkpoint_path,
                try_checkpoint=self._try_copy_checkpoint_file,
                try_tarball=self._try_copy_tarball_from_build,
                try_regular=self._try_copy_regular_file,
                logger=self.logger,
            )

    def _prepare_map_sandbox(
        self,
        input_table: str,
        output_table: str,
    ) -> tuple[Path, Path, Path]:
        """Prepare sandbox directory and input/output file paths."""
        self._pipeline_dir_or_raise()

        input_path = self._table_local_path(input_table)

        if not input_path.exists():
            msg = (
                f"Dev: input table file not found: {input_path}. "
                "Create it (e.g. run a previous stage or add .jsonl manually)."
            )
            raise FileNotFoundError(msg)

        self._dev_dir().mkdir(parents=True, exist_ok=True)

        # Create sandbox directory
        sandbox_dir = (
            self._dev_dir()
            / f"sandbox_{self._table_basename(input_table)}->{self._table_basename(output_table)}"
        )
        sandbox_dir.mkdir(parents=True, exist_ok=True)

        # Setup input/output files in sandbox
        sandbox_input = sandbox_dir / "input.jsonl"
        sandbox_output = sandbox_dir / "output.jsonl"
        shutil.copy2(input_path, sandbox_input)

        self.logger.info("  Dev: sandbox=%s", sandbox_dir)
        self.logger.info("  Dev: stdin=%s, stdout=%s", sandbox_input, sandbox_output)

        return sandbox_dir, sandbox_input, sandbox_output

    def _setup_map_environment(self, env: dict[str, str]) -> dict[str, str]:
        """Build the environment dict for a dev-mode map run."""
        env_merged = self._build_env(env)

        # Try to setup checkpoint config from stage config
        # This attempts to find stage config by looking for stages directory
        # If found, sets JOB_CONFIG_PATH and CHECKPOINT_FILE env vars
        self._setup_checkpoint_config(env_merged)

        return env_merged

    def _find_checkpoint_in_config(
        self, stage_config: DictConfig | ListConfig
    ) -> str | None:
        """Find checkpoint local_checkpoint_path in stage config.

        Searches through all operations in client.operations dynamically,
        then falls back to client.local_checkpoint_path (legacy).

        Args:
            stage_config: OmegaConf DictConfig for the stage

        Returns:
            Local checkpoint path string if found, None otherwise

        """
        return dev_find_checkpoint_in_config(stage_config)

    def _get_local_checkpoint_path(self) -> str | None:
        """Get local checkpoint path from stage config if available."""
        self._pipeline_dir_or_raise()

        # Try to find stage config by scanning stages directory
        stages_dir = self._pipeline_dir_or_raise() / "stages"
        if not stages_dir.exists():
            return None
        return dev_scan_stages_dir_for_checkpoint(
            stages_dir,
            self.logger,
            dev_find_checkpoint_in_config,
        )

    def _merge_checkpoint_env_from_stage(
        self,
        stage_cfg: object,
        env_merged: dict[str, str],
    ) -> None:
        if not isinstance(stage_cfg, DictConfig):
            return
        local_checkpoint = self._find_checkpoint_in_config(stage_cfg)
        model_name = OmegaConf.select(stage_cfg, "job.model_name")
        if not local_checkpoint:
            return
        checkpoint_path = Path(local_checkpoint).resolve()
        if not checkpoint_path.exists():
            self.logger.warning(
                "  Dev: local_checkpoint_path not found: %s",
                checkpoint_path,
            )
            return
        checkpoint_filename = (
            str(model_name) if model_name not in (None, "") else checkpoint_path.name
        )
        env_merged["CHECKPOINT_FILE"] = checkpoint_filename
        self.logger.info(
            "  Dev: checkpoint file set to: %s (from %s)",
            checkpoint_filename,
            checkpoint_path,
        )

    def _setup_checkpoint_config(self, env_merged: dict[str, str]) -> None:
        """Merge checkpoint-related variables from stage config when available."""
        self._pipeline_dir_or_raise()

        # Try to find stage config by scanning stages directory
        # This is a best-effort approach since we no longer have mapper_script path
        stages_dir = self._pipeline_dir_or_raise() / "stages"
        if not stages_dir.exists():
            return
        dev_apply_first_stage_checkpoint_fallback(
            stages_dir,
            env_merged,
            self.logger,
            self._merge_checkpoint_env_from_stage,
        )
