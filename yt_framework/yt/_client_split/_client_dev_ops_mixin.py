"""Upload and map/reduce/vanilla/sort methods for dev YT client."""

# pyright: reportAttributeAccessIssue=false, reportUnusedImport=false

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING, cast

from yt_framework.yt._client_dev_runtime import (
    dev_rewrite_build_path_cmd,
    dev_run_map_subprocess,
    dev_run_vanilla_subprocess,
)
from yt_framework.yt._client_split._client_dev_mr_reduce_sort_mixin import (
    ClientDevMrReduceSortMixin,
)
from yt_framework.yt._client_split.dev_operation import DevOperation
from yt_framework.yt.operation_secure_env import pop_secure_env_client_kwargs

if TYPE_CHECKING:
    from pathlib import Path

    from yt.wrapper import Operation
    from yt.wrapper.schema import TableSchema

    from yt_framework.yt.clients.client_base import OperationResources

_DEV_BUILD_SPLIT_PARTS = 2


class ClientDevOpsMixin(ClientDevMrReduceSortMixin):
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
        local_command = dev_rewrite_build_path_cmd(
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
