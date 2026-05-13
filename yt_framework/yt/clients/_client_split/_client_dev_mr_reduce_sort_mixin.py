"""Dev-mode map-reduce, reduce, and sort stubs."""

# pyright: reportAttributeAccessIssue=false, reportUnusedImport=false, reportPrivateUsage=false

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, cast

from omegaconf import DictConfig, ListConfig, OmegaConf

if TYPE_CHECKING:
    from yt.wrapper import Operation

    from yt_framework.yt.clients.operation_specs import (
        MapReduceSubmitSpec,
        ReduceSubmitSpec,
    )

from yt_framework.job_command import is_typed_job, resolve_aliased_job
from yt_framework.yt.clients._client_split.dev_operation import DevOperation
from yt_framework.yt.support._client_dev_runtime import (
    dev_apply_stage_checkpoint_fallback,
    dev_find_checkpoint_in_config,
    dev_pythonpath_entries,
    dev_resolve_ytjobs_source,
    dev_scan_stages_checkpoint,
    dev_try_upload_one_dependency,
)
from yt_framework.yt.support.operation_secure_env import pop_secure_env_client_kwargs


class ClientDevMrReduceSortMixin:
    """Mixin for dev map-reduce / reduce / sort no-ops."""

    def run_map_reduce_submit(self, spec: MapReduceSubmitSpec) -> Operation:
        """Dev: no-op; copy input table to output table."""
        mapper = spec.mapper
        reducer = spec.reducer
        input_table = spec.input_table
        output_table = spec.output_table
        reduce_by_cols = spec.reduce_by_list()
        files = spec.files_list()
        resources = spec.resources
        env = spec.env_dict()
        sort_by = spec.sort_by_list()
        output_schema = spec.output_schema
        max_failed_jobs = spec.max_failed_jobs
        docker_auth = spec.docker_auth_dict()
        map_job = spec.map_job
        reduce_job = spec.reduce_job
        kwargs = dict(spec.extras_dict())
        self.logger.debug(
            "Dev map-reduce ignoring hints: reduce_by=%s sort_by=%s schema=%s max_failed=%s files=%s docker=%s env_keys=%s pool=%s",
            reduce_by_cols,
            sort_by,
            output_schema is not None,
            max_failed_jobs,
            len(files),
            docker_auth is not None,
            len(env),
            resources.pool,
        )
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

    def run_reduce_submit(self, spec: ReduceSubmitSpec) -> Operation:
        """Dev: no-op; copy input table to output table."""
        reducer = spec.reducer
        input_table = spec.input_table
        output_table = spec.output_table
        reduce_by_cols = spec.reduce_by_list()
        files = spec.files_list()
        resources = spec.resources
        env = spec.env_dict()
        output_schema = spec.output_schema
        max_failed_jobs = spec.max_failed_jobs
        docker_auth = spec.docker_auth_dict()
        job = spec.job
        kwargs = dict(spec.extras_dict())
        self.logger.debug(
            "Dev reduce ignoring hints: reduce_by=%s schema=%s max_failed=%s files=%s docker=%s env_keys=%s pool=%s",
            reduce_by_cols,
            output_schema is not None,
            max_failed_jobs,
            len(files),
            docker_auth is not None,
            len(env),
            resources.pool,
        )
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
        parts = dev_pythonpath_entries(
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
        return dev_scan_stages_checkpoint(
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
        dev_apply_stage_checkpoint_fallback(
            stages_dir,
            env_merged,
            self.logger,
            self._merge_checkpoint_env_from_stage,
        )
