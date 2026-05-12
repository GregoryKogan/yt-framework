"""Local filesystem stand-in for Cypress tables and subprocess-backed jobs."""

import importlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

from omegaconf import DictConfig, ListConfig, OmegaConf
from yt.wrapper import Operation

import yt_framework
import ytjobs
from yt_framework.operations.job_command import is_typed_job, resolve_aliased_job
from yt_framework.yt.client_base import BaseYTClient, OperationResources
from yt_framework.yt.dev_simulator import (
    DuckDBSimulator,
    extract_output_table,
    extract_table_references,
)
from yt_framework.yt.max_row_weight import ensure_max_row_weight_pragma
from yt_framework.yt.operation_secure_env import pop_secure_env_client_kwargs
from yt_framework.yt.yql_builder import (
    build_distinct_query,
    build_filter_query,
    build_group_by_query,
    build_join_query,
    build_limit_query,
    build_select_query,
    build_sort_query,
    build_union_query,
)

if TYPE_CHECKING:
    from yt.wrapper.schema import TableSchema

_DEV_BUILD_SPLIT_PARTS = 2


def _raise_value_error(message: str) -> None:
    raise ValueError(message)


class _DevOperation:
    """Fake operation for dev run_map; implements wait, get_state, get_error."""

    def __init__(self, returncode: int, stderr_message: str = "") -> None:
        self._returncode = returncode
        self._stderr = stderr_message
        self.id = f"dev-operation-{id(self)}"  # Fake operation ID for dev mode

    def wait(self) -> None:
        pass

    def get_state(self) -> str:
        return "completed" if self._returncode == 0 else "failed"

    def get_error(self) -> str | None:
        if self._returncode == 0:
            return None
        return self._stderr or f"Mapper exited with code {self._returncode}"


class YTDevClient(BaseYTClient):
    """Development YT client implementation.

    Uses local file system for all operations, simulating YT behavior.
    Tables are stored as .jsonl files in .dev/ directory.
    """

    def __init__(
        self,
        logger: logging.Logger,
        pipeline_dir: Path | None = None,
    ) -> None:
        """Initialize development YT client.

        Args:
            logger: Logger instance
            pipeline_dir: Pipeline directory (required for dev mode)

        """
        if pipeline_dir is not None:
            resolved_pipeline_dir = Path(pipeline_dir).resolve()
        else:
            pd = os.environ.get("YT_PIPELINE_DIR")
            if pd:
                resolved_pipeline_dir = Path(pd).resolve()
            else:
                resolved_pipeline_dir = Path.cwd()
                logger.warning(
                    "mode=dev but pipeline_dir not set and YT_PIPELINE_DIR not set; using cwd as pipeline_dir",
                )

        super().__init__(logger, pipeline_dir=resolved_pipeline_dir)

    def _pipeline_dir_or_raise(self) -> Path:
        pd = self.pipeline_dir
        if pd is None:
            msg = "pipeline_dir is required in dev mode"
            raise RuntimeError(msg)
        return pd

    def _dev_dir(self) -> Path:
        """Return .dev directory under pipeline_dir. Caller should mkdir when writing."""
        return self._pipeline_dir_or_raise() / ".dev"

    def _table_basename(self, yt_path: str) -> str:
        """Last path component of a YT table path, e.g. //home/.../name -> name."""
        return yt_path.rstrip("/").split("/")[-1]

    def _table_local_path(self, yt_path: str) -> Path:
        """Local .jsonl path for a YT table in dev: {pipeline_dir}/.dev/{basename}.jsonl."""
        return self._dev_dir() / f"{self._table_basename(yt_path)}.jsonl"

    def create_path(
        self,
        path: str,
        node_type: Literal[
            "table",
            "file",
            "map_node",
            "list_node",
            "document",
        ] = "map_node",
    ) -> None:
        """Create a path in YT (no-op in dev mode).

        Args:
            path: YT path to create (not used in dev mode).
            node_type: Type of node to create (not used in dev mode).

        Returns:
            None

        """

    def exists(self, path: str) -> bool:
        """Check if a path exists in YT.

        In dev mode, always returns True (assumes files exist locally).

        Args:
            path: YT path to check.

        Returns:
            bool: Always True in dev mode.

        """
        return True

    def write_table(
        self,
        table_path: str,
        rows: list[dict[str, Any]],
        *,
        append: bool = False,
        replication_factor: int = 1,
    ) -> None:
        r"""Write rows to a YT table (saves as local .jsonl file).

        In dev mode, tables are stored as JSONL files in the .dev directory.
        Each row is written as a JSON object on a single line.

        Args:
            table_path: YT table path (e.g., "//tmp/my_table").
            rows: List of dictionaries representing table rows.
            append: If True, append to existing file; otherwise overwrite.
            replication_factor: Not used in dev mode (kept for API compatibility).

        Returns:
            None

        Example:
            >>> client.write_table("//tmp/data", [{"id": 1, "name": "Alice"}])
            >>> # Creates .dev/data.jsonl with: {"id":1,"name":"Alice"}\\n

        """
        mode_str = "append" if append else "overwrite"
        self.logger.info("Writing %s rows → %s (%s)", len(rows), table_path, mode_str)
        p = self._table_local_path(table_path)
        self._dev_dir().mkdir(parents=True, exist_ok=True)
        with p.open("a" if append else "w") as f:
            f.writelines(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)

    def read_table(self, table_path: str) -> list[dict[str, Any]]:
        """Read rows from a YT table (reads from local .jsonl file).

        Args:
            table_path: YT table path (e.g., "//tmp/my_table").

        Returns:
            List[Dict[str, Any]]: List of dictionaries representing table rows.
                                  Returns empty list if file doesn't exist.

        """
        self.logger.info("Reading table: %s", table_path)

        p = self._table_local_path(table_path)
        if not p.exists():
            self.logger.warning("Table file not found: %s, returning empty list", p)
            return []
        results = []
        with p.open() as f:
            for raw_line in f:
                line = raw_line.strip()
                if line:
                    results.append(json.loads(line))
        self.logger.info("✓ Read %s rows", len(results))
        return results

    def row_count(self, table_path: str) -> int:
        """Get number of rows in a YT table (counts lines in local .jsonl file).

        Args:
            table_path: YT table path (e.g., "//tmp/my_table").

        Returns:
            int: Number of non-empty lines in the JSONL file. Returns 0 if file doesn't exist.

        """
        p = self._table_local_path(table_path)
        if not p.exists():
            return 0
        with p.open() as f:
            n = sum(1 for line in f if line.strip())
        self.logger.debug("Row count: %s", n)
        return n

    def _get_table_columns(self, table_path: str) -> list[str]:
        """Get column names from a table by reading one row.

        In dev mode, tables are stored as JSONL files, so binary columns are less likely.
        This implementation matches the production client structure for consistency.

        Args:
            table_path: Path to YT table

        Returns:
            List of column names (filtered to exclude internal YQL columns)

        Raises:
            ValueError: If table is empty or cannot be read

        """
        try:
            rows = self.read_table(table_path)
            if not rows:
                _raise_value_error(
                    f"Table {table_path} is empty, cannot determine columns",
                )
            # Get column names from first row
            columns = list(rows[0].keys())
            # Filter out internal YQL columns like _other, _yql_column_*
            columns = [col for col in columns if not col.startswith("_")]
            if not columns:
                # If all columns were filtered out, use all keys (fallback)
                columns = list(rows[0].keys())
        except Exception:
            self.logger.exception("Failed to get table columns")
            raise
        else:
            return columns

    def run_yql(
        self,
        query: str,
        pool: str = "default",
        max_row_weight: str | None = None,
    ) -> None:
        """Execute a YQL query locally using DuckDB simulation.

        Args:
            query: YQL query string to execute
            pool: YT pool name (default: 'default')
            max_row_weight: Optional max row weight override

        """
        self.logger.info("Executing YQL query (dev mode - DuckDB simulation)")
        self.logger.debug("Pool: %s", pool)
        query_with_max_row_weight = ensure_max_row_weight_pragma(
            query=query,
            max_row_weight=max_row_weight,
        )
        self.logger.debug("Query:\n%s", query_with_max_row_weight)

        # Create DuckDB simulator
        simulator = DuckDBSimulator(dev_dir=self._dev_dir(), logger=self.logger)

        try:
            # Extract table references
            input_tables = extract_table_references(query_with_max_row_weight)
            output_table = extract_output_table(query_with_max_row_weight)

            self.logger.debug("Input tables: %s", input_tables)
            self.logger.debug("Output table: %s", output_table)

            # Load input tables
            for table_path in input_tables:
                local_path = self._table_local_path(table_path)
                if local_path.exists():
                    simulator.load_table(table_path, local_path)
                else:
                    self.logger.warning("Input table not found: %s", local_path)

            # Execute query
            results, _ = simulator.execute_yql(query_with_max_row_weight)

            # Save results if output table specified
            if output_table and results:
                self.write_table(output_table, results, append=False)
                self.logger.info("Wrote %s rows to %s", len(results), output_table)

            self.logger.info("✓ YQL query executed successfully")

        except Exception:
            self.logger.exception("Failed to execute YQL query in dev mode")
            raise
        finally:
            simulator.close()

    # Convenience methods for common YQL operations

    def join_tables(
        self,
        left_table: str,
        right_table: str,
        output_table: str,
        on: str | list[str] | dict[str, str],
        how: Literal["inner", "left", "right", "full"] = "left",
        select_columns: list[str] | None = None,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Join two tables using YQL (executed locally with DuckDB in dev mode)."""
        query = build_join_query(
            left_table=left_table,
            right_table=right_table,
            output_table=output_table,
            on=on,
            how=how,
            select_columns=select_columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def filter_table(
        self,
        input_table: str,
        output_table: str,
        condition: str,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Filter table rows using WHERE condition (executed locally with DuckDB in dev mode)."""
        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_filter_query(
            input_table=input_table,
            output_table=output_table,
            condition=condition,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def select_columns(
        self,
        input_table: str,
        output_table: str,
        columns: list[str],
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Select specific columns from a table (executed locally with DuckDB in dev mode)."""
        query = build_select_query(
            input_table=input_table,
            output_table=output_table,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def group_by_aggregate(
        self,
        input_table: str,
        output_table: str,
        group_by: str | list[str],
        aggregations: dict[str, str | tuple[str, str]],
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Group by columns and compute aggregations (executed locally with DuckDB in dev mode)."""
        query = build_group_by_query(
            input_table=input_table,
            output_table=output_table,
            group_by=group_by,
            aggregations=aggregations,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def union_tables(
        self,
        tables: list[str],
        output_table: str,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Union multiple tables (executed locally with DuckDB in dev mode)."""
        # Get columns from first table to avoid _other column issues
        # All tables in union should have the same columns
        columns = self._get_table_columns(tables[0])

        query = build_union_query(
            tables=tables,
            output_table=output_table,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def distinct(
        self,
        input_table: str,
        output_table: str,
        columns: list[str] | None = None,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Get distinct rows from a table (executed locally with DuckDB in dev mode)."""
        query = build_distinct_query(
            input_table=input_table,
            output_table=output_table,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def sort_table(
        self,
        input_table: str,
        output_table: str,
        order_by: str | list[str],
        *,
        ascending: bool = True,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Sort table by columns (executed locally with DuckDB in dev mode)."""
        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_sort_query(
            input_table=input_table,
            output_table=output_table,
            order_by=order_by,
            columns=columns,
            ascending=ascending,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

    def limit_table(
        self,
        input_table: str,
        output_table: str,
        limit: int,
        *,
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Limit number of rows from a table (executed locally with DuckDB in dev mode)."""
        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_limit_query(
            input_table=input_table,
            output_table=output_table,
            limit=limit,
            columns=columns,
            max_row_weight=max_row_weight,
        )

        if dry_run:
            return query

        self.run_yql(query, max_row_weight=max_row_weight)
        return None

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
        output_schema: Optional["TableSchema"] = None,
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
        with (
            sandbox_input.open() as fin,
            sandbox_output.open("w") as fout,
            logs_path.open("w") as ferr,
        ):
            proc = subprocess.run(  # noqa: S603
                ["/bin/bash", "-c", mapper_job],
                stdin=fin,
                stdout=fout,
                stderr=ferr,
                env=env_merged,
                cwd=str(sandbox_dir),
                check=False,
                shell=False,
            )

        # Copy output back
        output_path = self._table_local_path(output_table)
        if proc.returncode == 0 and sandbox_output.exists():
            if append and output_path.exists():
                with output_path.open("ab") as out, sandbox_output.open("rb") as sand:
                    out.write(sand.read())
            else:
                shutil.copy2(sandbox_output, output_path)

        err_hint = f"Stderr written to {logs_path}" if proc.returncode != 0 else ""
        return cast("Operation", _DevOperation(proc.returncode, err_hint))

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
        # YT path format: //tmp/.../build/stages/.../vanilla.py
        # Local path format: stages/.../vanilla.py (relative to sandbox)
        local_command = vanilla_job
        if "/build/" in vanilla_job:
            # Extract the path after /build/ and use it as local path
            # Split command into parts and find the /build/ part
            # Command format: "python3 //tmp/examples/05_vanilla_operation/build/stages/run_vanilla/src/vanilla.py"
            # We want to extract: "stages/run_vanilla/src/vanilla.py"
            parts = vanilla_job.split("/build/", 1)
            if len(parts) == _DEV_BUILD_SPLIT_PARTS:
                # parts[1] contains "stages/run_vanilla/src/vanilla.py" (may have leading/trailing spaces)
                local_path = parts[1].strip()
                # Replace the entire YT path with the local path
                # Match pattern: //anything/build/local_path
                yt_path_pattern = r"//[^/\s]+(?:/[^/\s]+)*/build/" + re.escape(
                    local_path.split()[0],
                )
                local_command = re.sub(
                    yt_path_pattern,
                    local_path.split()[0],
                    vanilla_job,
                )
                if local_command != vanilla_job:
                    self.logger.debug(
                        "  Dev: converted command: %s -> %s",
                        vanilla_job,
                        local_command,
                    )
                else:
                    # Fallback: simple string replacement
                    yt_full_path = "/build/".join(parts)
                    if yt_full_path in vanilla_job:
                        local_command = vanilla_job.replace(
                            yt_full_path,
                            local_path.split()[0],
                        )
                        self.logger.debug(
                            "  Dev: converted command (fallback): %s -> %s",
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
        with logs_path.open("w") as ferr:
            proc = subprocess.run(  # noqa: S603
                ["/bin/bash", "-c", local_command],
                stderr=ferr,
                env=env_merged,
                cwd=str(sandbox_dir),
                check=False,
                shell=False,
            )

        err_hint = f"Output written to {logs_path}" if proc.returncode != 0 else ""
        return cast("Operation", _DevOperation(proc.returncode, err_hint))

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
        output_schema: Optional["TableSchema"] = None,
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
        return cast("Operation", _DevOperation(0))

    def run_reduce(
        self,
        reducer: object,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: Optional["TableSchema"] = None,
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
        return cast("Operation", _DevOperation(0))

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
        # Set PYTHONPATH to include pipeline dir
        env_merged = {**os.environ, **(env or {})}
        pp_parts = [str(self._pipeline_dir_or_raise())]

        # Add yt_framework to PYTHONPATH
        yt_framework_dir = Path(yt_framework.__file__).parent
        if yt_framework_dir.parent not in [Path(p) for p in pp_parts]:
            pp_parts.append(str(yt_framework_dir.parent))

        # Add ytjobs to PYTHONPATH
        ytjobs_dir = Path(ytjobs.__file__).parent
        if ytjobs_dir.parent not in [Path(p) for p in pp_parts]:
            pp_parts.append(str(ytjobs_dir.parent))

        if env_merged.get("PYTHONPATH"):
            pp_parts.append(env_merged["PYTHONPATH"])
        env_merged["PYTHONPATH"] = os.pathsep.join(pp_parts)

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
        if not local_name.startswith("ytjobs/"):
            return None
        yj_mod = sys.modules.get("ytjobs")
        if yj_mod is None:
            try:
                yj_mod = importlib.import_module("ytjobs")
            except ImportError:
                return None
        yj_file = getattr(yj_mod, "__file__", None)
        if not yj_file:
            return None
        ytjobs_dir = Path(yj_file).parent
        ytjobs_rel_path = local_name.replace("ytjobs/", "")
        source_file = ytjobs_dir / ytjobs_rel_path
        if source_file.exists():
            return source_file
        return None

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
            copied = self._try_copy_checkpoint_file(
                yt_path=yt_path,
                local_name=local_name,
                sandbox_dir=sandbox_dir,
                local_checkpoint_path=local_checkpoint_path,
            )
            if not copied:
                copied = self._try_copy_tarball_from_build(
                    yt_path=yt_path,
                    local_name=local_name,
                    sandbox_dir=sandbox_dir,
                )
            if not copied:
                copied = self._try_copy_regular_file(
                    local_name=local_name,
                    sandbox_dir=sandbox_dir,
                )
            if not copied:
                self.logger.debug(
                    "  Dev: skipping file %s -> %s (not found locally)",
                    yt_path,
                    local_name,
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
        if not isinstance(stage_config, DictConfig):
            return None
        # First, try legacy path
        local_checkpoint = OmegaConf.select(
            stage_config,
            "client.local_checkpoint_path",
        )
        if local_checkpoint:
            return str(local_checkpoint)

        # Then, iterate over all operations dynamically
        operations = OmegaConf.select(stage_config, "client.operations")
        if operations and isinstance(operations, (DictConfig, dict)):
            for op_name in operations:
                checkpoint_path = (
                    f"client.operations.{op_name}.checkpoint.local_checkpoint_path"
                )
                local_checkpoint = OmegaConf.select(stage_config, checkpoint_path)
                if local_checkpoint:
                    return str(local_checkpoint)

        return None

    def _get_local_checkpoint_path(self) -> str | None:
        """Get local checkpoint path from stage config if available."""
        self._pipeline_dir_or_raise()

        # Try to find stage config by scanning stages directory
        stages_dir = self._pipeline_dir_or_raise() / "stages"
        if not stages_dir.exists():
            return None

        try:
            # Try to find a stage config with checkpoint configuration
            # Check all stage configs, not just the first one
            for stage_dir in stages_dir.iterdir():
                if stage_dir.is_dir():
                    stage_config_path = stage_dir / "config.yaml"
                    if stage_config_path.exists():
                        try:
                            stage_cfg = OmegaConf.load(stage_config_path)
                            if not isinstance(stage_cfg, DictConfig):
                                continue
                            local_checkpoint = self._find_checkpoint_in_config(
                                stage_cfg,
                            )
                            if local_checkpoint:
                                checkpoint_path = Path(local_checkpoint).resolve()
                                if checkpoint_path.exists():
                                    self.logger.debug(
                                        "  Dev: found local_checkpoint_path: %s",
                                        checkpoint_path,
                                    )
                                    return str(checkpoint_path)
                        except Exception as e:  # noqa: BLE001
                            # Continue to next stage config
                            self.logger.debug(
                                "  Dev: error reading %s: %s",
                                stage_config_path,
                                e,
                            )
                            continue
        except Exception as e:  # noqa: BLE001
            self.logger.debug("  Dev: error scanning stages directory: %s", e)

        return None

    def _setup_checkpoint_config(self, env_merged: dict[str, str]) -> None:
        """Merge checkpoint-related variables from stage config when available."""
        self._pipeline_dir_or_raise()

        # Try to find stage config by scanning stages directory
        # This is a best-effort approach since we no longer have mapper_script path
        stages_dir = self._pipeline_dir_or_raise() / "stages"
        if not stages_dir.exists():
            return

        # Look for any stage config that might be relevant
        # In practice, the calling code should set JOB_CONFIG_PATH if needed
        # This is a fallback for backward compatibility
        try:
            # Try to find a stage config (use first one found as fallback)
            for stage_dir in stages_dir.iterdir():
                if stage_dir.is_dir():
                    stage_config_path = stage_dir / "config.yaml"
                    if stage_config_path.exists():
                        env_merged["JOB_CONFIG_PATH"] = str(stage_config_path)

                        try:
                            stage_cfg = OmegaConf.load(stage_config_path)
                            if not isinstance(stage_cfg, DictConfig):
                                continue
                            local_checkpoint = self._find_checkpoint_in_config(
                                stage_cfg,
                            )
                            # Get model_name from job.model_name (used as checkpoint filename)
                            model_name = OmegaConf.select(
                                stage_cfg,
                                "job.model_name",
                            )

                            if local_checkpoint:
                                checkpoint_path = Path(local_checkpoint).resolve()
                                if checkpoint_path.exists():
                                    # Set CHECKPOINT_FILE to the filename (not full path) since the file
                                    # will be copied to the sandbox directory and processor expects it there
                                    checkpoint_filename = (
                                        str(model_name)
                                        if model_name not in (None, "")
                                        else checkpoint_path.name
                                    )
                                    env_merged["CHECKPOINT_FILE"] = checkpoint_filename
                                    self.logger.info(
                                        "  Dev: checkpoint file set to: %s (from %s)",
                                        checkpoint_filename,
                                        checkpoint_path,
                                    )
                                else:
                                    self.logger.warning(
                                        "  Dev: local_checkpoint_path not found: %s",
                                        checkpoint_path,
                                    )
                        except Exception as e:  # noqa: BLE001
                            self.logger.warning(
                                "  Dev: failed to load checkpoint config: %s",
                                e,
                            )

                        # Only use first found config as fallback
                        break
        except Exception as e:  # noqa: BLE001
            self.logger.debug("  Dev: could not setup checkpoint config: %s", e)
