"""
Development YT Client
=====================

Development implementation of YT client using local file system.
"""

import json
import os
import logging
import subprocess
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple, Literal

from yt.wrapper import Operation  # pyright: ignore[reportMissingImports]
from typing import TYPE_CHECKING

from yt_framework.yt.client_base import BaseYTClient, OperationResources

if TYPE_CHECKING:
    from yt.wrapper.schema import TableSchema  # pyright: ignore[reportMissingImports]


class _DevOperation:
    """Fake operation for dev run_map; implements wait, get_state, get_error."""

    def __init__(self, returncode: int, stderr_message: str = ""):
        self._returncode = returncode
        self._stderr = stderr_message
        self.id = f"dev-operation-{id(self)}"  # Fake operation ID for dev mode

    def wait(self) -> None:
        pass

    def get_state(self) -> str:
        return "completed" if self._returncode == 0 else "failed"

    def get_error(self) -> Optional[str]:
        if self._returncode == 0:
            return None
        return self._stderr or f"Mapper exited with code {self._returncode}"


class YTDevClient(BaseYTClient):
    """
    Development YT client implementation.

    Uses local file system for all operations, simulating YT behavior.
    Tables are stored as .jsonl files in .dev/ directory.
    """

    def __init__(
        self,
        logger: logging.Logger,
        pipeline_dir: Optional[Path] = None,
    ) -> None:
        """
        Initialize development YT client.

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
                    "mode=dev but pipeline_dir not set and YT_PIPELINE_DIR not set; using cwd as pipeline_dir"
                )

        super().__init__(logger, pipeline_dir=resolved_pipeline_dir)

    def _dev_dir(self) -> Path:
        """Return .dev directory under pipeline_dir. Caller should mkdir when writing."""
        assert self.pipeline_dir is not None, "pipeline_dir is required in dev mode"
        return self.pipeline_dir / ".dev"

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
            "table", "file", "map_node", "list_node", "document"
        ] = "map_node",
    ) -> None:
        """Create a path in YT (no-op in dev mode).
        
        Args:
            path: YT path to create (not used in dev mode).
            node_type: Type of node to create (not used in dev mode).
            
        Returns:
            None
        """
        pass

    def exists(self, path: str) -> bool:
        """
        Check if a path exists in YT.

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
        rows: List[Dict[str, Any]],
        append: bool = False,
        replication_factor: int = 1,
    ) -> None:
        """Write rows to a YT table (saves as local .jsonl file).
        
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
        self.logger.info(f"Writing {len(rows)} rows → {table_path} ({mode_str})")

        p = self._table_local_path(table_path)
        self._dev_dir().mkdir(parents=True, exist_ok=True)
        with open(p, "a" if append else "w") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def read_table(self, table_path: str) -> List[Dict[str, Any]]:
        """Read rows from a YT table (reads from local .jsonl file).
        
        Args:
            table_path: YT table path (e.g., "//tmp/my_table").
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries representing table rows.
                                  Returns empty list if file doesn't exist.
        """
        self.logger.info(f"Reading table: {table_path}")

        p = self._table_local_path(table_path)
        if not p.exists():
            self.logger.warning(f"Table file not found: {p}, returning empty list")
            return []
        results = []
        with open(p, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    results.append(json.loads(line))
        self.logger.info(f"✓ Read {len(results)} rows")
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
        with open(p) as f:
            n = sum(1 for line in f if line.strip())
        self.logger.debug(f"Row count: {n}")
        return n

    def _get_table_columns(self, table_path: str) -> List[str]:
        """
        Get column names from a table by reading one row.

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
                raise ValueError(
                    f"Table {table_path} is empty, cannot determine columns"
                )
            # Get column names from first row
            columns = list(rows[0].keys())
            # Filter out internal YQL columns like _other, _yql_column_*
            columns = [col for col in columns if not col.startswith("_")]
            if not columns:
                # If all columns were filtered out, use all keys (fallback)
                columns = list(rows[0].keys())
            return columns
        except Exception as e:
            self.logger.error(f"Failed to get table columns: {e}")
            raise

    def run_yql(
        self,
        query: str,
        pool: str = "default",
    ) -> None:
        """
        Execute a YQL query locally using DuckDB simulation.

        Args:
            query: YQL query string to execute
            pool: YT pool name (default: 'default')
        """
        self.logger.info("Executing YQL query (dev mode - DuckDB simulation)")
        self.logger.debug(f"Pool: {pool}")
        self.logger.debug(f"Query:\n{query}")

        from yt_framework.yt.dev_simulator import (
            DuckDBSimulator,
            extract_table_references,
            extract_output_table,
        )

        # Create DuckDB simulator
        simulator = DuckDBSimulator(dev_dir=self._dev_dir(), logger=self.logger)

        try:
            # Extract table references
            input_tables = extract_table_references(query)
            output_table = extract_output_table(query)

            self.logger.debug(f"Input tables: {input_tables}")
            self.logger.debug(f"Output table: {output_table}")

            # Load input tables
            for table_path in input_tables:
                local_path = self._table_local_path(table_path)
                if local_path.exists():
                    simulator.load_table(table_path, local_path)
                else:
                    self.logger.warning(f"Input table not found: {local_path}")

            # Execute query
            results, _ = simulator.execute_yql(query)

            # Save results if output table specified
            if output_table and results is not None:
                self.write_table(output_table, results, append=False)
                self.logger.info(f"Wrote {len(results)} rows to {output_table}")

            self.logger.info("✓ YQL query executed successfully")

        except Exception as e:
            self.logger.error(f"Failed to execute YQL query in dev mode: {e}")
            raise
        finally:
            simulator.close()

    # Convenience methods for common YQL operations

    def join_tables(
        self,
        left_table: str,
        right_table: str,
        output_table: str,
        on: Union[str, List[str], Dict[str, str]],
        how: Literal["inner", "left", "right", "full"] = "left",
        select_columns: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> Optional[str]:
        """Join two tables using YQL (executed locally with DuckDB in dev mode)."""
        from yt_framework.yt.yql_builder import build_join_query

        query = build_join_query(
            left_table=left_table,
            right_table=right_table,
            output_table=output_table,
            on=on,
            how=how,
            select_columns=select_columns,
        )

        if dry_run:
            return query

        self.run_yql(query)
        return None

    def filter_table(
        self,
        input_table: str,
        output_table: str,
        condition: str,
        dry_run: bool = False,
    ) -> Optional[str]:
        """Filter table rows using WHERE condition (executed locally with DuckDB in dev mode)."""
        from yt_framework.yt.yql_builder import build_filter_query

        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_filter_query(
            input_table=input_table,
            output_table=output_table,
            condition=condition,
            columns=columns,
        )

        if dry_run:
            return query

        self.run_yql(query)
        return None

    def select_columns(
        self,
        input_table: str,
        output_table: str,
        columns: List[str],
        dry_run: bool = False,
    ) -> Optional[str]:
        """Select specific columns from a table (executed locally with DuckDB in dev mode)."""
        from yt_framework.yt.yql_builder import build_select_query

        query = build_select_query(
            input_table=input_table,
            output_table=output_table,
            columns=columns,
        )

        if dry_run:
            return query

        self.run_yql(query)
        return None

    def group_by_aggregate(
        self,
        input_table: str,
        output_table: str,
        group_by: Union[str, List[str]],
        aggregations: Dict[str, Union[str, Tuple[str, str]]],
        dry_run: bool = False,
    ) -> Optional[str]:
        """Group by columns and compute aggregations (executed locally with DuckDB in dev mode)."""
        from yt_framework.yt.yql_builder import build_group_by_query

        query = build_group_by_query(
            input_table=input_table,
            output_table=output_table,
            group_by=group_by,
            aggregations=aggregations,
        )

        if dry_run:
            return query

        self.run_yql(query)
        return None

    def union_tables(
        self,
        tables: List[str],
        output_table: str,
        dry_run: bool = False,
    ) -> Optional[str]:
        """Union multiple tables (executed locally with DuckDB in dev mode)."""
        from yt_framework.yt.yql_builder import build_union_query

        # Get columns from first table to avoid _other column issues
        # All tables in union should have the same columns
        columns = self._get_table_columns(tables[0])

        query = build_union_query(
            tables=tables,
            output_table=output_table,
            columns=columns,
        )

        if dry_run:
            return query

        self.run_yql(query)
        return None

    def distinct(
        self,
        input_table: str,
        output_table: str,
        columns: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> Optional[str]:
        """Get distinct rows from a table (executed locally with DuckDB in dev mode)."""
        from yt_framework.yt.yql_builder import build_distinct_query

        query = build_distinct_query(
            input_table=input_table,
            output_table=output_table,
            columns=columns,
        )

        if dry_run:
            return query

        self.run_yql(query)
        return None

    def sort_table(
        self,
        input_table: str,
        output_table: str,
        order_by: Union[str, List[str]],
        ascending: bool = True,
        dry_run: bool = False,
    ) -> Optional[str]:
        """Sort table by columns (executed locally with DuckDB in dev mode)."""
        from yt_framework.yt.yql_builder import build_sort_query

        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_sort_query(
            input_table=input_table,
            output_table=output_table,
            order_by=order_by,
            columns=columns,
            ascending=ascending,
        )

        if dry_run:
            return query

        self.run_yql(query)
        return None

    def limit_table(
        self,
        input_table: str,
        output_table: str,
        limit: int,
        dry_run: bool = False,
    ) -> Optional[str]:
        """Limit number of rows from a table (executed locally with DuckDB in dev mode)."""
        from yt_framework.yt.yql_builder import build_limit_query

        # Get columns from input table to avoid _other column issues
        columns = self._get_table_columns(input_table)

        query = build_limit_query(
            input_table=input_table,
            output_table=output_table,
            limit=limit,
            columns=columns,
        )

        if dry_run:
            return query

        self.run_yql(query)
        return None

    def upload_file(
        self, local_path: Path, yt_path: str, create_parent_dir: bool = False
    ) -> None:
        """
        Upload a file to YT (no-op in dev mode).

        Args:
            local_path: Local file path to upload
            yt_path: YT destination path
            create_parent_dir: If True, create parent directory if it doesn't exist (default: False)
        """
        self.logger.debug(f"Dev: upload_file no-op {local_path.name} → {yt_path}")

    def upload_directory(
        self, local_dir: Path, yt_dir: str, pattern: str = "*"
    ) -> List[str]:
        """Upload a directory to YT (no-op in dev mode).
        
        Args:
            local_dir: Local directory path to upload.
            yt_dir: YT destination directory path.
            pattern: File pattern to match (not used in dev mode).
            
        Returns:
            List[str]: Empty list in dev mode.
        """
        self.logger.debug(f"Dev: upload_directory no-op {local_dir} → {yt_dir}")
        return []

    def run_map(
        self,
        command: str,
        input_table: str,
        output_table: str,
        files: List[Tuple[str, str]],
        resources: OperationResources,
        env: Dict[str, str],
        output_schema: Optional["TableSchema"] = None,
        max_failed_jobs: int = 1,
        docker_auth: Optional[Dict[str, str]] = None,
    ) -> Operation:
        """Run a map operation locally using subprocess.
        
        In dev mode, executes the mapper script locally with input/output tables
        as JSONL files. The command is executed in a temporary sandbox directory
        with all dependencies available.
        
        Args:
            command: Command to execute (typically bash command with script path).
            input_table: Input YT table path (read from local JSONL).
            output_table: Output YT table path (written to local JSONL).
            files: List of (yt_path, local_path) tuples for dependencies.
            resources: Operation resource configuration (not fully used in dev mode).
            env: Environment variables dictionary.
            output_schema: Optional output table schema (not used in dev mode).
            max_failed_jobs: Maximum failed jobs allowed (not used in dev mode).
            docker_auth: Optional Docker authentication (not used in dev mode).
            
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
        assert self.pipeline_dir is not None

        self.logger.info("Submitting map operation")
        self.logger.info(f"  Input: {input_table}")
        self.logger.info(f"  Output: {output_table}")
        self.logger.info(f"  Command: {command}")

        # Prepare sandbox and input/output files
        sandbox_dir, sandbox_input, sandbox_output = self._prepare_map_sandbox(
            input_table, output_table
        )

        # Copy files to sandbox
        self._upload_files(files, sandbox_dir)

        # Setup environment
        env_merged = self._setup_map_environment(env)

        logs_path = self._dev_dir() / f"{self._table_basename(output_table)}.log"
        with (
            open(sandbox_input) as fin,
            open(sandbox_output, "w") as fout,
            open(logs_path, "w") as ferr,
        ):
            proc = subprocess.run(
                ["bash", "-c", command],
                stdin=fin,
                stdout=fout,
                stderr=ferr,
                env=env_merged,
                cwd=str(sandbox_dir),
            )

        # Copy output back
        output_path = self._table_local_path(output_table)
        if proc.returncode == 0 and sandbox_output.exists():
            shutil.copy2(sandbox_output, output_path)

        err_hint = f"Stderr written to {logs_path}" if proc.returncode != 0 else ""
        return _DevOperation(proc.returncode, err_hint)  # type: ignore[return-value]

    def run_vanilla(
        self,
        command: str,
        files: List[Tuple[str, str]],
        env: Dict[str, str],
        task_name: str = "main",
        **kwargs,
    ) -> Operation:
        """Run a vanilla operation locally using subprocess.
        
        In dev mode, executes the vanilla script locally in a temporary sandbox
        directory with all dependencies available. No input/output tables are involved.
        
        Args:
            command: Command to execute (typically bash command with script path).
            files: List of (yt_path, local_path) tuples for dependencies.
            env: Environment variables dictionary.
            task_name: Task name for logging (default: "main").
            **kwargs: Additional arguments (not used in dev mode).
            
        Returns:
            Operation: Mock operation object that simulates YT operation.
        """
        self.logger.info("Submitting vanilla operation")
        self.logger.info(f"  Command: {command}")
        self.logger.info(f"  Task: {task_name}")

        assert self.pipeline_dir is not None
        self._dev_dir().mkdir(parents=True, exist_ok=True)

        sandbox_dir = self._dev_dir() / f"{task_name}_sandbox"
        sandbox_dir.mkdir(parents=True, exist_ok=True)
        self._upload_files(files, sandbox_dir)

        # Copy config.yaml to the correct location in sandbox if it exists
        # config.yaml dependency has local_name="config.yaml" but should be at stages/{task_name}/config.yaml
        stage_config_source = self.pipeline_dir / "stages" / task_name / "config.yaml"
        if stage_config_source.exists():
            stage_config_dest = sandbox_dir / "stages" / task_name / "config.yaml"
            stage_config_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(stage_config_source, stage_config_dest)
            self.logger.debug(f"  Dev: copied config.yaml to {stage_config_dest}")

        # Convert YT paths in command to local sandbox paths
        # YT path format: //tmp/.../build/stages/.../vanilla.py
        # Local path format: stages/.../vanilla.py (relative to sandbox)
        local_command = command
        if "/build/" in command:
            # Extract the path after /build/ and use it as local path
            import re

            # Split command into parts and find the /build/ part
            # Command format: "python3 //tmp/examples/05_vanilla_operation/build/stages/run_vanilla/src/vanilla.py"
            # We want to extract: "stages/run_vanilla/src/vanilla.py"
            parts = command.split("/build/", 1)
            if len(parts) == 2:
                # parts[1] contains "stages/run_vanilla/src/vanilla.py" (may have leading/trailing spaces)
                local_path = parts[1].strip()
                # Replace the entire YT path with the local path
                # Match pattern: //anything/build/local_path
                yt_path_pattern = r"//[^/\s]+(?:/[^/\s]+)*/build/" + re.escape(
                    local_path.split()[0]
                )
                local_command = re.sub(yt_path_pattern, local_path.split()[0], command)
                if local_command != command:
                    self.logger.debug(
                        f"  Dev: converted command: {command} -> {local_command}"
                    )
                else:
                    # Fallback: simple string replacement
                    yt_full_path = "/build/".join(parts)
                    if yt_full_path in command:
                        local_command = command.replace(
                            yt_full_path, local_path.split()[0]
                        )
                        self.logger.debug(
                            f"  Dev: converted command (fallback): {command} -> {local_command}"
                        )

        logs_path = self._dev_dir() / f"{task_name}.log"

        # Set up environment with JOB_CONFIG_PATH pointing to the config file in sandbox
        env_merged = self._build_env(env)
        config_path_in_sandbox = sandbox_dir / "stages" / task_name / "config.yaml"
        if config_path_in_sandbox.exists():
            env_merged["JOB_CONFIG_PATH"] = str(config_path_in_sandbox)
            self.logger.debug(f"  Dev: JOB_CONFIG_PATH={config_path_in_sandbox}")
        else:
            self.logger.warning(
                f"  Dev: config file not found at {config_path_in_sandbox}"
            )

        self.logger.info(f"  Dev: sandbox={sandbox_dir}")
        self.logger.info(f"  Dev: stderr={logs_path}")
        with open(logs_path, "w") as ferr:
            proc = subprocess.run(
                ["bash", "-c", local_command],
                stderr=ferr,
                env=env_merged,
                cwd=str(sandbox_dir),
            )

        err_hint = f"Output written to {logs_path}" if proc.returncode != 0 else ""
        return _DevOperation(proc.returncode, err_hint)  # type: ignore[return-value]

    def _build_env(self, env: Dict[str, str]) -> Dict[str, str]:
        """Build environment variables for subprocess."""

        # Set PYTHONPATH to include pipeline dir
        env_merged = {**os.environ, **(env or {})}
        pp_parts = [str(self.pipeline_dir)]

        # Add yt_framework to PYTHONPATH
        import yt_framework

        yt_framework_dir = Path(yt_framework.__file__).parent
        if yt_framework_dir.parent not in [Path(p) for p in pp_parts]:
            pp_parts.append(str(yt_framework_dir.parent))

        # Add ytjobs to PYTHONPATH
        import ytjobs

        ytjobs_dir = Path(ytjobs.__file__).parent
        if ytjobs_dir.parent not in [Path(p) for p in pp_parts]:
            pp_parts.append(str(ytjobs_dir.parent))

        if env_merged.get("PYTHONPATH"):
            pp_parts.append(env_merged["PYTHONPATH"])
        env_merged["PYTHONPATH"] = os.pathsep.join(pp_parts)

        return env_merged

    def _upload_files(self, files: List[Tuple[str, str]], sandbox_dir: Path) -> None:
        """Upload files to sandbox directory."""
        assert self.pipeline_dir is not None

        # Try to get local checkpoint path from stage config for checkpoint files
        local_checkpoint_path = self._get_local_checkpoint_path()
        if local_checkpoint_path:
            self.logger.debug(
                f"  Dev: local_checkpoint_path available: {local_checkpoint_path}"
            )

        for file_info in files:
            yt_path, local_name = file_info
            copied = False

            # Handle checkpoint files - copy from local_checkpoint_path if available
            # Match if either the yt_path filename or local_name matches the checkpoint filename
            if local_checkpoint_path:
                checkpoint_filename = Path(local_checkpoint_path).name
                yt_filename = Path(yt_path).name
                # Check if this is a checkpoint file (matches by filename)
                if (
                    checkpoint_filename == yt_filename
                    or checkpoint_filename == local_name
                ):
                    checkpoint_path = Path(local_checkpoint_path)
                    if checkpoint_path.exists():
                        # Use the expected local_name in sandbox (from dependency)
                        dest_file = sandbox_dir / local_name
                        dest_file.parent.mkdir(parents=True, exist_ok=True)
                        self.logger.info(
                            f"  Dev: copying checkpoint {checkpoint_path} -> {dest_file}"
                        )
                        shutil.copy2(checkpoint_path, dest_file)
                        copied = True
                    else:
                        self.logger.warning(
                            f"  Dev: checkpoint path does not exist: {checkpoint_path}"
                        )

            # Handle build files (code.tar.gz, etc.)
            if not copied and (".build" in yt_path or yt_path.endswith(".tar.gz")):
                # Try to find the file in .build directory
                local_build = self.pipeline_dir / ".build"
                if local_build.exists():
                    # Extract just the filename from yt_path
                    filename = Path(yt_path).name
                    source_file = local_build / filename
                    if source_file.exists():
                        dest_file = sandbox_dir / local_name
                        dest_file.parent.mkdir(parents=True, exist_ok=True)
                        self.logger.debug(
                            f"  Dev: copying {source_file} -> {dest_file}"
                        )
                        shutil.copy2(source_file, dest_file)
                        copied = True

            # Handle regular stage files and ytjobs files
            # local_name is like "stages/run_vanilla/src/vanilla.py" or "ytjobs/..."
            if not copied:
                # Try to find the file relative to pipeline_dir
                source_file = self.pipeline_dir / local_name
                if source_file.exists():
                    dest_file = sandbox_dir / local_name
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    self.logger.debug(f"  Dev: copying {source_file} -> {dest_file}")
                    shutil.copy2(source_file, dest_file)
                    copied = True
                else:
                    # Also try ytjobs files - they might be in the installed package
                    if local_name.startswith("ytjobs/"):
                        try:
                            import ytjobs

                            ytjobs_dir = Path(ytjobs.__file__).parent
                            ytjobs_rel_path = local_name.replace("ytjobs/", "")
                            source_file = ytjobs_dir / ytjobs_rel_path
                            if source_file.exists():
                                dest_file = sandbox_dir / local_name
                                dest_file.parent.mkdir(parents=True, exist_ok=True)
                                self.logger.debug(
                                    f"  Dev: copying ytjobs {source_file} -> {dest_file}"
                                )
                                shutil.copy2(source_file, dest_file)
                                copied = True
                        except ImportError:
                            pass

            if not copied:
                self.logger.debug(
                    f"  Dev: skipping file {yt_path} -> {local_name} (not found locally)"
                )

    def _prepare_map_sandbox(
        self, input_table: str, output_table: str
    ) -> Tuple[Path, Path, Path]:
        """Prepare sandbox directory and input/output file paths."""
        assert self.pipeline_dir is not None

        input_path = self._table_local_path(input_table)

        if not input_path.exists():
            raise FileNotFoundError(
                f"Dev: input table file not found: {input_path}. "
                "Create it (e.g. run a previous stage or add .jsonl manually)."
            )

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

        self.logger.info(f"  Dev: sandbox={sandbox_dir}")
        self.logger.info(f"  Dev: stdin={sandbox_input}, stdout={sandbox_output}")

        return sandbox_dir, sandbox_input, sandbox_output

    def _setup_map_environment(self, env: Dict[str, str]) -> Dict[str, str]:
        """Setup environment variables for map operation."""
        env_merged = self._build_env(env)

        # Try to setup checkpoint config from stage config
        # This attempts to find stage config by looking for stages directory
        # If found, sets JOB_CONFIG_PATH and CHECKPOINT_FILE env vars
        self._setup_checkpoint_config(env_merged)

        return env_merged

    def _find_checkpoint_in_config(self, stage_config) -> Optional[str]:
        """
        Find checkpoint local_checkpoint_path in stage config.

        Searches through all operations in client.operations dynamically,
        then falls back to client.local_checkpoint_path (legacy).

        Args:
            stage_config: OmegaConf DictConfig for the stage

        Returns:
            Local checkpoint path string if found, None otherwise
        """
        from omegaconf import OmegaConf

        # First, try legacy path
        local_checkpoint = OmegaConf.select(
            stage_config, "client.local_checkpoint_path"
        )
        if local_checkpoint:
            return str(local_checkpoint)

        # Then, iterate over all operations dynamically
        operations = OmegaConf.select(stage_config, "client.operations")
        if operations:
            for op_name in operations.keys():
                checkpoint_path = (
                    f"client.operations.{op_name}.checkpoint.local_checkpoint_path"
                )
                local_checkpoint = OmegaConf.select(stage_config, checkpoint_path)
                if local_checkpoint:
                    return str(local_checkpoint)

        return None

    def _get_local_checkpoint_path(self) -> Optional[str]:
        """Get local checkpoint path from stage config if available."""
        assert self.pipeline_dir is not None

        # Try to find stage config by scanning stages directory
        stages_dir = self.pipeline_dir / "stages"
        if not stages_dir.exists():
            return None

        try:
            from omegaconf import OmegaConf

            # Try to find a stage config with checkpoint configuration
            # Check all stage configs, not just the first one
            for stage_dir in stages_dir.iterdir():
                if stage_dir.is_dir():
                    stage_config_path = stage_dir / "config.yaml"
                    if stage_config_path.exists():
                        try:
                            stage_config = OmegaConf.load(stage_config_path)
                            local_checkpoint = self._find_checkpoint_in_config(
                                stage_config
                            )
                            if local_checkpoint:
                                checkpoint_path = Path(local_checkpoint).resolve()
                                if checkpoint_path.exists():
                                    self.logger.debug(
                                        f"  Dev: found local_checkpoint_path: {checkpoint_path}"
                                    )
                                    return str(checkpoint_path)
                        except Exception as e:
                            # Continue to next stage config
                            self.logger.debug(
                                f"  Dev: error reading {stage_config_path}: {e}"
                            )
                            continue
        except Exception as e:
            self.logger.debug(f"  Dev: error scanning stages directory: {e}")

        return None

    def _setup_checkpoint_config(self, env_merged: Dict[str, str]) -> None:
        """Setup checkpoint config from stage config if available."""
        assert self.pipeline_dir is not None

        # Try to find stage config by scanning stages directory
        # This is a best-effort approach since we no longer have mapper_script path
        stages_dir = self.pipeline_dir / "stages"
        if not stages_dir.exists():
            return

        # Look for any stage config that might be relevant
        # In practice, the calling code should set JOB_CONFIG_PATH if needed
        # This is a fallback for backward compatibility
        try:
            from omegaconf import OmegaConf

            # Try to find a stage config (use first one found as fallback)
            for stage_dir in stages_dir.iterdir():
                if stage_dir.is_dir():
                    stage_config_path = stage_dir / "config.yaml"
                    if stage_config_path.exists():
                        env_merged["JOB_CONFIG_PATH"] = str(stage_config_path)

                        try:
                            stage_config = OmegaConf.load(stage_config_path)
                            # Find checkpoint path dynamically (searches all operations)
                            local_checkpoint = self._find_checkpoint_in_config(
                                stage_config
                            )
                            # Get model_name from job.model_name (used as checkpoint filename)
                            model_name = OmegaConf.select(
                                stage_config, "job.model_name"
                            )

                            if local_checkpoint:
                                checkpoint_path = Path(local_checkpoint).resolve()
                                if checkpoint_path.exists():
                                    # Set CHECKPOINT_FILE to the filename (not full path) since the file
                                    # will be copied to the sandbox directory and processor expects it there
                                    checkpoint_filename = (
                                        model_name or checkpoint_path.name
                                    )
                                    env_merged["CHECKPOINT_FILE"] = checkpoint_filename
                                    self.logger.info(
                                        f"  Dev: checkpoint file set to: {checkpoint_filename} (from {checkpoint_path})"
                                    )
                                else:
                                    self.logger.warning(
                                        f"  Dev: local_checkpoint_path not found: {checkpoint_path}"
                                    )
                        except Exception as e:
                            self.logger.warning(
                                f"  Dev: failed to load checkpoint config: {e}"
                            )

                        # Only use first found config as fallback
                        break
        except Exception as e:
            self.logger.debug(f"  Dev: could not setup checkpoint config: {e}")
