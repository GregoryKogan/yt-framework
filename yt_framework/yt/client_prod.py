"""
Production YT Client
====================

Production implementation of YT client using actual YTsaurus client.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple, Literal

from yt.wrapper import (  # pyright: ignore[reportMissingImports]
    YtClient,
    FilePath,
    TablePath,
    Operation,
    MapSpecBuilder,
    VanillaSpecBuilder,
    format as yt_format,
)
from yt.wrapper.schema import TableSchema  # pyright: ignore[reportMissingImports]

from yt_framework.yt.client_base import BaseYTClient, OperationResources
from yt_framework.utils.ignore import YTIgnoreMatcher


class YTProdClient(BaseYTClient):
    """
    Production YT client implementation.
    
    Uses actual YTsaurus client for all operations.
    """

    def __init__(
        self,
        logger: logging.Logger,
        secrets: Dict[str, str],
    ) -> None:
        """
        Initialize production YT client.

        Args:
            logger: Logger instance
            secrets: Dictionary containing YT credentials. Expected keys:
                    - YT_PROXY
                    - YT_TOKEN
        """
        super().__init__(logger)

        yt_proxy = secrets.get("YT_PROXY")
        if not yt_proxy:
            raise ValueError("YT_PROXY is not set (check secrets.env or environment variables)")

        yt_token = secrets.get("YT_TOKEN")
        if not yt_token:
            raise ValueError("YT_TOKEN is not set (check secrets.env or environment variables)")

        self.client = YtClient(proxy=yt_proxy, token=yt_token)
        try:
            if "proxy" in self.client.config:
                self.client.config["proxy"]["enable_proxy_discovery"] = False  # type: ignore[index]
                self.logger.debug(f"YT Client initialized with proxy: {yt_proxy} (proxy discovery disabled)")
            else:
                self.logger.debug(f"YT Client initialized with proxy: {yt_proxy}")
        except Exception as e:
            self.logger.warning(
                f"Could not disable proxy discovery: {e}. Continuing with default settings."
            )
            self.logger.debug(f"YT Client initialized with proxy: {yt_proxy}")

    def create_path(
        self,
        path: str,
        node_type: Literal["table", "file", "map_node", "list_node", "document"] = "map_node",
    ) -> None:
        """Create a path in YT."""
        try:
            self.client.create(node_type, path, recursive=True, ignore_existing=True)
        except Exception as e:
            self.logger.error(f"Failed to create path: {e}")
            raise

    def exists(self, path: str) -> bool:
        """Check if a path exists in YT."""
        try:
            return self.client.exists(path)
        except Exception as e:
            self.logger.error(f"Failed to check if path exists: {e}")
            raise

    def write_table(
        self,
        table_path: str,
        rows: List[Dict[str, Any]],
        append: bool = False,
        replication_factor: int = 1,
        make_parents: bool = True,
    ) -> None:
        """Write rows to a YT table.
        
        Args:
            table_path: YT table path
            rows: List of rows to write
            append: If True, append rows to the table
            replication_factor: Replication factor for the table
            make_parents: If True, create parent directories if they don't exist
        """
        mode_str = "append" if append else "overwrite"
        self.logger.info(f"Writing {len(rows)} rows → {table_path} ({mode_str})")

        try:
            # Create parent directories if they don't exist
            if make_parents and "/" in table_path:
                parent_dir = "/".join(table_path.rstrip("/").split("/")[:-1])
                if parent_dir:
                    self.logger.debug(f"Ensuring parent directory exists: {parent_dir}")
                    self.create_path(parent_dir, node_type="map_node")

            # Create table with replication factor if it doesn't exist
            if not append:
                if self.client.exists(table_path):
                    self.client.remove(table_path, force=True)
                self.client.create(
                    "table",
                    table_path,
                    attributes={"replication_factor": replication_factor},
                    ignore_existing=True,
                )

            self.client.write_table(
                TablePath(table_path, append=append),
                rows,
                format=yt_format.JsonFormat(),
            )
        except Exception as e:
            self.logger.error(f"Failed to write table: {e}")
            raise

    def read_table(self, table_path: str) -> List[Dict[str, Any]]:
        """Read rows from a YT table."""
        self.logger.info(f"Reading table: {table_path}")

        try:
            # Type ignore needed because YT client's read_table has complex return types
            # but when called with JsonFormat(), it returns an iterable of dicts
            table_iterator = self.client.read_table(
                TablePath(table_path), 
                format=yt_format.JsonFormat()
            )
            results: List[Dict[str, Any]] = list(table_iterator)  # type: ignore[arg-type]
            self.logger.info(f"✓ Read {len(results)} rows")
            return results
        except Exception as e:
            self.logger.error(f"Failed to read table: {e}")
            raise

    def row_count(self, table_path: str) -> int:
        """Get number of rows in a YT table."""
        try:
            count = self.client.row_count(table_path)
            self.logger.debug(f"Row count: {count}")
            return count
        except Exception as e:
            self.logger.error(f"Failed to get row count: {e}")
            raise

    def _get_table_columns(self, table_path: str) -> List[str]:
        """
        Get column names from a table.
        
        Tries multiple methods:
        1. Get schema from table attributes (handles binary columns)
        2. Read one row from table
        3. Use YQL query with LIMIT 0 to infer schema (when reading fails due to binary columns)
        
        Args:
            table_path: Path to YT table
            
        Returns:
            List of column names (filtered to exclude internal YQL columns)
            
        Raises:
            ValueError: If table is empty or cannot be read
        """
        # Method 1: Try to get schema from table attributes first (handles binary columns)
        try:
            attrs = self.client.get(table_path, attributes=["schema"])  # type: ignore[assignment]
            if attrs and isinstance(attrs, dict) and "schema" in attrs:  # type: ignore[operator]
                schema = attrs["schema"]  # type: ignore[index]
                if schema and isinstance(schema, list):
                    columns = [col["name"] for col in schema if isinstance(col, dict) and "name" in col]
                    # Filter out internal YQL columns like _other, _yql_column_*
                    columns = [col for col in columns if not col.startswith("_")]
                    if columns:
                        return columns
        except Exception as e:
            self.logger.debug(f"Could not get schema from attributes: {e}, trying to read table")
        
        # Method 2: Try to read one row (may fail with binary columns)
        try:
            rows = self.read_table(table_path)
            if not rows:
                raise ValueError(f"Table {table_path} is empty, cannot determine columns")
            # Get column names from first row
            columns = list(rows[0].keys())
            # Filter out internal YQL columns like _other, _yql_column_*
            columns = [col for col in columns if not col.startswith("_")]
            if not columns:
                # If all columns were filtered out, use all keys (fallback)
                columns = list(rows[0].keys())
            return columns
        except Exception as read_error:
            # Method 3: If reading fails (e.g., binary columns), use YQL to infer schema
            error_str = str(read_error)
            if "Failed to decode string" in error_str or "encoding" in error_str.lower():
                temp_output = None
                try:
                    self.logger.debug("Reading failed due to binary columns, using YQL to infer schema")
                    # Use YQL to create a temporary table with LIMIT 0 to infer schema
                    # This doesn't read actual data, just infers the schema
                    import uuid
                    temp_output = f"{table_path}.temp_schema_{uuid.uuid4().hex[:8]}"
                    query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO `{temp_output}` WITH TRUNCATE
SELECT * FROM `{table_path}` LIMIT 0;"""
                    
                    # Execute query to create temp table with schema
                    self.run_yql(query)
                    
                    # Get schema from the temporary table
                    temp_attrs = self.client.get(temp_output, attributes=["schema"])  # type: ignore[assignment]
                    if temp_attrs and isinstance(temp_attrs, dict) and "schema" in temp_attrs:  # type: ignore[operator]
                        temp_schema = temp_attrs["schema"]  # type: ignore[index]
                        if temp_schema and isinstance(temp_schema, list):
                            columns = [col["name"] for col in temp_schema if isinstance(col, dict) and "name" in col]
                            # Filter out internal YQL columns
                            columns = [col for col in columns if not col.startswith("_")]
                            if columns:
                                # Clean up temp table before returning
                                if temp_output:
                                    try:
                                        self.client.remove(temp_output)
                                    except Exception:
                                        pass
                                return columns
                    
                    # Clean up temp table if we got here
                    if temp_output:
                        try:
                            self.client.remove(temp_output)
                        except Exception:
                            pass
                except Exception as yql_error:
                    self.logger.debug("YQL schema inference failed: %s", yql_error)
                    # Clean up temp table if it was created
                    if temp_output:
                        try:
                            self.client.remove(temp_output)
                        except Exception:
                            pass
                
                # If all methods fail, provide helpful error message
                raise ValueError(
                    f"Table {table_path} contains binary columns that cannot be decoded. "
                    f"This usually happens when a table was created with SELECT * and contains "
                    f"internal YQL columns like _yql_column_0. Please recreate the table with "
                    f"explicit column selection, or delete and recreate it. Original error: {read_error}"
                ) from read_error
            
            raise ValueError(f"Failed to get table columns from {table_path}: {read_error}") from read_error

    def run_yql(
        self,
        query: str,
        pool: str = "default",
    ) -> None:
        """
        Execute a YQL query on YT cluster.
        
        Args:
            query: YQL query string to execute
            pool: YT pool name (default: 'default')
        """
        self.logger.info("Executing YQL query on YT cluster")
        self.logger.debug(f"Pool: {pool}")
        self.logger.debug(f"Query:\n{query}")

        try:
            # Execute YQL query on YT cluster using Python API
            query_obj = self.client.run_query(
                engine="yql",
                query=query,
                settings={"pool": pool},
            )

            # Wait for query to complete
            self.logger.info(f"Query started: {query_obj.id}")

            # Check result
            state = query_obj.get_state()
            if state == "completed":
                self.logger.info("✓ YQL query completed successfully")
            else:
                error = query_obj.get_error()
                raise RuntimeError(f"Query failed with state {state}: {error}")

        except Exception as e:
            self.logger.error(f"Failed to execute YQL query: {e}")
            raise

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
        """Join two tables using YQL."""
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
        """Filter table rows using WHERE condition."""
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
        """Select specific columns from a table."""
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
        """Group by columns and compute aggregations."""
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
        """Union multiple tables."""
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
        """Get distinct rows from a table."""
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
        """Sort table by columns."""
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
        """Limit number of rows from a table."""
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

    def upload_file(self, local_path: Path, yt_path: str, create_parent_dir: bool = False) -> None:
        """
        Upload a file to YT.
        
        Args:
            local_path: Local file path to upload
            yt_path: YT destination path
            create_parent_dir: If True, create parent directory if it doesn't exist (default: False)
        """
        self.logger.info(f"Uploading {local_path.name} → {yt_path}")

        try:
            # Ensure parent directory exists before uploading if requested
            if create_parent_dir:
                # Extract parent directory from yt_path (everything before the last '/')
                if "/" in yt_path:
                    parent_dir = "/".join(yt_path.split("/")[:-1])
                    if parent_dir:
                        self.logger.debug(f"Ensuring parent directory exists: {parent_dir}")
                        self.create_path(parent_dir, node_type="map_node")

            with open(local_path, "rb") as f:
                self.client.write_file(
                    yt_path,
                    f,
                    force_create=True,
                    compute_md5=True,
                )
            self.logger.debug(f"Upload completed: {yt_path}")
        except Exception as e:
            self.logger.error(f"Failed to upload file: {e}")
            raise

    def upload_directory(
        self, local_dir: Path, yt_dir: str, pattern: str = "*"
    ) -> List[str]:
        """Upload a directory to YT."""
        self.logger.info(f"Uploading directory {local_dir} → {yt_dir}")

        # Create YT directory
        self.create_path(yt_dir, node_type="map_node")

        # Initialize .ytignore matcher
        ignore_matcher = YTIgnoreMatcher(local_dir)

        uploaded = []
        ignored_count = 0
        for local_file in local_dir.rglob(pattern):
            if local_file.is_file():
                # Check if file should be ignored
                if ignore_matcher.should_ignore(local_file):
                    self.logger.debug(
                        f"Ignoring file (matched .ytignore): {local_file}"
                    )
                    ignored_count += 1
                    continue

                # Compute relative path
                rel_path = local_file.relative_to(local_dir)
                yt_path = f"{yt_dir}/{rel_path}".replace("\\", "/")

                # Create parent directories if needed
                parent = "/".join(yt_path.split("/")[:-1])
                if parent:
                    self.create_path(parent, node_type="map_node")

                # Upload file
                self.upload_file(local_file, yt_path)
                uploaded.append(yt_path)

        self.logger.info(f"Uploaded {len(uploaded)} files")
        if ignored_count > 0:
            self.logger.info(
                f"Ignored {ignored_count} files (matched .ytignore patterns)"
            )
        return uploaded

    def run_map(
        self,
        command: str,
        input_table: str,
        output_table: str,
        files: List[Tuple[str, str]],
        resources: OperationResources,
        env: Dict[str, str],
        output_schema: Optional[TableSchema] = None,
        max_failed_jobs: int = 1,
        docker_auth: Optional[Dict[str, str]] = None,
    ) -> Operation:
        """Run a map operation on YT."""
        self.logger.info("Submitting map operation")
        self.logger.info(f"  Input: {input_table}")
        self.logger.info(f"  Output: {output_table}")
        self.logger.info(f"  Output Schema: {output_schema}")
        self.logger.info(f"  Command: {command}")
        self.logger.info(f"  Files: {files}")
        self.logger.info(f"  Resources: {resources}")

        try:
            file_paths = [
                FilePath(yt_path, file_name=local_path) 
                for yt_path, local_path in files
            ]

            output_path = TablePath(output_table, append=False, schema=output_schema)
            spec_builder = (
                MapSpecBuilder()
                .pool(resources.pool)
                .resource_limits({"user_slots": resources.user_slots})
                .max_failed_job_count(max_failed_jobs)
                .job_count(resources.job_count)
                .input_table_paths([input_table])
                .output_table_paths([output_path])
            )
            
            # Set pool tree if specified
            if resources.pool_tree:
                spec_builder = spec_builder.pool_trees([resources.pool_tree])
                self.logger.debug(f"Set pool tree to {resources.pool_tree}")
            
            mapper_builder = (
                spec_builder.begin_mapper()
                .command(command)
                .format(yt_format.JsonFormat(encode_utf8=False))
                .file_paths(file_paths)
                .environment(env)
                .memory_limit(resources.memory_gb * 1024**3)
                .cpu_limit(resources.cpu_limit)
                .gpu_limit(resources.gpu_limit)
            )

            if resources.docker_image:
                mapper_builder = mapper_builder.docker_image(resources.docker_image)
                spec_builder.secure_vault({"docker_auth": docker_auth})

            mapper_builder = mapper_builder.end_mapper()

            operation = self.client.run_operation(spec_builder, sync=False)
            if operation is None:
                raise RuntimeError("Failed to submit operation: run_operation returned None")

            self.logger.info(f"Operation submitted: {operation.id}")
            return operation

        except Exception as e:
            self.logger.error(f"Failed to submit operation: {e}")
            raise

    def run_vanilla(
        self,
        command: str,
        files: List[Tuple[str, str]],
        env: Dict[str, str],
        task_name: str,
        resources: OperationResources,
        docker_auth: Optional[Dict[str, str]] = None,
        max_failed_jobs: int = 1,
    ) -> Operation:
        """Run a vanilla operation on YT."""
        self.logger.info("Submitting vanilla operation")
        self.logger.info(f"  Task Name: {task_name}")
        self.logger.info(f"  Command: {command}")
        self.logger.info(f"  Files: {files}")
        self.logger.info(f"  Resources: {resources}")

        try:
            file_paths = [
                FilePath(yt_path, file_name=local_path) 
                for yt_path, local_path in files
            ]

            spec_builder = (
                VanillaSpecBuilder()
                .pool(resources.pool)
                .resource_limits({"user_slots": resources.user_slots})
                .max_failed_job_count(max_failed_jobs)
            )
            
            # Set pool tree if specified
            if resources.pool_tree:
                spec_builder = spec_builder.pool_trees([resources.pool_tree])
                self.logger.debug(f"Set pool tree to {resources.pool_tree}")
            
            task_builder = (
                spec_builder
                .begin_task(task_name)
                .command(command)
                .file_paths(file_paths)
                .environment(env)
                .memory_limit(resources.memory_gb * 1024**3)
                .cpu_limit(resources.cpu_limit)
                .gpu_limit(resources.gpu_limit)
                .job_count(resources.job_count)
            )
            
            if resources.docker_image:
                task_builder = task_builder.docker_image(resources.docker_image)
                spec_builder.secure_vault({"docker_auth": docker_auth})

            task_builder.end_task()

            operation = self.client.run_operation(spec_builder, sync=False)
            if operation is None:
                raise RuntimeError("Failed to submit operation: run_operation returned None")

            self.logger.info(f"Operation submitted: {operation.id}")
            return operation

        except Exception as e:
            self.logger.error(f"Failed to submit vanilla operation: {e}")
            raise
