"""Thin wrapper around `yt.wrapper.YtClient` for real cluster operations."""

import contextlib
import logging
import uuid
from collections.abc import Collection
from pathlib import Path
from typing import Any, Literal, cast

from yt.wrapper import (
    FilePath,
    Operation,
    TablePath,
    YtClient,
)
from yt.wrapper import (
    format as yt_format,
)
from yt.wrapper import (
    schema as yt_schema,
)
from yt.wrapper.schema import TableSchema

from yt_framework.operations.job_command import (
    resolve_aliased_job as _resolve_aliased_job,
)
from yt_framework.utils.ignore import YTIgnoreMatcher
from yt_framework.yt._client_prod_runtime import (
    _apply_command_leg_format,
    _optional_str_kw,
    _raise_runtime_error,
    _spec_builder_secure_vault,
    disable_yt_proxy_discovery_best_effort,
    prod_assemble_map_spec_with_vault,
    prod_assemble_vanilla_spec_with_vault,
    prod_map_reduce_after_legs,
    prod_map_reduce_open_spec_builder,
    prod_maybe_create_parent_for_table_path,
    prod_merge_sort_spec_into_kwargs,
    prod_reduce_finish_reducer_leg,
    prod_reduce_open_spec_builder,
    prod_submit_operation_with_kwargs,
    prod_upload_directory_files,
    prod_write_table_create_or_replace_if_needed,
    read_required_yt_secret,
)
from yt_framework.yt.client_base import BaseYTClient, OperationResources
from yt_framework.yt.max_row_weight import (
    build_max_row_weight_pragma,
    ensure_max_row_weight_pragma,
    validate_max_row_weight,
)
from yt_framework.yt.operation_secure_env import (
    merge_secure_vault,
    partition_env_for_yt_spec,
    pop_secure_env_client_kwargs,
    wrap_shell_command_with_secure_vault_promotion,
)
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


def _raise_value_error(message: str) -> None:
    raise ValueError(message)


def _raise_value_error_from(cause: BaseException, message: str) -> None:
    raise ValueError(message) from cause


def _public_env_keys_for_partition(raw: object) -> Collection[str] | None:
    if raw is None:
        return None
    if isinstance(raw, (list, tuple, set, frozenset)):
        return [str(x) for x in raw]
    return [str(raw)]


def _maybe_wrap_string_command_for_vault(
    leg: object, secure_flat: dict[str, str]
) -> object:
    if secure_flat and isinstance(leg, str):
        return wrap_shell_command_with_secure_vault_promotion(leg)
    return leg


def _partition_and_maybe_wrap_leg(
    leg: object,
    env: dict[str, str],
    *,
    environment_public_keys: object,
    use_plain_environment_for_secrets: bool,
) -> tuple[dict[str, str], dict[str, str], Any]:
    if use_plain_environment_for_secrets:
        public_env, secure_flat = dict(env), {}
    else:
        public_env, secure_flat = partition_env_for_yt_spec(
            env,
            _public_env_keys_for_partition(environment_public_keys),
        )
    leg = _maybe_wrap_string_command_for_vault(leg, secure_flat)
    return public_env, secure_flat, leg


class YTProdClient(BaseYTClient):
    """Production YT client implementation.

    Uses actual YTsaurus client for all operations.
    """

    def __init__(
        self,
        logger: logging.Logger,
        secrets: dict[str, str],
        pickling: dict[str, Any] | None = None,
    ) -> None:
        """Initialize production YT client.

        Args:
            logger: Logger instance
            secrets: Dictionary containing YT credentials. Expected keys:
                    - YT_PROXY
                    - YT_TOKEN
            pickling: Optional pickling-related client config (see ``_apply_pickling_config``).

        """
        super().__init__(logger)

        yt_proxy = read_required_yt_secret(
            secrets,
            key="YT_PROXY",
            missing_message="YT_PROXY is not set (check secrets.env or environment variables)",
        )
        yt_token = read_required_yt_secret(
            secrets,
            key="YT_TOKEN",
            missing_message="YT_TOKEN is not set (check secrets.env or environment variables)",
        )

        self.client = YtClient(proxy=yt_proxy, token=yt_token)
        self._apply_pickling_config(pickling or {})
        disable_yt_proxy_discovery_best_effort(self.client, self.logger, yt_proxy)

    def _apply_pickling_config(self, pickling: dict[str, Any]) -> None:
        """Apply pickling flags from pipeline config to the YT client.

        Supported flags:
          ignore_system_modules (bool): Skip stdlib/site-packages from auto-upload.
              Prevents shadow packages (certifi, importlib, boto3, etc.) from polluting
              the worker sandbox. Safe default for Docker-based jobs.
          disable_module_upload (bool): Skip ALL automatic module uploads.
              Worker relies entirely on the Docker image + source.tar.gz.
        """
        if not pickling:
            return
        cfg = cast("dict[str, Any]", self.client.config.setdefault("pickling", {}))
        if pickling.get("ignore_system_modules"):
            cfg["ignore_system_modules"] = True
            self.logger.debug("Pickling: ignore_system_modules=True")
        if pickling.get("disable_module_upload"):
            existing_module_filter = cfg.get("module_filter")

            def module_filter(module: object) -> bool:
                if callable(existing_module_filter):
                    existing_module_filter(module)
                return False

            cfg["module_filter"] = module_filter
            self.logger.debug("Pickling: module_filter=<upload nothing>")

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
        """Create a path in YT.

        Args:
            path: YT path to create.
            node_type: Type of node to create (default: "map_node").

        Returns:
            None

        Raises:
            Exception: If path creation fails.

        """
        try:
            self.client.create(node_type, path, recursive=True, ignore_existing=True)
        except Exception:
            self.logger.exception("Failed to create path")
            raise

    def exists(self, path: str) -> bool:
        """Check if a path exists in YT.

        Args:
            path: YT path to check.

        Returns:
            bool: True if path exists, False otherwise.

        Raises:
            Exception: If check fails.

        """
        try:
            return self.client.exists(path)
        except Exception:
            self.logger.exception("Failed to check if path exists")
            raise

    def write_table(
        self,
        table_path: str,
        rows: list[dict[str, Any]],
        *,
        append: bool = False,
        replication_factor: int = 1,
        make_parents: bool = True,
    ) -> None:
        """Write rows to a YT table.

        Args:
            table_path: YT table path
            rows: List of dictionaries representing table rows
            append: If True, append to existing table (default: False)
            replication_factor: Replication factor for the table (default: 1)
            make_parents: If True, create parent directories if they don't exist (default: True)

        """
        mode_str = "append" if append else "overwrite"
        self.logger.info("Writing %s rows → %s (%s)", len(rows), table_path, mode_str)

        try:
            prod_maybe_create_parent_for_table_path(
                make_parents=make_parents,
                table_path=table_path,
                create_path=self.create_path,
                logger=self.logger,
            )
            prod_write_table_create_or_replace_if_needed(
                self.client,
                append=append,
                table_path=table_path,
                replication_factor=replication_factor,
            )

            self.client.write_table(
                TablePath(table_path, append=append),
                rows,
                format=yt_format.JsonFormat(),
            )
        except Exception:
            self.logger.exception("Failed to write table")
            raise

    def read_table(self, table_path: str) -> list[dict[str, Any]]:
        """Read rows from a YT table.

        Args:
            table_path: YT table path to read.

        Returns:
            List[Dict[str, Any]]: List of dictionaries representing table rows.

        Raises:
            Exception: If table read fails.

        """
        self.logger.info("Reading table: %s", table_path)

        try:
            # Type ignore needed because YT client's read_table has complex return types
            # but when called with JsonFormat(), it returns an iterable of dicts
            table_iterator = self.client.read_table(
                TablePath(table_path),
                format=yt_format.JsonFormat(),
            )
            results: list[dict[str, Any]] = list(table_iterator)  # type: ignore[arg-type]
            self.logger.info("✓ Read %s rows", len(results))
        except Exception:
            self.logger.exception("Failed to read table")
            raise
        else:
            return results

    def row_count(self, table_path: str) -> int:
        """Get number of rows in a YT table.

        Args:
            table_path: YT table path.

        Returns:
            int: Number of rows in the table.

        Raises:
            Exception: If row count query fails.

        """
        try:
            count = self.client.row_count(table_path)
            self.logger.debug("Row count: %s", count)
        except Exception:
            self.logger.exception("Failed to get row count")
            raise
        else:
            return count

    @staticmethod
    def _filter_internal_yql_columns(columns: list[str]) -> list[str]:
        return [col for col in columns if not col.startswith("_")]

    @staticmethod
    def _extract_columns_from_schema_value(schema: object) -> list[str]:
        if not isinstance(schema, list):
            return []
        columns = [
            col["name"] for col in schema if isinstance(col, dict) and "name" in col
        ]
        return YTProdClient._filter_internal_yql_columns(columns)

    def _get_columns_from_table_attributes(self, table_path: str) -> list[str]:
        attrs = self.client.get(table_path, attributes=["schema"])
        if not (attrs and isinstance(attrs, dict) and "schema" in attrs):
            return []
        return self._extract_columns_from_schema_value(attrs["schema"])

    def _get_columns_from_first_row(self, table_path: str) -> list[str]:
        rows = self.read_table(table_path)
        if not rows:
            _raise_value_error(f"Table {table_path} is empty, cannot determine columns")
        filtered = self._filter_internal_yql_columns(list(rows[0].keys()))
        if filtered:
            return filtered
        return list(rows[0].keys())

    @staticmethod
    def _is_binary_decode_error(error: Exception) -> bool:
        error_str = str(error)
        return "Failed to decode string" in error_str or "encoding" in error_str.lower()

    def _infer_columns_via_temp_yql_table(self, table_path: str) -> list[str]:
        temp_output = f"{table_path}.temp_schema_{uuid.uuid4().hex[:8]}"
        try:
            query = f"""{build_max_row_weight_pragma()}
PRAGMA yt.InferSchema = '1';
INSERT INTO `{temp_output}` WITH TRUNCATE
SELECT * FROM `{table_path}` LIMIT 0;"""  # noqa: S608
            self.run_yql(query)
            temp_attrs = self.client.get(temp_output, attributes=["schema"])
            if temp_attrs and isinstance(temp_attrs, dict) and "schema" in temp_attrs:
                return self._extract_columns_from_schema_value(temp_attrs["schema"])
            return []
        finally:
            with contextlib.suppress(Exception):
                self.client.remove(temp_output)

    def _handle_table_column_read_error(
        self,
        *,
        table_path: str,
        read_error: Exception,
    ) -> list[str]:
        if not self._is_binary_decode_error(read_error):
            _raise_value_error_from(
                read_error,
                f"Failed to get table columns from {table_path}: {read_error}",
            )
        self.logger.debug(
            "Reading failed due to binary columns, using YQL to infer schema",
        )
        try:
            columns = self._infer_columns_via_temp_yql_table(table_path)
            if columns:
                return columns
        except Exception as yql_error:  # noqa: BLE001
            self.logger.debug("YQL schema inference failed: %s", yql_error)

        msg = (
            f"Table {table_path} contains binary columns that cannot be decoded. "
            "This usually happens when a table was created with SELECT * and contains "
            "internal YQL columns like _yql_column_0. Please recreate the table with "
            f"explicit column selection, or delete and recreate it. Original error: {read_error}"
        )
        _raise_value_error_from(read_error, msg)
        unreachable = "unreachable"
        raise AssertionError(unreachable)

    def _get_table_columns(self, table_path: str) -> list[str]:
        """Get column names from a table.

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
        try:
            columns = self._get_columns_from_table_attributes(table_path)
            if columns:
                return columns
        except Exception as e:  # noqa: BLE001
            # YT attribute/schema access is version-dependent; fall through to row read.
            self.logger.debug(
                "Could not get schema from attributes: %s, trying to read table",
                e,
            )

        try:
            return self._get_columns_from_first_row(table_path)
        except Exception as read_error:  # noqa: BLE001
            return self._handle_table_column_read_error(
                table_path=table_path,
                read_error=read_error,
            )

    def run_yql(
        self,
        query: str,
        pool: str = "default",
        max_row_weight: str | None = None,
    ) -> None:
        """Execute a YQL query on YT cluster.

        Args:
            query: YQL query string to execute
            pool: YT pool name (default: 'default')
            max_row_weight: Optional max row weight override

        Raises:
            Exception: If query execution fails

        """
        self.logger.info("Executing YQL query on YT cluster")
        self.logger.debug("Pool: %s", pool)
        query_with_max_row_weight = ensure_max_row_weight_pragma(
            query=query,
            max_row_weight=max_row_weight,
        )
        self.logger.debug("Query:\n%s", query_with_max_row_weight)

        try:
            # Execute YQL query on YT cluster using Python API
            query_obj = self.client.run_query(
                engine="yql",
                query=query_with_max_row_weight,
                settings={"pool": pool},
            )

            # Wait for query to complete
            self.logger.info("Query started: %s", query_obj.id)

            # Check result
            state = query_obj.get_state()
            if state == "completed":
                self.logger.info("✓ YQL query completed successfully")
            else:
                error = query_obj.get_error()
                msg = f"Query failed with state {state}: {error}"
                _raise_runtime_error(msg)

        except Exception:
            self.logger.exception("Failed to execute YQL query")
            raise

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
        """Join two tables using YQL.

        Args:
            left_table: Path to left table
            right_table: Path to right table
            output_table: Path to output table
            on: Join key(s) - column name(s) to join on
                - str: Same column name on both sides (e.g., "user_id")
                - List[str]: Multiple columns with same names (e.g., ["user_id", "region"])
                - Dict[str, str]: Different column names (e.g., {"left": "input_s3_path", "right": "path"})
            how: Join type - "inner", "left", "right", or "full"
            select_columns: Optional list of columns to select (with table aliases)
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight pragma for generated YQL.

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
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
        """Filter table rows using WHERE condition.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            condition: WHERE condition (e.g., "status = 'active' AND total > 100")
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight pragma for generated YQL.

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
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
        """Select specific columns from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            columns: List of column names to select
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight pragma for generated YQL.

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
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
        """Group by columns and compute aggregations.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            group_by: Column(s) to group by
            aggregations: Dict mapping output column names to aggregation functions
                         e.g., {"order_count": "count", "total_amount": "sum"}
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight pragma for generated YQL.

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
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
        """Union multiple tables.

        Args:
            tables: List of table paths to union
            output_table: Path to output table
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight pragma for generated YQL.

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
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
        """Get distinct rows from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            columns: Optional list of columns to select (if None, selects all)
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight pragma for generated YQL.

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
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
        """Sort table by columns.

        WARNING: Sorting large tables can be expensive. Use with caution.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            order_by: Column(s) to sort by
            ascending: Sort direction (True for ASC, False for DESC)
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight pragma for generated YQL.

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
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
        """Limit number of rows from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            limit: Maximum number of rows to return
            dry_run: If True, return the YQL query without executing
            max_row_weight: Optional max row weight pragma for generated YQL.

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
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
        """Upload a file to YT.

        Args:
            local_path: Local file path to upload
            yt_path: YT destination path
            create_parent_dir: If True, create parent directory if it doesn't exist (default: False)

        """
        self.logger.info("Uploading %s → %s", local_path.name, yt_path)
        try:
            # Ensure parent directory exists before uploading if requested
            if create_parent_dir and "/" in yt_path:
                # Extract parent directory from yt_path (everything before the last '/')
                parent_dir = "/".join(yt_path.split("/")[:-1])
                if parent_dir:
                    self.logger.debug(
                        "Ensuring parent directory exists: %s",
                        parent_dir,
                    )
                    self.create_path(parent_dir, node_type="map_node")

            with local_path.open("rb") as f:
                self.client.write_file(
                    yt_path,
                    f,
                    force_create=True,
                    compute_md5=True,
                )
            self.logger.debug("Upload completed: %s", yt_path)
        except Exception:
            self.logger.exception("Failed to upload file")
            raise

    def upload_directory(
        self,
        local_dir: Path,
        yt_dir: str,
        pattern: str = "*",
    ) -> list[str]:
        """Upload a directory to YT cluster.

        Recursively uploads all files from a local directory to a YT directory,
        respecting .ytignore patterns if present.

        Args:
            local_dir: Local directory path to upload
            yt_dir: YT destination directory path
            pattern: File pattern to match (default: "*" for all files)

        Returns:
            List of uploaded YT file paths

        Raises:
            Exception: If directory upload fails

        """
        self.logger.info(
            "Uploading directory %s → %s",
            local_dir,
            yt_dir,
        )
        return prod_upload_directory_files(
            local_dir=local_dir,
            yt_dir=yt_dir,
            pattern=pattern,
            ignore_matcher=YTIgnoreMatcher(local_dir),
            create_path=self.create_path,
            upload_file=self.upload_file,
            logger=self.logger,
        )

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
        """Run a map operation on YT cluster.

        Submits a map operation that processes each row of the input table independently
        and writes results to the output table. The operation runs on the YT cluster
        with the specified resources and dependencies.

        Args:
            command: Legacy mapper job argument (TypedJob instance or command string).
            input_table: Input YT table path.
            output_table: Output YT table path.
            files: List of (yt_path, local_path) tuples for dependencies.
            resources: Operation resource configuration (memory, CPU, GPU, etc.).
            env: Environment variables dictionary.
            output_schema: Optional output table schema for typed output.
            max_failed_jobs: Maximum failed jobs allowed before operation fails.
            docker_auth: Optional Docker authentication for private registries.
            job: Preferred mapper job alias.
            append: If True, append mapper output to an existing output table.
            **kwargs: SpecBuilder chain options (e.g. ``weight``, ``title``) and
                ``run_operation`` flags such as ``sync``.

        Returns:
            Operation: YT operation object that can be monitored and waited on.

        Raises:
            Exception: If operation submission fails.

        """
        self.logger.info("Submitting map operation")
        self.logger.info("  Input: %s", input_table)
        self.logger.info("  Output: %s", output_table)
        self.logger.info("  Append: %s", append)
        self.logger.info("  Output Schema: %s", output_schema)
        self.logger.info("  Command: %s", command)
        self.logger.info("  Files: %s", files)
        self.logger.info("  Resources: %s", resources)

        try:
            mapper_job = _resolve_aliased_job(
                legacy_name="command",
                legacy_value=command,
                preferred_name="job",
                preferred_value=job,
            )
            kwargs = dict(kwargs)
            environment_public_keys, use_plain, user_secure_vault = (
                pop_secure_env_client_kwargs(kwargs)
            )
            kwargs["max_row_weight"] = validate_max_row_weight(
                _optional_str_kw(kwargs.get("max_row_weight")),
            )
            file_paths = [
                FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
            ]

            public_env, secure_flat, mapper_job = _partition_and_maybe_wrap_leg(
                mapper_job,
                env,
                environment_public_keys=environment_public_keys,
                use_plain_environment_for_secrets=use_plain,
            )
            merged_vault = merge_secure_vault(
                secure_flat,
                docker_image=resources.docker_image,
                docker_auth=docker_auth,
                user_secure_vault=user_secure_vault,
            )

            output_path = TablePath(output_table, append=append, schema=output_schema)
            spec_builder = prod_assemble_map_spec_with_vault(
                input_table=input_table,
                output_path=output_path,
                resources=resources,
                max_failed_jobs=max_failed_jobs,
                mapper_job=mapper_job,
                file_paths=file_paths,
                public_env=public_env,
                merged_vault=merged_vault,
                logger=self.logger,
            )
            operation = prod_submit_operation_with_kwargs(
                self.client,
                self.logger,
                spec_builder,
                kwargs,
                none_message="Failed to submit operation: run_operation returned None",
                log_message="Operation submitted: %s",
            )
        except Exception:
            self.logger.exception("Failed to submit operation")
            raise
        else:
            return operation

    def run_vanilla(
        self,
        command: object,
        files: list[tuple[str, str]],
        env: dict[str, str],
        task_name: str,
        job: object = None,
        **kwargs: object,
    ) -> Operation | None:
        """Run a vanilla operation on YT cluster.

        Submits a vanilla operation that runs a standalone job without input/output tables.
        The operation runs on the YT cluster with the specified resources and dependencies.

        ``resources``, ``docker_auth``, and ``max_failed_jobs`` are passed via ``kwargs``
        (see :func:`yt_framework.operations.vanilla.run_vanilla`).

        Args:
            command: Legacy command argument (typically bash command with script path).
            files: List of (yt_path, local_path) tuples for dependencies.
            env: Environment variables dictionary.
            task_name: Task name for the operation.
            job: Preferred command alias.
            **kwargs: Must include ``resources=OperationResources``; may include
                ``docker_auth``, ``max_failed_jobs``, and SpecBuilder / ``run_operation`` keys.

        Returns:
            Operation object or None when submission fails.

        Raises:
            TypeError: If ``resources`` is missing or not an :class:`OperationResources`.

        """
        kw = dict(kwargs)
        resources_obj = kw.pop("resources", None)
        if not isinstance(resources_obj, OperationResources):
            msg = "run_vanilla requires resources=OperationResources in kwargs"
            raise TypeError(msg)
        resources = resources_obj
        docker_auth = cast("dict[str, str] | None", kw.pop("docker_auth", None))
        max_failed_jobs = int(cast("Any", kw.pop("max_failed_jobs", 1)))
        kwargs = kw

        self.logger.info("Submitting vanilla operation")
        self.logger.info("  Task Name: %s", task_name)
        self.logger.info("  Command: %s", command)
        self.logger.info("  Files: %s", files)
        self.logger.info("  Resources: %s", resources)

        try:
            vanilla_job = _resolve_aliased_job(
                legacy_name="command",
                legacy_value=command,
                preferred_name="job",
                preferred_value=job,
            )
            kwargs = dict(kwargs)
            environment_public_keys, use_plain, user_secure_vault = (
                pop_secure_env_client_kwargs(kwargs)
            )
            kwargs["max_row_weight"] = validate_max_row_weight(
                _optional_str_kw(kwargs.get("max_row_weight")),
            )
            file_paths = [
                FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
            ]

            operation_description = kwargs.pop("operation_description", None)

            public_env, secure_flat, vanilla_job = _partition_and_maybe_wrap_leg(
                vanilla_job,
                env,
                environment_public_keys=environment_public_keys,
                use_plain_environment_for_secrets=use_plain,
            )
            merged_vault = merge_secure_vault(
                secure_flat,
                docker_image=resources.docker_image,
                docker_auth=docker_auth,
                user_secure_vault=user_secure_vault,
            )

            spec_builder = prod_assemble_vanilla_spec_with_vault(
                resources=resources,
                max_failed_jobs=max_failed_jobs,
                task_name=task_name,
                vanilla_job=vanilla_job,
                file_paths=file_paths,
                public_env=public_env,
                merged_vault=merged_vault,
                logger=self.logger,
                operation_description=operation_description,
            )
            operation = prod_submit_operation_with_kwargs(
                self.client,
                self.logger,
                spec_builder,
                kwargs,
                none_message="Failed to submit operation: run_operation returned None",
                log_message="Operation submitted: %s",
            )
        except Exception:
            self.logger.exception("Failed to submit vanilla operation")
            raise
        else:
            return operation

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
        """Run a map-reduce operation on YT cluster."""
        self.logger.info("Submitting map-reduce operation")
        self.logger.info("  Input: %s -> Output: %s", input_table, output_table)
        self.logger.info("  Reduce by: %s", reduce_by)

        try:
            (
                kwargs,
                mapper_leg,
                reducer_leg,
                public_env,
                merged_vault,
                file_paths,
                source_table,
                dest_table,
            ) = self._prepare_map_reduce_runtime(
                mapper=mapper,
                reducer=reducer,
                map_job=map_job,
                reduce_job=reduce_job,
                env=env,
                resources=resources,
                docker_auth=docker_auth,
                files=files,
                input_table=input_table,
                output_table=output_table,
                output_schema=output_schema,
                kwargs=dict(cast("Any", kwargs)),
            )

            spec_builder = prod_map_reduce_open_spec_builder(
                source_table=source_table,
                dest_table=dest_table,
                resources=resources,
                max_failed_jobs=max_failed_jobs,
                kwargs=kwargs,
            )

            spec_builder = cast(
                "Any",
                self._configure_map_reduce_leg(
                    spec_builder=spec_builder,
                    leg_name="mapper",
                    leg_command=mapper_leg,
                    file_paths=file_paths,
                    public_env=public_env,
                    resources=resources,
                ),
            )
            spec_builder = cast(
                "Any",
                self._configure_map_reduce_leg(
                    spec_builder=spec_builder,
                    leg_name="reducer",
                    leg_command=reducer_leg,
                    file_paths=file_paths,
                    public_env=public_env,
                    resources=resources,
                ),
            )

            spec_builder = prod_map_reduce_after_legs(
                spec_builder,
                merged_vault,
                reduce_by,
                sort_by,
                kwargs,
            )
            operation = prod_submit_operation_with_kwargs(
                self.client,
                self.logger,
                spec_builder,
                kwargs,
                none_message="Failed to submit map-reduce operation",
                log_message="Map-reduce operation submitted: %s",
            )
        except Exception:
            self.logger.exception("Failed to submit map-reduce operation")
            raise
        else:
            return operation

    def _prepare_map_reduce_runtime(
        self,
        *,
        mapper: object,
        reducer: object,
        map_job: object,
        reduce_job: object,
        env: dict[str, str],
        resources: OperationResources,
        docker_auth: dict[str, str] | None,
        files: list[tuple[str, str]],
        input_table: str,
        output_table: str,
        output_schema: TableSchema | None,
        kwargs: dict[str, Any],
    ) -> tuple[
        dict[str, Any],
        object,
        object,
        dict[str, str],
        dict[str, str],
        list[FilePath],
        TablePath,
        TablePath,
    ]:
        mutable_kwargs = dict(kwargs)
        environment_public_keys, use_plain, user_secure_vault = (
            pop_secure_env_client_kwargs(mutable_kwargs)
        )
        mutable_kwargs["max_row_weight"] = validate_max_row_weight(
            _optional_str_kw(mutable_kwargs.get("max_row_weight")),
        )
        mapper_leg = _resolve_aliased_job(
            legacy_name="mapper",
            legacy_value=mapper,
            preferred_name="map_job",
            preferred_value=map_job,
        )
        reducer_leg = _resolve_aliased_job(
            legacy_name="reducer",
            legacy_value=reducer,
            preferred_name="reduce_job",
            preferred_value=reduce_job,
        )
        public_env, secure_flat, mapper_leg = _partition_and_maybe_wrap_leg(
            mapper_leg,
            env,
            environment_public_keys=environment_public_keys,
            use_plain_environment_for_secrets=use_plain,
        )
        reducer_leg = _maybe_wrap_string_command_for_vault(reducer_leg, secure_flat)
        merged_vault = merge_secure_vault(
            secure_flat,
            docker_image=resources.docker_image,
            docker_auth=docker_auth,
            user_secure_vault=user_secure_vault,
        )
        file_paths = [
            FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
        ]
        source_table = TablePath(input_table)
        dest_table = TablePath(output_table, append=False, schema=output_schema)
        return (
            mutable_kwargs,
            mapper_leg,
            reducer_leg,
            public_env,
            merged_vault,
            file_paths,
            source_table,
            dest_table,
        )

    def _configure_map_reduce_leg(
        self,
        *,
        spec_builder: object,
        leg_name: Literal["mapper", "reducer"],
        leg_command: object,
        file_paths: list[FilePath],
        public_env: dict[str, str],
        resources: OperationResources,
    ) -> object:
        leg_builder = (
            (
                cast("Any", spec_builder).begin_mapper()
                if leg_name == "mapper"
                else cast("Any", spec_builder).begin_reducer()
            )
            .command(leg_command)
            .file_paths(file_paths)
            .environment(public_env)
            .memory_limit(resources.memory_gb * 1024**3)
            .cpu_limit(resources.cpu_limit)
            .gpu_limit(resources.gpu_limit)
        )
        leg_builder = cast("Any", _apply_command_leg_format(leg_builder, leg_command))
        if resources.docker_image:
            leg_builder = leg_builder.docker_image(resources.docker_image)
        if leg_name == "mapper":
            leg_builder.end_mapper()
        else:
            leg_builder.end_reducer()
        return spec_builder

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
        """Run a reduce-only operation on YT cluster."""
        self.logger.info("Submitting reduce operation")
        self.logger.info("  Input: %s -> Output: %s", input_table, output_table)
        self.logger.info("  Reduce by: %s", reduce_by)

        try:
            reducer_leg = _resolve_aliased_job(
                legacy_name="reducer",
                legacy_value=reducer,
                preferred_name="job",
                preferred_value=job,
            )
            kwargs = dict(kwargs)
            environment_public_keys, use_plain, user_secure_vault = (
                pop_secure_env_client_kwargs(kwargs)
            )
            kwargs["max_row_weight"] = validate_max_row_weight(
                _optional_str_kw(kwargs.get("max_row_weight")),
            )
            public_env, secure_flat, reducer_leg = _partition_and_maybe_wrap_leg(
                reducer_leg,
                env,
                environment_public_keys=environment_public_keys,
                use_plain_environment_for_secrets=use_plain,
            )
            merged_vault = merge_secure_vault(
                secure_flat,
                docker_image=resources.docker_image,
                docker_auth=docker_auth,
                user_secure_vault=user_secure_vault,
            )
            file_paths = [
                FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
            ]
            source_table = TablePath(input_table)
            dest_table = TablePath(output_table, append=False, schema=output_schema)

            spec_builder = prod_reduce_open_spec_builder(
                source_table=source_table,
                dest_table=dest_table,
                resources=resources,
                max_failed_jobs=max_failed_jobs,
                kwargs=kwargs,
            )
            spec_builder = prod_reduce_finish_reducer_leg(
                spec_builder,
                reducer_leg=reducer_leg,
                file_paths=file_paths,
                public_env=public_env,
                resources=resources,
            )
            spec_builder = cast(
                "Any", _spec_builder_secure_vault(spec_builder, merged_vault)
            )
            spec_builder = spec_builder.reduce_by(reduce_by)
            operation = prod_submit_operation_with_kwargs(
                self.client,
                self.logger,
                spec_builder,
                kwargs,
                none_message="Failed to submit reduce operation",
                log_message="Reduce operation submitted: %s",
            )
        except Exception:
            self.logger.exception("Failed to submit reduce operation")
            raise
        else:
            return operation

    def run_sort(
        self,
        table_path: str,
        sort_by: list[str],
        pool: str | None = None,
        pool_tree: str | None = None,
        **kwargs: object,
    ) -> None:
        """Sort a table in place by the given columns."""
        self.logger.info("Sorting table %s by %s", table_path, sort_by)
        try:
            sort_columns = [
                yt_schema.SortColumn(col, sort_order="ascending") for col in sort_by
            ]
            call_kw = prod_merge_sort_spec_into_kwargs(
                dict(cast("Any", kwargs)),
                pool=pool,
                pool_tree=pool_tree,
            )
            self.client.run_sort(table_path, sort_by=sort_columns, **call_kw)
            self.logger.info("Sort completed")
        except Exception:
            self.logger.exception("Failed to sort table")
            raise
