"""Thin wrapper around `yt.wrapper.YtClient` for real cluster operations."""

import contextlib
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from yt.wrapper import (  # pyright: ignore[reportMissingImports]
    FilePath,
    MapSpecBuilder,
    Operation,
    TablePath,
    TypedJob,
    VanillaSpecBuilder,
    YtClient,
)
from yt.wrapper import (
    format as yt_format,
)
from yt.wrapper.schema import TableSchema  # pyright: ignore[reportMissingImports]
from yt.wrapper.spec_builders import (  # pyright: ignore[reportMissingImports]
    MapReduceSpecBuilder,
    ReduceSpecBuilder,
)

from yt_framework.operations.job_command import (
    resolve_aliased_job as _resolve_aliased_job,
)
from yt_framework.utils.ignore import YTIgnoreMatcher
from yt_framework.yt.client_base import BaseYTClient, OperationResources
from yt_framework.yt.max_row_weight import (
    build_max_row_weight_pragma,
    ensure_max_row_weight_pragma,
    parse_max_row_weight_to_bytes,
    validate_max_row_weight,
)
from yt_framework.yt.operation_secure_env import (
    merge_secure_vault,
    partition_env_for_yt_spec,
    pop_secure_env_client_kwargs,
    wrap_shell_command_with_secure_vault_promotion,
)


def _maybe_wrap_string_command_for_vault(leg: Any, secure_flat: dict[str, str]) -> Any:
    if secure_flat and isinstance(leg, str):
        return wrap_shell_command_with_secure_vault_promotion(leg)
    return leg


def _partition_and_maybe_wrap_leg(
    leg: Any,
    env: dict[str, str],
    *,
    environment_public_keys: Any,
    use_plain_environment_for_secrets: bool,
) -> tuple[dict[str, str], dict[str, str], Any]:
    if use_plain_environment_for_secrets:
        public_env, secure_flat = dict(env), {}
    else:
        public_env, secure_flat = partition_env_for_yt_spec(
            env, environment_public_keys
        )
    leg = _maybe_wrap_string_command_for_vault(leg, secure_flat)
    return public_env, secure_flat, leg


def _spec_builder_secure_vault(spec_builder: Any, vault: Mapping[str, Any]) -> Any:
    if not vault:
        return spec_builder
    return spec_builder.secure_vault(dict(vault))


# YtClient.run_operation() only accepts these keyword args; everything else must be
# applied via SpecBuilder chain methods (weight, title, description, …).
_RUN_OPERATION_KWARGS = frozenset(
    {"enable_optimizations", "run_operation_mutation_id", "sync"}
)


def _apply_spec_options_and_split_run_operation_kwargs(
    spec_builder: Any,
    kwargs: dict[str, Any],
) -> tuple[Any, dict[str, Any]]:
    """Apply kwargs that correspond to SpecBuilder chain methods (e.g. ``weight``,
    ``title``, ``alias``). Remaining keys must be only those accepted by
    ``YtClient.run_operation`` (see ``_RUN_OPERATION_KWARGS``).
    """
    kwargs = dict(kwargs)
    run_op: dict[str, Any] = {}
    for k in list(kwargs.keys()):
        if k in _RUN_OPERATION_KWARGS:
            run_op[k] = kwargs.pop(k)
    for k, v in list(kwargs.items()):
        if k == "max_row_weight":
            del kwargs[k]
            continue
        meth = getattr(spec_builder, k, None)
        if meth is not None and callable(meth):
            spec_builder = meth(v)
            del kwargs[k]
        else:
            msg = (
                f"Unknown YT operation option {k!r}: not a SpecBuilder method on "
                f"{type(spec_builder).__name__} and not one of "
                f"{sorted(_RUN_OPERATION_KWARGS)}."
            )
            raise ValueError(msg)
    return spec_builder, run_op


def _apply_command_leg_format(
    leg_builder: Any,
    leg: Any,
) -> Any:
    """Configure wire format for command legs only.

    TypedJob legs keep their native typed protocol; command string legs use JSON.
    """
    if isinstance(leg, TypedJob):
        return leg_builder
    return leg_builder.format(yt_format.JsonFormat(encode_utf8=False))


def _apply_max_row_weight_to_spec_builder(
    spec_builder: Any,
    max_row_weight: str | None,
) -> Any:
    """Apply max row weight to spec builder when supported."""
    if max_row_weight is None:
        return spec_builder
    max_row_weight_bytes = parse_max_row_weight_to_bytes(max_row_weight)
    table_writer = getattr(spec_builder, "table_writer", None)
    if callable(table_writer):
        return table_writer({"max_row_weight": max_row_weight_bytes})
    job_io = getattr(spec_builder, "job_io", None)
    if callable(job_io):
        return job_io({"table_writer": {"max_row_weight": max_row_weight_bytes}})
    return spec_builder


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

        """
        super().__init__(logger)

        yt_proxy = secrets.get("YT_PROXY")
        if not yt_proxy:
            msg = "YT_PROXY is not set (check secrets.env or environment variables)"
            raise ValueError(msg)

        yt_token = secrets.get("YT_TOKEN")
        if not yt_token:
            msg = "YT_TOKEN is not set (check secrets.env or environment variables)"
            raise ValueError(msg)

        self.client = YtClient(proxy=yt_proxy, token=yt_token)
        self._apply_pickling_config(pickling or {})
        try:
            if "proxy" in self.client.config:
                self.client.config["proxy"]["enable_proxy_discovery"] = False  # type: ignore[index]
                self.logger.debug(
                    "YT Client initialized with proxy: %s (proxy discovery disabled)",
                    yt_proxy,
                )
            else:
                self.logger.debug("YT Client initialized with proxy: %s", yt_proxy)
        except Exception as e:
            self.logger.warning(
                "Could not disable proxy discovery: %s. Continuing with default settings.",
                e,
            )
            self.logger.debug("YT Client initialized with proxy: %s", yt_proxy)

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
        cfg = self.client.config.setdefault("pickling", {})  # type: ignore[attr-defined]
        if pickling.get("ignore_system_modules"):
            cfg["ignore_system_modules"] = True
            self.logger.debug("Pickling: ignore_system_modules=True")
        if pickling.get("disable_module_upload"):
            existing_module_filter = cfg.get("module_filter")

            def module_filter(module: Any) -> bool:
                if callable(existing_module_filter):
                    existing_module_filter(module)
                return False

            cfg["module_filter"] = module_filter
            self.logger.debug("Pickling: module_filter=<upload nothing>")

    def create_path(
        self,
        path: str,
        node_type: Literal[
            "table", "file", "map_node", "list_node", "document"
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
            # Create parent directories if they don't exist
            if make_parents and "/" in table_path:
                parent_dir = "/".join(table_path.rstrip("/").split("/")[:-1])
                if parent_dir:
                    self.logger.debug(
                        "Ensuring parent directory exists: %s", parent_dir
                    )
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
                TablePath(table_path), format=yt_format.JsonFormat()
            )
            results: list[dict[str, Any]] = list(table_iterator)  # type: ignore[arg-type]
            self.logger.info("✓ Read %s rows", len(results))
            return results
        except Exception:
            self.logger.exception("Failed to read table")
            raise

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
            return count
        except Exception:
            self.logger.exception("Failed to get row count")
            raise

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
        # Method 1: Try to get schema from table attributes first (handles binary columns)
        try:
            attrs = self.client.get(table_path, attributes=["schema"])  # type: ignore[assignment]
            if attrs and isinstance(attrs, dict) and "schema" in attrs:  # type: ignore[operator]
                schema = attrs["schema"]  # type: ignore[index]
                if schema and isinstance(schema, list):
                    columns = [
                        col["name"]
                        for col in schema
                        if isinstance(col, dict) and "name" in col
                    ]
                    # Filter out internal YQL columns like _other, _yql_column_*
                    columns = [col for col in columns if not col.startswith("_")]
                    if columns:
                        return columns
        except Exception as e:
            self.logger.debug(
                "Could not get schema from attributes: %s, trying to read table", e
            )

        # Method 2: Try to read one row (may fail with binary columns)
        try:
            rows = self.read_table(table_path)
            if not rows:
                msg = f"Table {table_path} is empty, cannot determine columns"
                raise ValueError(msg)
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
            if (
                "Failed to decode string" in error_str
                or "encoding" in error_str.lower()
            ):
                temp_output = None
                try:
                    self.logger.debug(
                        "Reading failed due to binary columns, using YQL to infer schema"
                    )
                    # Use YQL to create a temporary table with LIMIT 0 to infer schema
                    # This doesn't read actual data, just infers the schema
                    import uuid

                    temp_output = f"{table_path}.temp_schema_{uuid.uuid4().hex[:8]}"
                    query = f"""{build_max_row_weight_pragma()}
PRAGMA yt.InferSchema = '1';
INSERT INTO `{temp_output}` WITH TRUNCATE
SELECT * FROM `{table_path}` LIMIT 0;"""

                    # Execute query to create temp table with schema
                    self.run_yql(query)

                    # Get schema from the temporary table
                    temp_attrs = self.client.get(temp_output, attributes=["schema"])  # type: ignore[assignment]
                    if (
                        temp_attrs
                        and isinstance(temp_attrs, dict)
                        and "schema" in temp_attrs
                    ):  # type: ignore[operator]
                        temp_schema = temp_attrs["schema"]  # type: ignore[index]
                        if temp_schema and isinstance(temp_schema, list):
                            columns = [
                                col["name"]
                                for col in temp_schema
                                if isinstance(col, dict) and "name" in col
                            ]
                            # Filter out internal YQL columns
                            columns = [
                                col for col in columns if not col.startswith("_")
                            ]
                            if columns:
                                # Clean up temp table before returning
                                if temp_output:
                                    with contextlib.suppress(Exception):
                                        self.client.remove(temp_output)
                                return columns

                    # Clean up temp table if we got here
                    if temp_output:
                        with contextlib.suppress(Exception):
                            self.client.remove(temp_output)
                except Exception as yql_error:
                    self.logger.debug("YQL schema inference failed: %s", yql_error)
                    # Clean up temp table if it was created
                    if temp_output:
                        with contextlib.suppress(Exception):
                            self.client.remove(temp_output)

                # If all methods fail, provide helpful error message
                msg = (
                    f"Table {table_path} contains binary columns that cannot be decoded. "
                    f"This usually happens when a table was created with SELECT * and contains "
                    f"internal YQL columns like _yql_column_0. Please recreate the table with "
                    f"explicit column selection, or delete and recreate it. Original error: {read_error}"
                )
                raise ValueError(msg) from read_error

            msg = f"Failed to get table columns from {table_path}: {read_error}"
            raise ValueError(msg) from read_error

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
                raise RuntimeError(msg)

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

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
        from yt_framework.yt.yql_builder import build_join_query

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
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Filter table rows using WHERE condition.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            condition: WHERE condition (e.g., "status = 'active' AND total > 100")
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
        from yt_framework.yt.yql_builder import build_filter_query

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
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Select specific columns from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            columns: List of column names to select
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
        from yt_framework.yt.yql_builder import build_select_query

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

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
        from yt_framework.yt.yql_builder import build_group_by_query

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
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Union multiple tables.

        Args:
            tables: List of table paths to union
            output_table: Path to output table
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
        from yt_framework.yt.yql_builder import build_union_query

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
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Get distinct rows from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            columns: Optional list of columns to select (if None, selects all)
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
        from yt_framework.yt.yql_builder import build_distinct_query

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

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
        from yt_framework.yt.yql_builder import build_sort_query

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
        dry_run: bool = False,
        max_row_weight: str | None = None,
    ) -> str | None:
        """Limit number of rows from a table.

        Args:
            input_table: Path to input table
            output_table: Path to output table
            limit: Maximum number of rows to return
            dry_run: If True, return the YQL query without executing

        Returns:
            YQL query string if dry_run=True, None otherwise

        """
        from yt_framework.yt.yql_builder import build_limit_query

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
        self, local_path: Path, yt_path: str, create_parent_dir: bool = False
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
            if create_parent_dir:
                # Extract parent directory from yt_path (everything before the last '/')
                if "/" in yt_path:
                    parent_dir = "/".join(yt_path.split("/")[:-1])
                    if parent_dir:
                        self.logger.debug(
                            "Ensuring parent directory exists: %s", parent_dir
                        )
                        self.create_path(parent_dir, node_type="map_node")

            with open(local_path, "rb") as f:
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
        self, local_dir: Path, yt_dir: str, pattern: str = "*"
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
            "Uploading directory %s → %s", local_dir, yt_dir
        )  # Create YT directory
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
                        "Ignoring file (matched .ytignore): %s", local_file
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

        self.logger.info("Uploaded %s files", len(uploaded))
        if ignored_count > 0:
            self.logger.info(
                "Ignored %s files (matched .ytignore patterns)", ignored_count
            )
        return uploaded

    def run_map(
        self,
        command: Any,
        input_table: str,
        output_table: str,
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: TableSchema | None = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: Any = None,
        append: bool = False,
        **kwargs: Any,
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
                kwargs.get("max_row_weight")
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
                self.logger.debug("Set pool tree to %s", resources.pool_tree)

            mapper_builder = (
                spec_builder.begin_mapper()
                .command(mapper_job)
                .file_paths(file_paths)
                .environment(public_env)
                .memory_limit(resources.memory_gb * 1024**3)
                .cpu_limit(resources.cpu_limit)
                .gpu_limit(resources.gpu_limit)
            )
            mapper_builder = _apply_command_leg_format(mapper_builder, mapper_job)

            if resources.docker_image:
                mapper_builder = mapper_builder.docker_image(resources.docker_image)

            mapper_builder = mapper_builder.end_mapper()
            spec_builder = _spec_builder_secure_vault(spec_builder, merged_vault)
            spec_builder = _apply_max_row_weight_to_spec_builder(
                spec_builder,
                kwargs.get("max_row_weight"),
            )

            spec_builder, run_op = _apply_spec_options_and_split_run_operation_kwargs(
                spec_builder, kwargs
            )
            run_op.setdefault("sync", False)
            operation = self.client.run_operation(spec_builder, **run_op)
            if operation is None:
                msg = "Failed to submit operation: run_operation returned None"
                raise RuntimeError(msg)

            self.logger.info("Operation submitted: %s", operation.id)
            return operation

        except Exception:
            self.logger.exception("Failed to submit operation")
            raise

    def run_vanilla(
        self,
        command: str,
        files: list[tuple[str, str]],
        env: dict[str, str],
        task_name: str,
        resources: OperationResources,
        docker_auth: dict[str, str] | None = None,
        max_failed_jobs: int = 1,
        job: str | None = None,
        **kwargs: Any,
    ) -> Operation:
        """Run a vanilla operation on YT cluster.

        Submits a vanilla operation that runs a standalone job without input/output tables.
        The operation runs on the YT cluster with the specified resources and dependencies.

        Args:
            command: Legacy command argument (typically bash command with script path).
            files: List of (yt_path, local_path) tuples for dependencies.
            env: Environment variables dictionary.
            task_name: Task name for the operation.
            resources: Operation resource configuration (memory, CPU, GPU, etc.).
            docker_auth: Optional Docker authentication for private registries.
            max_failed_jobs: Maximum failed jobs allowed before operation fails.
            job: Preferred command alias.
            **kwargs: Extra options applied to the spec builder (e.g. weight, title) or
                forwarded to run_operation (sync, enable_optimizations).

        Returns:
            Operation: YT operation object that can be monitored and waited on.

        Raises:
            Exception: If operation submission fails.

        """
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
                kwargs.get("max_row_weight")
            )
            file_paths = [
                FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
            ]

            od = kwargs.pop("operation_description", None)

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

            spec_builder = (
                VanillaSpecBuilder()
                .pool(resources.pool)
                .resource_limits({"user_slots": resources.user_slots})
                .max_failed_job_count(max_failed_jobs)
            )

            if isinstance(od, dict):
                spec_builder = spec_builder.description(od)

            # Set pool tree if specified
            if resources.pool_tree:
                spec_builder = spec_builder.pool_trees([resources.pool_tree])
                self.logger.debug("Set pool tree to %s", resources.pool_tree)

            task_builder = (
                spec_builder.begin_task(task_name)
                .command(vanilla_job)
                .file_paths(file_paths)
                .environment(public_env)
                .memory_limit(resources.memory_gb * 1024**3)
                .cpu_limit(resources.cpu_limit)
                .gpu_limit(resources.gpu_limit)
                .job_count(resources.job_count)
            )

            if resources.docker_image:
                task_builder = task_builder.docker_image(resources.docker_image)

            task_builder.end_task()
            spec_builder = _spec_builder_secure_vault(spec_builder, merged_vault)
            spec_builder = _apply_max_row_weight_to_spec_builder(
                spec_builder,
                kwargs.get("max_row_weight"),
            )

            spec_builder, run_op = _apply_spec_options_and_split_run_operation_kwargs(
                spec_builder, kwargs
            )
            run_op.setdefault("sync", False)
            operation = self.client.run_operation(spec_builder, **run_op)
            if operation is None:
                msg = "Failed to submit operation: run_operation returned None"
                raise RuntimeError(msg)

            self.logger.info("Operation submitted: %s", operation.id)
            return operation

        except Exception:
            self.logger.exception("Failed to submit vanilla operation")
            raise

    def run_map_reduce(
        self,
        mapper: Any,
        reducer: Any,
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
        map_job: Any = None,
        reduce_job: Any = None,
        **kwargs: Any,
    ) -> Operation:
        """Run a map-reduce operation on YT cluster."""
        self.logger.info("Submitting map-reduce operation")
        self.logger.info("  Input: %s -> Output: %s", input_table, output_table)
        self.logger.info("  Reduce by: %s", reduce_by)

        try:
            kwargs = dict(kwargs)
            environment_public_keys, use_plain, user_secure_vault = (
                pop_secure_env_client_kwargs(kwargs)
            )
            kwargs["max_row_weight"] = validate_max_row_weight(
                kwargs.get("max_row_weight")
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

            spec_builder = (
                MapReduceSpecBuilder()
                .input_table_paths([source_table])
                .output_table_paths([dest_table])
                .pool(resources.pool)
                .max_failed_job_count(max_failed_jobs)
            )
            if resources.pool_tree:
                spec_builder = spec_builder.pool_trees([resources.pool_tree])
            if resources.user_slots:
                spec_builder = spec_builder.resource_limits(
                    {"user_slots": resources.user_slots}
                )

            od = kwargs.pop("operation_description", None)
            if isinstance(od, dict):
                spec_builder = spec_builder.description(od)

            mapper_builder = (
                spec_builder.begin_mapper()
                .command(mapper_leg)
                .file_paths(file_paths)
                .environment(public_env)
                .memory_limit(resources.memory_gb * 1024**3)
                .cpu_limit(resources.cpu_limit)
                .gpu_limit(resources.gpu_limit)
            )
            mapper_builder = _apply_command_leg_format(mapper_builder, mapper_leg)
            if resources.docker_image:
                mapper_builder = mapper_builder.docker_image(resources.docker_image)
            mapper_builder.end_mapper()

            reducer_builder = (
                spec_builder.begin_reducer()
                .command(reducer_leg)
                .file_paths(file_paths)
                .environment(public_env)
                .memory_limit(resources.memory_gb * 1024**3)
                .cpu_limit(resources.cpu_limit)
                .gpu_limit(resources.gpu_limit)
            )
            reducer_builder = _apply_command_leg_format(reducer_builder, reducer_leg)
            if resources.docker_image:
                reducer_builder = reducer_builder.docker_image(resources.docker_image)
            reducer_builder.end_reducer()

            spec_builder = _spec_builder_secure_vault(spec_builder, merged_vault)
            spec_builder = spec_builder.reduce_by(reduce_by)
            if sort_by:
                spec_builder = spec_builder.sort_by(sort_by)
            map_job_count = kwargs.pop("map_job_count", None)
            if map_job_count is not None:
                spec_builder = spec_builder.map_job_count(map_job_count)
            spec_builder = _apply_max_row_weight_to_spec_builder(
                spec_builder,
                kwargs.get("max_row_weight"),
            )

            spec_builder, run_op = _apply_spec_options_and_split_run_operation_kwargs(
                spec_builder, kwargs
            )
            run_op.setdefault("sync", False)
            operation = self.client.run_operation(spec_builder, **run_op)
            if operation is None:
                msg = "Failed to submit map-reduce operation"
                raise RuntimeError(msg)
            self.logger.info("Map-reduce operation submitted: %s", operation.id)
            return operation
        except Exception:
            self.logger.exception("Failed to submit map-reduce operation")
            raise

    def run_reduce(
        self,
        reducer: Any,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: TableSchema | None = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: Any = None,
        **kwargs: Any,
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
                kwargs.get("max_row_weight")
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

            spec_builder = (
                ReduceSpecBuilder()
                .input_table_paths([source_table])
                .output_table_paths([dest_table])
                .pool(resources.pool)
                .max_failed_job_count(max_failed_jobs)
            )
            if resources.pool_tree:
                spec_builder = spec_builder.pool_trees([resources.pool_tree])
            if resources.user_slots:
                spec_builder = spec_builder.resource_limits(
                    {"user_slots": resources.user_slots}
                )

            rod = kwargs.pop("operation_description", None)
            if isinstance(rod, dict):
                spec_builder = spec_builder.description(rod)

            reducer_builder = (
                spec_builder.begin_reducer()
                .command(reducer_leg)
                .file_paths(file_paths)
                .environment(public_env)
                .memory_limit(resources.memory_gb * 1024**3)
                .cpu_limit(resources.cpu_limit)
                .gpu_limit(resources.gpu_limit)
            )
            reducer_builder = _apply_command_leg_format(reducer_builder, reducer_leg)
            if resources.docker_image:
                reducer_builder = reducer_builder.docker_image(resources.docker_image)
            reducer_builder.end_reducer()

            spec_builder = _spec_builder_secure_vault(spec_builder, merged_vault)
            spec_builder = spec_builder.reduce_by(reduce_by)
            spec_builder = _apply_max_row_weight_to_spec_builder(
                spec_builder,
                kwargs.get("max_row_weight"),
            )

            spec_builder, run_op = _apply_spec_options_and_split_run_operation_kwargs(
                spec_builder, kwargs
            )
            run_op.setdefault("sync", False)
            operation = self.client.run_operation(spec_builder, **run_op)
            if operation is None:
                msg = "Failed to submit reduce operation"
                raise RuntimeError(msg)
            self.logger.info("Reduce operation submitted: %s", operation.id)
            return operation
        except Exception:
            self.logger.exception("Failed to submit reduce operation")
            raise

    def run_sort(
        self,
        table_path: str,
        sort_by: list[str],
        pool: str | None = None,
        pool_tree: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Sort a table in place by the given columns."""
        self.logger.info("Sorting table %s by %s", table_path, sort_by)
        try:
            from yt.wrapper import (
                schema as yt_schema,
            )  # pyright: ignore[reportMissingImports]

            sort_columns = [
                yt_schema.SortColumn(col, sort_order="ascending") for col in sort_by
            ]
            spec: dict = dict(kwargs.pop("spec", None) or {})
            if pool:
                spec["pool"] = pool
            if pool_tree:
                spec["pool_tree"] = pool_tree
            if spec:
                kwargs["spec"] = spec
            self.client.run_sort(table_path, sort_by=sort_columns, **kwargs)
            self.logger.info("Sort completed")
        except Exception:
            self.logger.exception("Failed to sort table")
            raise
