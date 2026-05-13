"""Thin wrapper around `yt.wrapper.YtClient` for real cluster operations."""

import contextlib
import logging
import uuid
from typing import Any, Literal, cast

from yt.wrapper import TablePath, YtClient
from yt.wrapper import format as yt_format

from yt_framework.yt._client_prod_runtime import (
    _raise_runtime_error,
    disable_yt_proxy_discovery_best_effort,
    prod_create_table_parent_if_missing,
    prod_write_table_replace_or_create,
    read_required_yt_secret,
)
from yt_framework.yt._client_split._client_prod_ops_mixin import ClientProdOpsMixin
from yt_framework.yt._client_split._client_prod_yql_mixin import ClientProdYqlMixin
from yt_framework.yt.client_base import BaseYTClient
from yt_framework.yt.max_row_weight import (
    build_max_row_weight_pragma,
    ensure_max_row_weight_pragma,
)


def _raise_value_error(message: str) -> None:
    raise ValueError(message)


def _raise_value_error_from(cause: BaseException, message: str) -> None:
    raise ValueError(message) from cause


class YTProdClient(ClientProdYqlMixin, ClientProdOpsMixin, BaseYTClient):
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
            prod_create_table_parent_if_missing(
                make_parents=make_parents,
                table_path=table_path,
                create_path=self.create_path,
                logger=self.logger,
            )
            prod_write_table_replace_or_create(
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
