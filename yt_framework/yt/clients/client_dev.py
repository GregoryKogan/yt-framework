"""Local filesystem stand-in for Cypress tables and subprocess-backed jobs."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Literal

from yt_framework.yt._client_dev_runtime import (
    dev_columns_from_first_row,
    dev_run_yql_simulation,
)
from yt_framework.yt._client_split._client_dev_ops_mixin import ClientDevOpsMixin
from yt_framework.yt._client_split._client_dev_yql_mixin import ClientDevYqlMixin
from yt_framework.yt.clients.client_base import BaseYTClient
from yt_framework.yt.max_row_weight import ensure_max_row_weight_pragma


class YTDevClient(ClientDevYqlMixin, ClientDevOpsMixin, BaseYTClient):
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
            return dev_columns_from_first_row(rows, table_path)
        except ValueError:
            raise
        except Exception:
            self.logger.exception("Failed to get table columns")
            raise

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

        try:
            dev_run_yql_simulation(
                query_with_max_row_weight=query_with_max_row_weight,
                dev_dir=self._dev_dir(),
                logger=self.logger,
                table_local_path=self._table_local_path,
                write_table=lambda table, rows, *, append: self.write_table(
                    table,
                    rows,
                    append=append,
                ),
            )
        except Exception:
            self.logger.exception("Failed to execute YQL query in dev mode")
            raise
