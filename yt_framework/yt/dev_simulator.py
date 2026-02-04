"""
DuckDB Dev Mode Simulator
==========================

Simulates YQL operations locally using DuckDB for dev mode testing.
"""

import logging
import re
from pathlib import Path
from typing import List, Dict, Any, Optional

import duckdb


class DuckDBSimulator:
    """
    DuckDB-based simulator for YQL operations in dev mode.
    
    Provides local execution of SQL queries with YT-like table storage.
    """
    
    def __init__(self, dev_dir: Path, logger: logging.Logger):
        """
        Initialize DuckDB simulator.
        
        Args:
            dev_dir: Directory containing .jsonl table files
            logger: Logger instance
        """
        self.dev_dir = dev_dir
        self.logger = logger
        self.conn = duckdb.connect(":memory:")
        self.loaded_tables: Dict[str, str] = {}  # Maps YT paths to DuckDB table names
    
    def _table_basename(self, yt_path: str) -> str:
        """Extract basename from YT table path."""
        return yt_path.rstrip("/").split("/")[-1]
    
    def _sanitize_table_name(self, yt_path: str) -> str:
        """Convert YT path to valid DuckDB table name."""
        # Replace special characters with underscores
        basename = self._table_basename(yt_path)
        # Remove or replace special chars
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', basename)
        return f"yt_{sanitized}"
    
    def load_table(self, yt_path: str, local_jsonl_path: Path) -> str:
        """
        Load a .jsonl table file into DuckDB.
        
        Args:
            yt_path: YT table path (for reference)
            local_jsonl_path: Local path to .jsonl file
        
        Returns:
            DuckDB table name
        """
        if not local_jsonl_path.exists():
            self.logger.warning(f"Table file not found: {local_jsonl_path}")
            # Create empty table
            table_name = self._sanitize_table_name(yt_path)
            self.conn.execute(f"CREATE TABLE {table_name} (dummy INTEGER)")
            self.loaded_tables[yt_path] = table_name
            return table_name
        
        table_name = self._sanitize_table_name(yt_path)
        
        try:
            # DuckDB can read JSONL files directly
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_json_auto('{local_jsonl_path}', format='newline_delimited')
            """)
            
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            if count_result is None:
                row_count = 0
            else:
                row_count = count_result[0]
            self.logger.debug(f"Loaded {row_count} rows from {local_jsonl_path} into {table_name}")
            
            self.loaded_tables[yt_path] = table_name
            return table_name
            
        except Exception as e:
            self.logger.error(f"Failed to load table {yt_path}: {e}")
            raise
    
    def yql_to_sql(self, yql_query: str) -> tuple[str, Optional[str]]:
        """
        Convert YQL query to DuckDB SQL.
        
        This is a simplified conversion that handles common patterns.
        For complex queries, users should use raw run_yql with DuckDB-compatible SQL.
        
        Args:
            yql_query: YQL query string
        
        Returns:
            Tuple of (sql_query, output_table_path)
        """
        # Remove PRAGMA directives (YT-specific)
        sql = re.sub(r'PRAGMA\s+\w+\.[^;]+;', '', yql_query, flags=re.IGNORECASE)
        
        # Extract output table from INSERT INTO before transformation
        output_match = re.search(r'INSERT\s+INTO\s+`([^`]+)`', sql, re.IGNORECASE)
        output_table = output_match.group(1) if output_match else None
        
        # Remove INSERT INTO ... WITH TRUNCATE entirely
        # Pattern: INSERT INTO `table` WITH TRUNCATE
        sql = re.sub(
            r'INSERT\s+INTO\s+`[^`]+`\s+WITH\s+TRUNCATE\s+',
            '',
            sql,
            flags=re.IGNORECASE
        )
        
        # Replace YT table references with DuckDB table names
        for yt_path, db_table in self.loaded_tables.items():
            # Replace backtick-quoted table names
            sql = re.sub(
                rf'`{re.escape(yt_path)}`',
                db_table,
                sql,
                flags=re.IGNORECASE
            )
        
        # Clean up extra whitespace
        sql = re.sub(r'\s+', ' ', sql).strip()
        
        # Ensure query ends with semicolon
        if not sql.endswith(';'):
            sql += ';'
        
        return sql, output_table
    
    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results.
        
        Args:
            sql: SQL query string
        
        Returns:
            List of result rows as dicts
        """
        try:
            self.logger.debug(f"Executing SQL: {sql}")
            result = self.conn.execute(sql).fetchall()
            
            # Get column names
            if result:
                columns = [desc[0] for desc in self.conn.description]
                # Convert to list of dicts
                return [dict(zip(columns, row)) for row in result]
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"Failed to execute SQL query: {e}")
            self.logger.error(f"Query: {sql}")
            raise
    
    def execute_yql(self, yql_query: str) -> tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Execute YQL query by converting to SQL.
        
        Args:
            yql_query: YQL query string
        
        Returns:
            Tuple of (results, output_table_path)
        """
        sql, output_table = self.yql_to_sql(yql_query)
        results = self.execute_query(sql)
        return results, output_table
    
    def close(self):
        """Close DuckDB connection."""
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()


def extract_table_references(yql_query: str) -> List[str]:
    """
    Extract table references from YQL query.
    
    This is a simple regex-based parser that finds table names in backticks.
    
    Args:
        yql_query: YQL query string
    
    Returns:
        List of table paths referenced in the query
    """
    # Find all backtick-quoted strings that look like table paths
    pattern = r'`(//[^`]+)`'
    matches = re.findall(pattern, yql_query)
    
    # Filter out output tables (those in INSERT INTO)
    output_pattern = r'INSERT\s+INTO\s+`([^`]+)`'
    output_matches = re.findall(output_pattern, yql_query, re.IGNORECASE)
    
    # Return input tables (not output tables)
    input_tables = [match for match in matches if match not in output_matches]
    
    return input_tables


def extract_output_table(yql_query: str) -> Optional[str]:
    """
    Extract output table path from YQL query.
    
    Args:
        yql_query: YQL query string
    
    Returns:
        Output table path or None
    """
    pattern = r'INSERT\s+INTO\s+`([^`]+)`'
    match = re.search(pattern, yql_query, re.IGNORECASE)
    return match.group(1) if match else None
