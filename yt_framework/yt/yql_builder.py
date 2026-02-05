"""
YQL Query Builder
=================

Utility functions for building YQL queries programmatically.
"""

from typing import List, Dict, Union, Optional, Tuple


def _escape_table_name(table: str) -> str:
    """Escape table name with backticks for YQL.
    
    Args:
        table: Table path (e.g., "//tmp/my_table").
        
    Returns:
        str: Escaped table name (e.g., "`//tmp/my_table`").
    """
    return f"`{table}`"


def _format_column_list(columns: List[str]) -> str:
    """Format column list for SELECT clause.
    
    Args:
        columns: List of column names or expressions (e.g., ["a.id", "b.name"]).
        
    Returns:
        str: Formatted column list with indentation (e.g., "a.id,\n    b.name").
    """
    return ",\n    ".join(columns)


def _format_join_conditions(
    on: Union[str, List[str], Dict[str, str]],
    left_alias: str = "a",
    right_alias: str = "b",
) -> str:
    """Format JOIN ON conditions.
    
    Args:
        on: Join key specification:
            - str: Single column name (same on both sides).
            - List[str]: Multiple column names (same on both sides).
            - Dict[str, str]: Different column names. Must use special keys "left" and "right"
              where "left" maps to the left table column name and "right" maps to the right
              table column name (e.g., {"left": "user_id", "right": "id"}).
        left_alias: Alias for left table (default: "a").
        right_alias: Alias for right table (default: "b").
        
    Returns:
        str: Formatted JOIN condition (e.g., "a.id = b.id" or "a.user_id = b.id AND a.region = b.region_code").
    """
    if isinstance(on, str):
        # Same column name on both sides
        return f"{left_alias}.{on} = {right_alias}.{on}"
    elif isinstance(on, dict):
        # Different column names: {"left": "user_id", "right": "id"}
        # or multiple pairs: {"left": ["user_id", "region"], "right": ["id", "region_code"]}
        if isinstance(on.get("left"), list):
            # Multiple column pairs
            left_cols = on["left"]
            right_cols = on["right"]
            conditions = [
                f"{left_alias}.{left_col} = {right_alias}.{right_col}"
                for left_col, right_col in zip(left_cols, right_cols)
            ]
            return " AND ".join(conditions)
        else:
            # Single column pair
            return f"{left_alias}.{on['left']} = {right_alias}.{on['right']}"
    else:
        # List of column names - same on both sides
        conditions = [f"{left_alias}.{col} = {right_alias}.{col}" for col in on]
        return " AND ".join(conditions)


def _format_group_by_list(group_by: Union[str, List[str]]) -> str:
    """Format GROUP BY column list.
    
    Args:
        group_by: Column name(s) to group by.
        
    Returns:
        str: Formatted GROUP BY clause (e.g., "region" or "region, status").
    """
    if isinstance(group_by, str):
        return group_by
    return ", ".join(group_by)


def _format_aggregations(
    aggregations: Dict[str, Union[str, Tuple[str, str]]],
    group_by: Union[str, List[str]],
) -> str:
    """Format aggregation expressions for SELECT clause.

    Args:
        aggregations: Dict mapping output column names to aggregation functions
                     - Simple format: {"total_amount": "sum"} -> SUM(amount) AS total_amount
                     - Explicit column format: {"total_amount": ("sum", "amount")} -> SUM(amount) AS total_amount
                     - Column with alias: {"total_amount": ("sum", "a.amount")} -> SUM(a.amount) AS total_amount
        group_by: Column(s) to group by
    """
    # Include group by columns
    group_cols = [group_by] if isinstance(group_by, str) else group_by
    select_parts = group_cols.copy()

    # Add aggregations
    for col, agg_spec in aggregations.items():
        # Handle tuple format: (function, column_name)
        if isinstance(agg_spec, tuple) and len(agg_spec) == 2:
            agg_func, column_ref = agg_spec
            agg_upper = str(agg_func).upper()
            if agg_upper == "COUNT":
                select_parts.append(f"COUNT(*) AS {col}")
            else:
                select_parts.append(f"{agg_upper}({column_ref}) AS {col}")
        else:
            # Handle string format: "sum" -> extract column name from output name
            agg_func = str(agg_spec)
            agg_upper = agg_func.upper()
            if agg_upper == "COUNT":
                select_parts.append(f"COUNT(*) AS {col}")
            else:
                # For sum, avg, min, max - assume column name is part of the key
                # User provides {"total_amount": "sum"} meaning SUM(amount) AS total_amount
                # We'll extract base column name by removing common prefixes
                base_col = col
                for prefix in ["total_", "avg_", "min_", "max_", "count_"]:
                    if col.startswith(prefix):
                        base_col = col[len(prefix) :]
                        break
                select_parts.append(f"{agg_upper}({base_col}) AS {col}")

    return ",\n    ".join(select_parts)


def _format_order_by_list(
    order_by: Union[str, List[str]], ascending: bool = True
) -> str:
    """Format ORDER BY column list.
    
    Args:
        order_by: Column name(s) to sort by.
        ascending: Sort direction (True for ASC, False for DESC). Applies to all columns.
        
    Returns:
        str: Formatted ORDER BY clause (e.g., "id ASC" or "id ASC, name ASC").
             All columns use the same sort direction (mixed directions not supported).
    """
    direction = "ASC" if ascending else "DESC"
    if isinstance(order_by, str):
        return f"{order_by} {direction}"
    return ", ".join([f"{col} {direction}" for col in order_by])


def build_join_query(
    left_table: str,
    right_table: str,
    output_table: str,
    on: Union[str, List[str], Dict[str, str]],
    how: str = "left",
    select_columns: Optional[List[str]] = None,
) -> str:
    """
    Build a YQL JOIN query.

    Args:
        left_table: Left table path
        right_table: Right table path
        output_table: Output table path
        on: Join key(s) - column name(s) to join on
            - str: Same column name on both sides (e.g., "user_id")
            - List[str]: Multiple columns with same names (e.g., ["user_id", "region"])
            - Dict[str, str]: Different column names (e.g., {"left": "input_s3_path", "right": "path"})
        how: Join type - "inner", "left", "right", or "full"
        select_columns: Optional list of columns to select (with table aliases)
                       e.g., ["a.col1", "a.col2", "b.col3"]

    Returns:
        YQL query string
    """
    join_type = how.upper()
    if join_type == "FULL":
        join_type = "FULL OUTER"

    # Determine if we can use USING clause (same column names on both sides)
    # When select_columns is provided, USING works well
    # When select_columns is NOT provided, USING + SELECT * causes _other conflicts
    # So we use ON clause with a.*, b.* when select_columns is not provided
    use_using = False
    using_columns = None
    join_conditions = None

    if isinstance(on, str):
        # Single column with same name on both sides
        if select_columns:
            # When select_columns is provided, use USING
            use_using = True
            using_columns = [on]
        else:
            # When select_columns is not provided, use ON clause to avoid _other conflicts
            use_using = False
            join_conditions = _format_join_conditions(
                on, left_alias="a", right_alias="b"
            )
    elif isinstance(on, list):
        # Multiple columns with same names
        if select_columns:
            # When select_columns is provided, use USING
            use_using = True
            using_columns = on
        else:
            # When select_columns is not provided, use ON clause to avoid _other conflicts
            use_using = False
            join_conditions = _format_join_conditions(
                on, left_alias="a", right_alias="b"
            )
    elif isinstance(on, dict):
        # Different column names - must use ON clause
        use_using = False
        join_conditions = _format_join_conditions(on, left_alias="a", right_alias="b")
    else:
        # Fallback: use ON clause
        use_using = False
        join_conditions = _format_join_conditions(on, left_alias="a", right_alias="b")

    if select_columns:
        select_clause = _format_column_list(select_columns)
    else:
        # When select_columns is not provided, use a.*, b.* with ON clause
        # This avoids the _other conflict that occurs with USING + SELECT *
        select_clause = "a.*, b.*"

    if use_using:
        # Format USING clause
        assert (
            using_columns is not None
        ), "using_columns must be set when use_using is True"
        if len(using_columns) == 1:
            using_clause = f"USING ({using_columns[0]})"
        else:
            using_clause = f"USING ({', '.join(using_columns)})"

        query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT
    {select_clause}
FROM {_escape_table_name(left_table)} AS a
{join_type} JOIN {_escape_table_name(right_table)} AS b
{using_clause};"""
    else:
        assert (
            join_conditions is not None
        ), "join_conditions must be set when use_using is False"
        query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT
    {select_clause}
FROM {_escape_table_name(left_table)} AS a
{join_type} JOIN {_escape_table_name(right_table)} AS b
ON {join_conditions};"""

    return query


def build_filter_query(
    input_table: str,
    output_table: str,
    condition: str,
    columns: List[str],
) -> str:
    """
    Build a YQL filter query with WHERE clause.

    Args:
        input_table: Input table path
        output_table: Output table path
        condition: WHERE condition (e.g., "status = 'active' AND total > 100")
        columns: List of columns to select (required to avoid _other column issues)

    Returns:
        YQL query string
    """
    select_clause = _format_column_list(columns)

    query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT
    {select_clause}
FROM {_escape_table_name(input_table)}
WHERE {condition};"""

    return query


def build_select_query(
    input_table: str,
    output_table: str,
    columns: List[str],
) -> str:
    """
    Build a YQL query to select specific columns.

    Args:
        input_table: Input table path
        output_table: Output table path
        columns: List of column names to select

    Returns:
        YQL query string
    """
    select_clause = _format_column_list(columns)

    query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT
    {select_clause}
FROM {_escape_table_name(input_table)};"""

    return query


def build_group_by_query(
    input_table: str,
    output_table: str,
    group_by: Union[str, List[str]],
    aggregations: Dict[str, Union[str, Tuple[str, str]]],
) -> str:
    """
    Build a YQL GROUP BY query with aggregations.

    Args:
        input_table: Input table path
        output_table: Output table path
        group_by: Column(s) to group by (empty list means aggregate all rows)
        aggregations: Dict mapping output column names to aggregation functions
                     - Simple format: {"order_count": "count", "total_amount": "sum"}
                     - Explicit column: {"total_amount": ("sum", "amount")} or {"total_amount": ("sum", "a.amount")}

    Returns:
        YQL query string
    """
    select_clause = _format_aggregations(aggregations, group_by)
    group_clause = _format_group_by_list(group_by)

    # If group_by is empty, omit GROUP BY clause (aggregate all rows)
    if isinstance(group_by, list) and len(group_by) == 0:
        query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT
    {select_clause}
FROM {_escape_table_name(input_table)};"""
    else:
        query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT
    {select_clause}
FROM {_escape_table_name(input_table)}
GROUP BY {group_clause};"""

    return query


def build_union_query(
    tables: List[str],
    output_table: str,
    columns: List[str],
) -> str:
    """
    Build a YQL UNION ALL query.

    Args:
        tables: List of table paths to union
        output_table: Output table path
        columns: List of columns to select (required to avoid _other column issues)

    Returns:
        YQL query string
    """
    if len(tables) < 2:
        raise ValueError("UNION requires at least 2 tables")

    select_clause = _format_column_list(columns)
    union_parts = [
        f"SELECT\n    {select_clause}\nFROM {_escape_table_name(table)}"
        for table in tables
    ]
    union_clause = "\nUNION ALL\n".join(union_parts)

    query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
{union_clause};"""

    return query


def build_distinct_query(
    input_table: str,
    output_table: str,
    columns: Optional[List[str]] = None,
) -> str:
    """
    Build a YQL DISTINCT query.

    Args:
        input_table: Input table path
        output_table: Output table path
        columns: Optional list of columns to select (if None, selects all)

    Returns:
        YQL query string
    """
    if columns:
        select_clause = _format_column_list(columns)
    else:
        select_clause = "*"

    query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT DISTINCT
    {select_clause}
FROM {_escape_table_name(input_table)};"""

    return query


def build_sort_query(
    input_table: str,
    output_table: str,
    order_by: Union[str, List[str]],
    columns: List[str],
    ascending: bool = True,
) -> str:
    """
    Build a YQL ORDER BY query.

    Uses a subquery pattern to prevent YQL from adding internal binary columns
    like _yql_column_0 that can appear with ORDER BY operations.

    Args:
        input_table: Input table path
        output_table: Output table path
        order_by: Column(s) to sort by
        columns: List of columns to select (required to avoid _other column issues)
        ascending: Sort direction (True for ASC, False for DESC)

    Returns:
        YQL query string
    """
    order_clause = _format_order_by_list(order_by, ascending)
    select_clause = _format_column_list(columns)

    # Use subquery pattern to prevent YQL from adding internal binary columns
    # Inner query does the ORDER BY, outer query selects only desired columns
    query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT
    {select_clause}
FROM (
    SELECT *
    FROM {_escape_table_name(input_table)}
    ORDER BY {order_clause}
);"""

    return query


def build_limit_query(
    input_table: str,
    output_table: str,
    limit: int,
    columns: List[str],
) -> str:
    """
    Build a YQL LIMIT query.

    Args:
        input_table: Input table path
        output_table: Output table path
        limit: Maximum number of rows to return
        columns: List of columns to select (required to avoid _other column issues)

    Returns:
        YQL query string
    """
    select_clause = _format_column_list(columns)

    query = f"""PRAGMA yt.InferSchema = '1';
INSERT INTO {_escape_table_name(output_table)} WITH TRUNCATE
SELECT
    {select_clause}
FROM {_escape_table_name(input_table)}
LIMIT {limit};"""

    return query
