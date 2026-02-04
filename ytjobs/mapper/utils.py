"""
Mapper Utilities
================

Common utilities for YT mapper scripts.
Includes Row class and input reading helpers.
"""

import sys
import json
from typing import Iterable, Callable, Iterator, Any, Optional, Dict


def read_input_rows() -> Iterable[object]:
    """
    Read and parse input rows from stdin.

    Reads JSON lines from stdin, parses them, and creates Row objects.
    Skips empty lines and logs parsing errors to stderr.

    Returns:
        Iterable of objects

    Example:
        for row in read_input_rows():
            print(row.bucket, row.path)
    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            row_data = json.loads(line)
            yield row_data
        except Exception as e:
            error_msg = {"error": f"Failed to parse row: {str(e)}", "row": line}
            print(json.dumps(error_msg), file=sys.stderr)


def parse_json_line(line: str) -> Optional[Any]:
    """
    Parse a JSON line and log errors to stderr if parsing fails.

    Args:
        line: JSON string to parse

    Returns:
        Parsed JSON object, or None if parsing failed
    """
    try:
        return json.loads(line)
    except json.JSONDecodeError as e:
        log_error({"error": f"Failed to parse row: {str(e)}", "row": line})
        return None


def log_error(error_dict: Dict[str, Any]) -> None:
    """
    Log an error message as JSON to stderr.

    Args:
        error_dict: Dictionary containing error information
    """
    print(json.dumps(error_dict), file=sys.stderr)
    sys.stderr.flush()


def process_and_write_results(
    processing_func: Callable[..., Iterator[Any]],
    data: Any,
    redirect_output: bool = True,
    **kwargs: Any,
) -> None:
    """
    Execute a processing function and write results as they're yielded.

    Streams results without loading them all into memory, which is critical
    for jobs processing millions of rows.

    Note: We manually manage stdout here rather than using redirect_stdout_to_stderr()
    context manager because we need to toggle stdout for each result:
    - Redirect stdout→stderr during processing (iterator yields)
    - Restore stdout to write each result as JSON
    - Re-redirect for next iteration

    This toggling pattern can't be achieved with a single context manager.

    Args:
        processing_func: Function that returns an Iterator of results
        data: Data to pass to processing_func
        redirect_output: If True, redirect stdout to stderr during processing,
                         then restore it for writing each result
        **kwargs: Additional keyword arguments to pass to processing_func
    """
    if redirect_output:
        # Save original stdout (same pattern as redirect_stdout_to_stderr)
        original_stdout = sys.stdout

        # Redirect stdout to stderr for processing
        sys.stdout = sys.stderr

        try:
            # Iterate and process - any processing output goes to stderr
            for result in processing_func(data, **kwargs):
                # Temporarily restore stdout to write result
                sys.stdout = original_stdout
                print(json.dumps(result))
                sys.stdout.flush()
                # Re-redirect to stderr for next iteration
                sys.stdout = sys.stderr
        finally:
            # Always restore stdout (matches redirect_stdout_to_stderr cleanup)
            sys.stdout = original_stdout
    else:
        # No redirection - stream results directly
        for result in processing_func(data, **kwargs):
            print(json.dumps(result))
            sys.stdout.flush()
