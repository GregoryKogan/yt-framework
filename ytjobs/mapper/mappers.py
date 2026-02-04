"""
Mapper Base Classes
===================

Reusable mapper classes that handle stdin/stdout boilerplate.
Allows users to focus on processing logic.
"""

import sys
from typing import Callable, Iterator, Optional, List, Any

from .utils import parse_json_line, log_error, process_and_write_results


class StreamMapper:
    """
    Mapper that processes stdin one line at a time.

    Reads JSON lines from stdin, processes each individually,
    and writes results to stdout.

    Example:
        def process_row(row):
            # Process single row
            result = {"output": row["input"] * 2}
            yield result

        mapper = StreamMapper()
        mapper.map(process_row)
    """

    def map(
        self,
        processing_func: Callable[[Any], Iterator[Any]],
        redirect_processing_output: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Read stdin line-by-line, process each, write results to stdout.

        Args:
            processing_func: Function that takes a row dict and returns Iterator of results
            redirect_processing_output: If True, redirect stdout to stderr during processing
            **kwargs: Additional keyword arguments to pass to processing_func
        """
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            # Parse input row
            row_data = parse_json_line(line)
            if row_data is None:
                continue

            # Process row and write results as they're yielded
            try:
                process_and_write_results(
                    processing_func, row_data, redirect_processing_output, **kwargs
                )
            except Exception as e:
                log_error({"error": f"Processing failed: {str(e)}", "row": line})


class BatchMapper:
    """
    Mapper that processes stdin in batches.

    Reads JSON lines from stdin in batches, processes each batch,
    and writes results to stdout.

    Example:
        def process_batch(rows):
            # Process batch of rows
            for row in rows:
                result = {"output": row["input"] * 2}
                yield result

        mapper = BatchMapper(batch_size=100)  # or None for all rows
        mapper.map(process_batch)
    """

    def __init__(self, batch_size: Optional[int] = None):
        """
        Initialize batch mapper.

        Args:
            batch_size: Number of rows per batch, or None to process all rows at once
        """
        self.batch_size = batch_size

    def map(
        self,
        processing_func: Callable[..., Iterator[Any]],
        redirect_processing_output: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Read stdin in batches, process each batch, write results to stdout.

        Args:
            processing_func: Function that takes a list of rows (and optional kwargs) and returns Iterator of results
            redirect_processing_output: If True, redirect stdout to stderr during processing
            **kwargs: Additional keyword arguments to pass to processing_func
        """
        if self.batch_size is None:
            self._process_all_rows(
                processing_func, redirect_processing_output, **kwargs
            )
        else:
            self._process_in_batches(
                processing_func, redirect_processing_output, **kwargs
            )

    def _process_all_rows(
        self,
        processing_func: Callable[..., Iterator[Any]],
        redirect_processing_output: bool,
        **kwargs: Any,
    ) -> None:
        """Process all rows from stdin at once."""
        rows = self._read_all_rows()

        if rows:
            try:
                process_and_write_results(
                    processing_func, rows, redirect_processing_output, **kwargs
                )
            except Exception as e:
                log_error(
                    {
                        "error": f"Batch processing failed: {str(e)}",
                        "total_rows": len(rows),
                    }
                )

    def _process_in_batches(
        self,
        processing_func: Callable[..., Iterator[Any]],
        redirect_processing_output: bool,
        **kwargs: Any,
    ) -> None:
        """Process rows from stdin in batches."""
        if self.batch_size is None:
            raise ValueError("Batch size must be set")

        batch = []
        batch_count = 0

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            row_data = parse_json_line(line)
            if row_data is None:
                continue

            batch.append(row_data)

            # Process batch when it reaches batch_size
            if len(batch) >= self.batch_size:
                self._process_batch(
                    batch,
                    batch_count,
                    processing_func,
                    redirect_processing_output,
                    **kwargs,
                )
                batch = []
                batch_count += 1

        # Process remaining rows
        if batch:
            self._process_batch(
                batch,
                batch_count,
                processing_func,
                redirect_processing_output,
                **kwargs,
            )

    def _read_all_rows(self) -> List[Any]:
        """Read all rows from stdin."""
        rows = []
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            row_data = parse_json_line(line)
            if row_data is not None:
                rows.append(row_data)

        return rows

    def _process_batch(
        self,
        batch: List[Any],
        batch_number: int,
        processing_func: Callable[..., Iterator[Any]],
        redirect_processing_output: bool,
        **kwargs: Any,
    ) -> None:
        """Process a single batch."""
        try:
            process_and_write_results(
                processing_func, batch, redirect_processing_output, **kwargs
            )
        except Exception as e:
            log_error(
                {
                    "error": f"Batch {batch_number} processing failed: {str(e)}",
                    "batch_size": len(batch),
                    "batch_number": batch_number,
                }
            )
