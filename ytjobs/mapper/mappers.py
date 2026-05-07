"""Mapper Base Classes.
===================

Reusable mapper classes that handle stdin/stdout boilerplate.
Allows users to focus on processing logic.
"""

import sys
from collections.abc import Callable, Iterator
from typing import Any

from .utils import log_error, parse_json_line, process_and_write_results


class StreamMapper:
    """Mapper that processes stdin one line at a time.

    Reads JSON lines from stdin, processes each individually,
    and writes results to stdout. Define ``processing_func(row)`` that yields
    result dicts, then call ``StreamMapper().map(processing_func)``.
    """

    def map(
        self,
        processing_func: Callable[[Any], Iterator[Any]],
        redirect_processing_output: bool = True,
        **kwargs: Any,
    ) -> None:
        """Read stdin line-by-line, process each, write results to stdout.

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
                log_error({"error": f"Processing failed: {e!s}", "row": line})
                raise


class BatchMapper:
    """Mapper that processes stdin in batches.

    Reads JSON lines from stdin in batches, processes each batch,
    and writes results to stdout. Pass ``batch_size`` (or ``None`` to buffer all
    stdin), define ``processing_func(rows)`` that yields result dicts, then
    call ``mapper.map(processing_func)``.
    """

    def __init__(self, batch_size: int | None = None) -> None:
        """Initialize batch mapper.

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
        """Read stdin in batches, process each batch, write results to stdout.

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
                        "error": f"Batch processing failed: {e!s}",
                        "total_rows": len(rows),
                    }
                )
                raise

    def _process_in_batches(
        self,
        processing_func: Callable[..., Iterator[Any]],
        redirect_processing_output: bool,
        **kwargs: Any,
    ) -> None:
        """Process rows from stdin in batches."""
        if self.batch_size is None:
            msg = "Batch size must be set"
            raise ValueError(msg)

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

    def _read_all_rows(self) -> list[Any]:
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
        batch: list[Any],
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
                    "error": f"Batch {batch_number} processing failed: {e!s}",
                    "batch_size": len(batch),
                    "batch_number": batch_number,
                }
            )
            raise
