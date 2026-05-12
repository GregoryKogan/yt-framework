"""Reusable mapper base classes for stdin/stdout JSONL processing.

Handles line iteration and error logging so stages implement only row logic.
"""

import sys
from collections.abc import Callable, Iterator
from typing import Any

from .utils import log_error, parse_json_line, process_and_write_results


def _json_row_from_stdin_line(raw_line: str) -> object | None:
    line = raw_line.strip()
    if not line:
        return None
    return parse_json_line(line)


def _iter_nonempty_json_rows_from_stdin() -> Iterator[object]:
    for raw_line in sys.stdin:
        row_data = _json_row_from_stdin_line(raw_line)
        if row_data is not None:
            yield row_data


class StreamMapper:
    """Mapper that processes stdin one line at a time.

    Reads JSON lines from stdin, processes each individually,
    and writes results to stdout. Define ``processing_func(row)`` that yields
    result dicts, then call ``StreamMapper().map(processing_func)``.
    """

    def map(
        self,
        processing_func: Callable[[Any], Iterator[Any]],
        *,
        redirect_processing_output: bool = True,
        **kwargs: object,
    ) -> None:
        """Read stdin line-by-line, process each, write results to stdout.

        Args:
            processing_func: Function that takes a row dict and returns Iterator of results
            redirect_processing_output: If True, redirect stdout to stderr during processing
            **kwargs: Additional keyword arguments to pass to processing_func

        """
        for raw_line in sys.stdin:
            self._process_stdin_line(
                raw_line,
                processing_func,
                redirect_processing_output=redirect_processing_output,
                **kwargs,
            )

    def _process_stdin_line(
        self,
        raw_line: str,
        processing_func: Callable[[Any], Iterator[Any]],
        *,
        redirect_processing_output: bool,
        **kwargs: object,
    ) -> None:
        row_data = _json_row_from_stdin_line(raw_line)
        if row_data is None:
            return
        try:
            process_and_write_results(
                processing_func,
                row_data,
                redirect_output=redirect_processing_output,
                **kwargs,
            )
        except Exception as e:
            log_error({"error": f"Processing failed: {e!s}", "row": raw_line.strip()})
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
        *,
        redirect_processing_output: bool = True,
        **kwargs: object,
    ) -> None:
        """Read stdin in batches, process each batch, write results to stdout.

        Args:
            processing_func: Function that takes a list of rows (and optional kwargs) and returns Iterator of results
            redirect_processing_output: If True, redirect stdout to stderr during processing
            **kwargs: Additional keyword arguments to pass to processing_func

        """
        if self.batch_size is None:
            self._process_all_rows(
                processing_func,
                redirect_processing_output=redirect_processing_output,
                **kwargs,
            )
        else:
            self._process_in_batches(
                processing_func,
                redirect_processing_output=redirect_processing_output,
                **kwargs,
            )

    def _process_all_rows(
        self,
        processing_func: Callable[..., Iterator[Any]],
        *,
        redirect_processing_output: bool,
        **kwargs: object,
    ) -> None:
        """Process all rows from stdin at once."""
        rows = self._read_all_rows()

        if rows:
            try:
                process_and_write_results(
                    processing_func,
                    rows,
                    redirect_output=redirect_processing_output,
                    **kwargs,
                )
            except Exception as e:
                log_error(
                    {
                        "error": f"Batch processing failed: {e!s}",
                        "total_rows": len(rows),
                    },
                )
                raise

    def _flush_batch_if_at_capacity(
        self,
        batch: list[Any],
        batch_count: int,
        batch_size: int,
        processing_func: Callable[..., Iterator[Any]],
        *,
        redirect_processing_output: bool,
        **kwargs: object,
    ) -> tuple[list[Any], int]:
        if len(batch) < batch_size:
            return batch, batch_count
        self._process_batch(
            batch,
            batch_count,
            processing_func,
            redirect_processing_output=redirect_processing_output,
            **kwargs,
        )
        return [], batch_count + 1

    def _process_in_batches(
        self,
        processing_func: Callable[..., Iterator[Any]],
        *,
        redirect_processing_output: bool,
        **kwargs: object,
    ) -> None:
        """Process rows from stdin in batches."""
        bs = self.batch_size
        if bs is None:
            msg = "Batch size must be set"
            raise ValueError(msg)

        batch: list[Any] = []
        batch_count = 0

        for row_data in _iter_nonempty_json_rows_from_stdin():
            batch.append(row_data)
            batch, batch_count = self._flush_batch_if_at_capacity(
                batch,
                batch_count,
                bs,
                processing_func,
                redirect_processing_output=redirect_processing_output,
                **kwargs,
            )

        if batch:
            self._process_batch(
                batch,
                batch_count,
                processing_func,
                redirect_processing_output=redirect_processing_output,
                **kwargs,
            )

    def _read_all_rows(self) -> list[Any]:
        """Read all rows from stdin."""
        rows = []
        for raw_line in sys.stdin:
            row_data = _json_row_from_stdin_line(raw_line)
            if row_data is not None:
                rows.append(row_data)

        return rows

    def _process_batch(
        self,
        batch: list[Any],
        batch_number: int,
        processing_func: Callable[..., Iterator[Any]],
        *,
        redirect_processing_output: bool,
        **kwargs: object,
    ) -> None:
        """Process a single batch."""
        try:
            process_and_write_results(
                processing_func,
                batch,
                redirect_output=redirect_processing_output,
                **kwargs,
            )
        except Exception as e:
            log_error(
                {
                    "error": f"Batch {batch_number} processing failed: {e!s}",
                    "batch_size": len(batch),
                    "batch_number": batch_number,
                },
            )
            raise
