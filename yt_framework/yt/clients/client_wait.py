"""Wait for YT operations and log stderr from failures."""

# pyright: reportUninitializedInstanceVariable=false
# ``logger`` is supplied by concrete clients (``BaseYTClient`` subclasses).

from __future__ import annotations

import logging

from yt.wrapper import Operation

from .stderr_parse import stderr_text_from_yt_attrs


class ClientOperationWaitMixin:
    """Mixin adding ``wait_for_operation`` and stderr logging."""

    logger: logging.Logger

    def wait_for_operation(self, operation: Operation) -> bool:
        """Wait for operation to complete.

        Args:
            operation: Operation to wait for

        Returns:
            True if successful, False otherwise

        """
        self.logger.info("Waiting for operation to complete...")

        try:
            operation.wait()
            state = operation.get_state()
        except Exception as e:
            self.logger.exception("Operation failed")
            self._log_error_from_exception(e)
            return False
        else:
            if state == "completed":
                self.logger.info("Operation completed successfully")
                return True
            self.logger.error("Operation %s", state)
            self._log_operation_error(operation)
            return False

    def _log_operation_error(self, operation: Operation) -> None:
        """Log operation error details."""
        try:
            error = operation.get_error()
            if error:
                self.logger.error("Error: %s", error)
        except (AttributeError, KeyError, TypeError) as exc:
            self.logger.debug("Failed to read operation error details: %s", exc)

    def _stderr_text_from_yt_exception(self, exception: Exception) -> str | None:
        return stderr_text_from_yt_attrs(exception)

    def _log_stderr_lines_from_attributes(self, exception: Exception) -> None:
        stderr = self._stderr_text_from_yt_exception(exception)
        if not stderr:
            return
        self.logger.error("Job stderr:")
        for line in stderr.replace("\\n", "\n").split("\n"):
            if line.strip():
                self.logger.error("  %s", line)

    def _log_error_from_exception(self, exception: Exception) -> None:
        """Extract and log error from exception."""
        try:
            self._log_stderr_lines_from_attributes(exception)
        except Exception:
            self.logger.exception("Error: %s", exception)
