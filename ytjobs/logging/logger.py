"""Human-readable structured logging to stderr for job pipelines.

Formats log records as plain text on stderr.
"""

import datetime
import logging
import sys
from typing import ClassVar

_LOG_STRING_PREVIEW_MAX_LEN = 100
_LOG_STRING_ELLIPSIS_LEN = 3

_EXCLUDED_LOG_FIELDS: frozenset[str] = frozenset(
    {
        "name",
        "msg",
        "args",
        "created",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "thread",
        "threadName",
        "exc_info",
        "exc_text",
        "stack_info",
        "taskName",
    },
)


class TextFormatter(logging.Formatter):
    """Formatter that outputs logs as human-readable text."""

    _excluded: ClassVar[frozenset[str]] = _EXCLUDED_LOG_FIELDS

    def _timestamp_str(self, record: logging.LogRecord) -> str:
        return datetime.datetime.fromtimestamp(
            record.created,
            tz=datetime.UTC,
        ).strftime("%Y-%m-%d %H:%M:%S")

    def _context_fragments(self, record: logging.LogRecord) -> list[str]:
        parts: list[str] = []
        for key, value in record.__dict__.items():
            if key in self._excluded or key.startswith("_"):
                continue
            formatted_value = self._format_value(value)
            parts.append(f"{key}={formatted_value}")
        return parts

    def _base_message_line(
        self,
        record: logging.LogRecord,
        timestamp: str,
        level: str,
        message: str,
    ) -> str:
        if hasattr(record, "name"):
            return f"[{timestamp}] [{record.name}] {level}: {message}"
        return f"[{timestamp}] {level}: {message}"

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as human-readable text.

        Args:
            record: Log record to format

        Returns:
            Formatted log string

        """
        timestamp = self._timestamp_str(record)
        level = record.levelname
        message = record.getMessage()
        log_line = self._base_message_line(record, timestamp, level, message)
        context_parts = self._context_fragments(record)
        if context_parts:
            log_line += " | " + " ".join(context_parts)
        if record.exc_info:
            log_line += "\n" + self.formatException(record.exc_info)
        return log_line

    def _format_value(self, value: object) -> str:
        """Format a value for readable output."""
        if value is None:
            return "None"
        if isinstance(value, (list, tuple)):
            return "[" + ", ".join(str(self._format_value(v)) for v in value) + "]"
        if isinstance(value, dict):
            items = ", ".join(f"{k}={self._format_value(v)}" for k, v in value.items())
            return "{" + items + "}"
        if isinstance(value, str):
            if len(value) > _LOG_STRING_PREVIEW_MAX_LEN:
                tail = _LOG_STRING_PREVIEW_MAX_LEN - _LOG_STRING_ELLIPSIS_LEN
                return value[:tail] + "..."
            return value
        return str(value)


def get_logger(name: str | None = None, level: int = logging.INFO) -> logging.Logger:
    """Get a logger configured for human-readable text output to stderr.

    Args:
        name: Logger name (default: root logger)
        level: Logging level (default: INFO)

    Returns:
        Configured logger instance

    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create stderr handler
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(level)

    # Use text formatter
    formatter = TextFormatter()
    stderr_handler.setFormatter(formatter)

    logger.addHandler(stderr_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def log_with_extra(
    logger: logging.Logger,
    level: int,
    message: str,
    **kwargs: object,
) -> None:
    """Log a message with extra context fields.

    Args:
        logger: Logger instance
        level: Log level (e.g., logging.INFO)
        message: Log message
        **kwargs: Additional context fields to include in log

    """
    # Use the standard logging mechanism with extra parameter
    # This adds the kwargs to the record's __dict__
    logger.log(level, message, extra=kwargs)
