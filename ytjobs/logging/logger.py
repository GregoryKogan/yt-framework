"""
Text Logger for ptrack Pipeline
=================================

Provides structured human-readable logging to stderr for the ptrack pipeline.
All logs are formatted as readable text, written to stderr.
"""

import logging
import sys
from datetime import datetime
from typing import Any, Optional


class TextFormatter(logging.Formatter):
    """Formatter that outputs logs as human-readable text."""

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as human-readable text.

        Args:
            record: Log record to format

        Returns:
            Formatted log string
        """
        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")

        # Format level
        level = record.levelname

        # Get message
        message = record.getMessage()

        # Collect context fields
        excluded_fields = {
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
        }

        context_parts = []
        for key, value in record.__dict__.items():
            if key not in excluded_fields and not key.startswith("_"):
                # Format value readably
                formatted_value = self._format_value(value)
                context_parts.append(f"{key}={formatted_value}")

        # Build log line
        log_line = f"[{timestamp}] {level}: {message}"
        if hasattr(record, "name"):
            log_line = f"[{timestamp}] [{record.name}] {level}: {message}"

        if context_parts:
            log_line += " | " + " ".join(context_parts)

        # Add exception info if present
        if record.exc_info:
            log_line += "\n" + self.formatException(record.exc_info)

        return log_line

    def _format_value(self, value: Any) -> str:
        """Format a value for readable output."""
        if value is None:
            return "None"
        elif isinstance(value, (list, tuple)):
            return "[" + ", ".join(str(self._format_value(v)) for v in value) + "]"
        elif isinstance(value, dict):
            items = ", ".join(f"{k}={self._format_value(v)}" for k, v in value.items())
            return "{" + items + "}"
        elif isinstance(value, str):
            # Truncate very long strings
            if len(value) > 100:
                return value[:97] + "..."
            return value
        else:
            return str(value)


def get_logger(name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """
    Get a logger configured for human-readable text output to stderr.

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
    logger: logging.Logger, level: int, message: str, **kwargs: Any
) -> None:
    """
    Log a message with extra context fields.

    Args:
        logger: Logger instance
        level: Log level (e.g., logging.INFO)
        message: Log message
        **kwargs: Additional context fields to include in log
    """
    # Use the standard logging mechanism with extra parameter
    # This adds the kwargs to the record's __dict__
    logger.log(level, message, extra=kwargs)
