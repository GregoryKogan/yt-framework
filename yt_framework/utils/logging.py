"""
Logging Configuration Module
=============================

Centralized logging setup for the entire pipeline.
"""

import logging
import sys
from typing import Optional


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels."""

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    # Emoji indicators (only for non-INFO levels to reduce clutter)
    ICONS = {
        "DEBUG": "🔍",
        "WARNING": "⚠️",
        "ERROR": "✗",
        "CRITICAL": "🚨",
    }

    def format(self, record):
        """Format log record with colors and icons."""
        # Add color
        levelname = record.levelname
        if levelname in self.COLORS:
            icon = self.ICONS.get(levelname, "")
            icon_space = f"{icon} " if icon else ""
            record.levelname = (
                f"{self.COLORS[levelname]}"
                f"{icon_space}"
                f"{levelname}"
                f"{self.RESET}"
            )

        return super().format(record)


def setup_logging(
    level: int = logging.INFO, name: Optional[str] = None, use_colors: bool = True
) -> logging.Logger:
    """
    Setup logging with consistent formatting.

    Args:
        level: Logging level (default: INFO)
        name: Logger name (default: root logger)
        use_colors: Whether to use colored output

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # If this is a child logger (name is provided), disable propagation
    # to prevent duplicate messages from propagating to root logger
    if name is not None:
        logger.propagate = False

    # Remove existing handlers
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Formatter with timestamp
    if use_colors and sys.stdout.isatty():
        formatter = ColoredFormatter(
            "%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
    else:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def log_header(
    logger: logging.Logger, title: str, context: Optional[str] = None
) -> None:
    """
    Log a compact section header in format: [Title] context.

    Args:
        logger: Logger instance
        title: Section title (will be wrapped in brackets)
        context: Optional additional context information
    """
    if context:
        logger.info(f"[{title}] {context}")
    else:
        logger.info(f"[{title}]")


def log_operation(logger: logging.Logger, message: str) -> None:
    """
    Log an operation start message with → prefix.

    Args:
        logger: Logger instance
        message: Operation description
    """
    logger.info(f"  → {message}")


def log_success(logger: logging.Logger, message: str) -> None:
    """
    Log a success/completion message with ✓ prefix.

    Args:
        logger: Logger instance
        message: Success message
    """
    logger.info(f"  ✓ {message}")


def log_config(
    logger: logging.Logger, config_dict: dict, title: str = "Configuration"
) -> None:
    """
    Log configuration in a readable format.

    Args:
        logger: Logger instance
        config_dict: Configuration dictionary
        title: Title for the configuration section
    """
    log_header(logger, title)
    for key, value in config_dict.items():
        # Mask sensitive data
        if "secret" in key.lower() or "key" in key.lower():
            value = "***" + str(value)[-4:] if value else "(not set)"
        logger.info(f"    {key}: {value}")
