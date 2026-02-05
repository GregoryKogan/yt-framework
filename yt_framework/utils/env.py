"""
Environment Variable Utilities
===============================

Simple utilities for loading environment variables from .env files.
"""

import warnings
from pathlib import Path
from typing import Dict


def load_env_file(env_path: Path) -> Dict[str, str]:
    """
    Load environment variables from a .env file.

    File format: KEY=VALUE (one per line, # for comments)
    Missing file is optional and returns empty dict.

    Args:
        env_path: Path to the .env file

    Returns:
        Dictionary of loaded environment variables (key -> value).
        Returns empty dict if file doesn't exist or cannot be read.

    Raises:
        UserWarning: If file doesn't exist or cannot be parsed (non-fatal).

    Example:
        >>> env_vars = load_env_file(Path("configs/secrets.env"))
        >>> print(env_vars.get("YT_TOKEN"))
    """
    env_vars = {}

    # Skip if file doesn't exist (optional file)
    if not env_path.exists():
        warnings.warn(
            f"Secrets file not found: {env_path} (optional, continuing without it)",
            UserWarning,
        )
        return env_vars

    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    env_vars[key] = value
    except Exception as e:
        warnings.warn(f"Could not load {env_path}: {e}", UserWarning)

    return env_vars


def load_secrets(secrets_dir: Path, env_file: str = "secrets.env") -> Dict[str, str]:
    """
    Load secrets from secrets.env file in the specified directory.

    Args:
        secrets_dir: Directory containing the secrets.env file
        env_file: Name of the environment file (default: "secrets.env")

    Returns:
        Dictionary of loaded secrets (key -> value).
        Returns empty dict if file doesn't exist or cannot be read.

    Raises:
        UserWarning: If file doesn't exist or cannot be parsed (non-fatal).

    Example:
        >>> secrets = load_secrets(Path("configs"))
        >>> yt_token = secrets.get("YT_TOKEN")
    """
    secrets_path = secrets_dir / env_file
    return load_env_file(secrets_path)
