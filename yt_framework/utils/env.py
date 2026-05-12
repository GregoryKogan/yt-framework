"""Load `KEY=value` files such as `configs/secrets.env` into plain dicts."""

import warnings
from pathlib import Path


def _parse_env_kv_line(line: str) -> tuple[str, str] | None:
    if not line or line.startswith("#") or "=" not in line:
        return None
    key, value = line.split("=", 1)
    return key.strip(), value.strip()


def load_env_file(env_path: Path) -> dict[str, str]:
    """Load environment variables from a .env file.

    File format: KEY=VALUE (one per line, # for comments)
    Missing file is optional and returns empty dict.

    Args:
        env_path: Path to the .env file

    Returns:
        Dictionary of loaded environment variables (key -> value).
        Returns empty dict if file doesn't exist or cannot be read.

    Warns:
        UserWarning: If the file exists but cannot be read or parsed (non-fatal).
        Missing file is silent (returns empty dict).

    Example:
        >>> env_vars = load_env_file(Path("configs/secrets.env"))
        >>> print(env_vars.get("YT_TOKEN"))

    """
    env_vars = {}

    # Skip if file doesn't exist (optional file — no warning; callers treat as empty)
    if not env_path.exists():
        return env_vars

    try:
        with env_path.open() as f:
            for raw_line in f:
                kv = _parse_env_kv_line(raw_line.strip())
                if kv:
                    env_vars[kv[0]] = kv[1]
    except OSError as e:
        warnings.warn(f"Could not load {env_path}: {e}", UserWarning, stacklevel=2)

    return env_vars


def load_secrets(secrets_dir: Path, env_file: str = "secrets.env") -> dict[str, str]:
    """Load secrets from secrets.env file in the specified directory.

    Args:
        secrets_dir: Directory containing the secrets.env file
        env_file: Name of the environment file (default: "secrets.env")

    Returns:
        Dictionary of loaded secrets (key -> value).
        Returns empty dict if file doesn't exist or cannot be read.

    Warns:
        UserWarning: If the secrets file exists but cannot be read or parsed (non-fatal).

    Example:
        >>> secrets = load_secrets(Path("configs"))
        >>> yt_token = secrets.get("YT_TOKEN")

    """
    secrets_path = secrets_dir / env_file
    return load_env_file(secrets_path)
