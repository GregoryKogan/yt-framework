"""Shell wrappers and command-mode script resolution for code upload."""

# pyright: reportUnusedFunction=false
# Helpers are imported by ``yt_framework.operations.upload``; not all are
# referenced within this compilation unit.

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import yaml
from omegaconf import OmegaConf
from omegaconf.errors import OmegaConfBaseException


def _bash_wrapper_script_body(
    stage_name: str,
    python_script_relative: str,
    label: str,
) -> str:
    """Shared body for operation wrappers (map, vanilla, map-reduce legs, reduce)."""
    requirements_path = f"stages/{stage_name}/requirements.txt"
    return f"""
#!/bin/bash
set -e

# Get current directory (sandbox root)
SANDBOX_ROOT="$(pwd)"

# Set PYTHONPATH to current directory so ytjobs and stages packages can be imported
export PYTHONPATH="${{PYTHONPATH}}:${{SANDBOX_ROOT}}"

# Set config path for ytjobs config loader
# Config file is extracted to stages/{stage_name}/config.yaml
export JOB_CONFIG_PATH="${{SANDBOX_ROOT}}/stages/{stage_name}/config.yaml"

# Install requirements.txt if it exists
if [ -f "{requirements_path}" ]; then
    echo "Installing dependencies from requirements.txt..." >&2
    pip install --quiet --no-cache-dir -r {requirements_path} || echo "Warning: Failed to install some dependencies" >&2
fi

# Execute: {label}
python3 {python_script_relative}
"""


def _write_wrapper_file(
    build_dir: Path,
    filename: str,
    body: str,
    logger: logging.Logger,
) -> None:
    wrapper_path = build_dir / filename
    wrapper_path.write_text(body)
    wrapper_path.chmod(0o755)
    logger.debug("Created wrapper script: %s", wrapper_path)


def _load_stage_job_section(
    stage_dir: Path, logger: logging.Logger
) -> dict[str, object]:
    """Return ``job`` dict from stage ``config.yaml`` if present."""
    cfg_path = stage_dir / "config.yaml"
    if not cfg_path.is_file():
        return {}
    try:
        cfg = OmegaConf.to_container(OmegaConf.load(cfg_path), resolve=True)
        if not isinstance(cfg, dict):
            return {}
        job = cfg.get("job")
        return job if isinstance(job, dict) else {}
    except (
        OSError,
        OmegaConfBaseException,
        TypeError,
        ValueError,
        yaml.YAMLError,
    ) as e:
        logger.warning("Could not read %s for wrapper script paths: %s", cfg_path, e)
        return {}


_MAP_REDUCE_REDUCER_CANDIDATES = (
    "reducer.py",
    "reducer_mds.py",
    "reducer_main.py",
    "reducer_index.py",
)


def _first_existing_reducer_script(src: Path) -> str | None:
    for candidate in _MAP_REDUCE_REDUCER_CANDIDATES:
        if (src / candidate).is_file():
            return candidate
    return None


def _map_reduce_command_section(job: dict[str, object]) -> dict[str, object]:
    mrc = job.get("map_reduce_command") or {}
    return mrc if isinstance(mrc, dict) else {}


def _normalize_reducer_script_name(
    reducer: object | None,
    src: Path,
) -> str | None:
    if reducer is not None:
        return str(reducer)
    return _first_existing_reducer_script(src)


def _resolve_map_reduce_command_scripts(
    stage_dir: Path,
    logger: logging.Logger,
) -> tuple[str | None, str | None]:
    """Resolve mapper and reducer entrypoints for map-reduce command mode.

    Looks under ``stages/<name>/src/`` and uses ``job.map_reduce_command`` when set.
    """
    job = _load_stage_job_section(stage_dir, logger)
    mrc = _map_reduce_command_section(job)
    mapper = str(mrc.get("mapper_script") or "mapper.py")
    src = stage_dir / "src"
    reducer = _normalize_reducer_script_name(mrc.get("reducer_script"), src)
    if not (src / mapper).is_file():
        logger.debug("No %s at %s — skipping map_reduce_mapper wrapper", mapper, src)
        return None, None
    if not reducer or not (src / reducer).is_file():
        logger.debug("No reducer script resolved for map-reduce in %s", src)
        return mapper, None
    return mapper, reducer


_REDUCE_ONLY_SCRIPT_CANDIDATES = (
    "reducer.py",
    "reducer_index.py",
    "reducer_mds.py",
    "reducer_main.py",
)


def _reduce_command_section(job: dict[str, object]) -> dict[str, object]:
    rc = job.get("reduce_command") or {}
    return rc if isinstance(rc, dict) else {}


def _explicit_reduce_script_or_warn(
    rc: dict[str, object],
    src: Path,
    logger: logging.Logger,
) -> str | None:
    explicit = rc.get("reducer_script")
    if not explicit:
        return None
    name = str(explicit)
    if (src / name).is_file():
        return name
    logger.warning(
        "reduce_command.reducer_script %s not found under %s",
        name,
        src,
    )
    return None


def _first_reduce_only_candidate(src: Path) -> str | None:
    for candidate in _REDUCE_ONLY_SCRIPT_CANDIDATES:
        if (src / candidate).is_file():
            return candidate
    return None


def _resolve_reduce_command_script(
    stage_dir: Path,
    logger: logging.Logger,
) -> str | None:
    """Reducer entrypoint for reduce-only command mode (``job.reduce_command``)."""
    job = _load_stage_job_section(stage_dir, logger)
    rc = _reduce_command_section(job)
    src = stage_dir / "src"
    found = _explicit_reduce_script_or_warn(rc, src, logger)
    if found:
        return found
    return _first_reduce_only_candidate(src)


def _create_unified_wrapper_script(
    stage_name: str,
    operation_type: Literal["map", "vanilla"],
    build_dir: Path,
    logger: logging.Logger,
) -> None:
    """Create unified wrapper script for map or vanilla operations.

    The wrapper script:
    1. Extracts tar.gz archive
    2. Sets up PYTHONPATH to include current directory
    3. Sets JOB_CONFIG_PATH to stage config
    4. Installs requirements.txt if present
    5. Executes the appropriate script (mapper.py or vanilla.py)

    Args:
        stage_name: Name of the stage
        operation_type: Type of operation ('map' or 'vanilla')
        build_dir: Local build directory path
        logger: Logger instance

    Returns:
        None

    """
    if operation_type == "map":
        script_path = f"stages/{stage_name}/src/mapper.py"
    else:
        script_path = f"stages/{stage_name}/src/vanilla.py"

    body = _bash_wrapper_script_body(
        stage_name,
        script_path,
        f"{operation_type} operation",
    )
    _write_wrapper_file(
        build_dir,
        f"operation_wrapper_{stage_name}_{operation_type}.sh",
        body,
        logger,
    )
