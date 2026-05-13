"""Build `source.tar.gz`, merge extra modules/paths, and push artifacts to Cypress."""

# pyright: reportUnusedFunction=false, reportUnusedImport=false

import importlib
import logging
import shutil
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from omegaconf import DictConfig, OmegaConf
from omegaconf.errors import OmegaConfBaseException

import ytjobs
from yt_framework.utils import log_success
from yt_framework.utils.ignore import YTIgnoreMatcher

# Marker for the implicit ytjobs framework package in target conflict checks
_IMPLICIT_YTJOBS_SOURCE = "implicit (framework)"

# Default build code directory name
_BUILD_CODE_DIR = ".build"


def _get_ytjobs_dir() -> Path:
    """Get ytjobs package directory dynamically."""
    return Path(ytjobs.__file__).parent


def _copy_ytjobs_to_build_dir(
    build_dir: Path,
    logger: logging.Logger,
) -> int:
    """Copy ytjobs package to local build directory.

    Respects .ytignore patterns if present in the ytjobs directory.

    Args:
        build_dir: Local build directory path
        logger: Logger instance

    Returns:
        Number of files copied

    """
    ytjobs_dir = _get_ytjobs_dir()
    target_dir = build_dir / "ytjobs"
    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Copying ytjobs package to %s...", target_dir)

    file_count, ignored_count = _copy_tree_with_ytignore(ytjobs_dir, target_dir, logger)

    log_success(logger, f"Copied {file_count} ytjobs files")
    if ignored_count > 0:
        logger.debug("  Ignored %s files (matched .ytignore patterns)", ignored_count)
    return file_count


def _resolve_upload_target(source: str, target: str | None, _pipeline_dir: Path) -> str:
    """Resolve target name for upload_paths entry.

    Args:
        source: Source path from config
        target: Optional target from config
        _pipeline_dir: Reserved for future path resolution (call sites pass pipeline root).

    Returns:
        Target name (from config or derived from source basename)

    """
    if target is not None and str(target).strip():
        return str(target).strip()
    return Path(source).name


def _copy_tree_with_ytignore(
    source_dir: Path,
    target_dir: Path,
    logger: logging.Logger,
) -> tuple[int, int]:
    """Copy all files under ``source_dir`` to ``target_dir``, honoring ``.ytignore``."""
    ignore_matcher = YTIgnoreMatcher(source_dir)
    file_count = 0
    ignored_count = 0
    for source_file in source_dir.rglob("*"):
        if not source_file.is_file():
            continue
        if ignore_matcher.should_ignore(source_file):
            logger.debug(
                "Ignoring file (matched .ytignore): %s",
                source_file.relative_to(source_dir),
            )
            ignored_count += 1
            continue
        rel_path = source_file.relative_to(source_dir)
        target_file = target_dir / rel_path
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target_file)
        file_count += 1
    return file_count, ignored_count


def _append_targets_from_upload_path(
    i: int,
    entry: dict[str, str],
    pipeline_dir: Path,
    pipeline_dir_resolved: Path,
) -> tuple[str, str]:
    if "source" not in entry:
        msg = "upload_paths entry missing required 'source' key."
        raise ValueError(msg)
    source = entry.get("source", "")
    resolved_path = (pipeline_dir / source).resolve()
    if not resolved_path.is_relative_to(pipeline_dir_resolved):
        msg = (
            f"upload_paths[{i}] source must be within pipeline directory: "
            f"{resolved_path} (pipeline: {pipeline_dir_resolved})."
        )
        raise ValueError(msg)
    target = entry.get("target")
    resolved_target = _resolve_upload_target(source, target, pipeline_dir)
    return resolved_target, f"upload_paths[{source}]"


def _collect_upload_targets(
    upload_modules: list[str] | None,
    upload_paths: list[dict[str, str]] | None,
    pipeline_dir: Path,
) -> list[tuple[str, str]]:
    """Build (target_name, source_description) rows for validation."""
    targets: list[tuple[str, str]] = [("ytjobs", _IMPLICIT_YTJOBS_SOURCE)]
    for mod in upload_modules or []:
        top_level = mod.split(".", maxsplit=1)[0]
        targets.append((top_level, f"upload_modules[{mod}]"))

    pipeline_dir_resolved = pipeline_dir.resolve()
    for i, entry in enumerate(upload_paths or []):
        targets.append(
            _append_targets_from_upload_path(
                i,
                entry,
                pipeline_dir,
                pipeline_dir_resolved,
            ),
        )
    return targets


def _raise_if_reserved_upload_targets(targets: list[tuple[str, str]]) -> None:
    reserved = {"stages", "ytjobs"}
    for target, source_desc in targets:
        if target in reserved and source_desc != _IMPLICIT_YTJOBS_SOURCE:
            msg = (
                f"Reserved target name '{target}' cannot be used. "
                "Reserved names: stages, ytjobs."
            )
            raise ValueError(msg)


def _raise_if_upload_target_conflicts(targets: list[tuple[str, str]]) -> None:
    seen: dict[str, str] = {}
    for target, source_desc in targets:
        if target in seen and seen[target] != source_desc:
            sources = f"{seen[target]}, {source_desc}"
            msg = f"Upload target conflict: '{target}' is used by multiple sources: {sources}."
            raise ValueError(msg)
        seen[target] = source_desc


def _validate_upload_config(
    upload_modules: list[str] | None,
    upload_paths: list[dict[str, str]] | None,
    pipeline_dir: Path,
) -> None:
    """Validate upload config for reserved targets and conflicts.

    Args:
        upload_modules: List of module names
        upload_paths: List of dicts with source and optional target
        pipeline_dir: Pipeline directory for path resolution

    Raises:
        ValueError: If reserved target used or target conflict detected

    """
    targets = _collect_upload_targets(upload_modules, upload_paths, pipeline_dir)
    _raise_if_reserved_upload_targets(targets)
    _raise_if_upload_target_conflicts(targets)


def _import_top_level_module_for_upload(
    module_name: str, top_level: str
) -> types.ModuleType:
    try:
        return importlib.import_module(top_level)
    except ImportError as e:
        msg = f"Failed to import module '{module_name}' (top-level: {top_level}): {e}."
        raise ValueError(msg) from e


def _package_source_dir_for_upload(module: types.ModuleType, module_name: str) -> Path:
    if getattr(module, "__file__", None) is None:
        msg = (
            f"Module '{module_name}' has no __file__ (namespace or non-file package). "
            "Only file-based packages are supported."
        )
        raise ValueError(msg)
    if not hasattr(module, "__path__"):
        msg = (
            f"Module '{module_name}' is a single-file module, not a package. "
            "upload_modules supports only packages (directories with __init__.py). "
            "Single-file modules would copy the entire containing directory."
        )
        raise ValueError(msg)
    source_dir = Path(module.__path__[0]).resolve()
    if not source_dir.is_dir():
        msg = f"Module '{module_name}' has invalid __path__: {source_dir} is not a directory."
        raise ValueError(msg)
    return source_dir


def _require_upload_path_directory(
    source_path: str,
    pipeline_dir: Path,
    pipeline_dir_resolved: Path,
) -> Path:
    resolved = (pipeline_dir / source_path).resolve()
    if not resolved.is_relative_to(pipeline_dir_resolved):
        msg = (
            f"Upload path source must be within pipeline directory: {resolved} "
            f"(pipeline: {pipeline_dir_resolved}). Paths like '../foo' are not allowed."
        )
        raise ValueError(msg)
    if not resolved.exists():
        msg = f"Upload path source does not exist: {resolved}."
        raise FileNotFoundError(msg)
    if not resolved.is_dir():
        msg = f"Upload path source must be a directory: {resolved}."
        raise ValueError(msg)
    return resolved


def _copy_module_to_build_dir(
    module_name: str,
    target_dir: Path,
    logger: logging.Logger,
) -> int:
    """Copy an importable Python package to build directory.

    Only packages (directories with __init__.py) are supported; single-file
    modules are rejected. For dotted paths (e.g. my_package.submodule),
    copies the full top-level package. Respects .ytignore in the source dir.

    Args:
        module_name: Python module name to import (e.g. my_package or my_package.sub)
        target_dir: Target directory in build (top-level package name)
        logger: Logger instance

    Returns:
        Number of files copied

    Raises:
        ValueError: If module cannot be imported or has unsupported layout

    """
    top_level = module_name.split(".", maxsplit=1)[0]
    module = _import_top_level_module_for_upload(module_name, top_level)
    source_dir = _package_source_dir_for_upload(module, module_name)

    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Copying module %s to %s...", module_name, target_dir)

    file_count, ignored_count = _copy_tree_with_ytignore(source_dir, target_dir, logger)

    log_success(logger, f"Copied {file_count} {module_name} files")
    if ignored_count > 0:
        logger.debug("  Ignored %s files (matched .ytignore patterns)", ignored_count)
    return file_count


def _copy_path_to_build_dir(
    source_path: str,
    target_name: str,
    build_dir: Path,
    pipeline_dir: Path,
    logger: logging.Logger,
) -> int:
    """Copy a local path to build directory.

    Respects .ytignore patterns if present in the source directory.

    Args:
        source_path: Source path relative to pipeline_dir
        target_name: Target directory name in build
        build_dir: Build directory path
        pipeline_dir: Pipeline directory for resolving source
        logger: Logger instance

    Returns:
        Number of files copied

    Raises:
        ValueError: If source path escapes pipeline directory or is not a directory
        FileNotFoundError: If source does not exist

    """
    pipeline_dir_resolved = pipeline_dir.resolve()
    resolved = _require_upload_path_directory(
        source_path,
        pipeline_dir,
        pipeline_dir_resolved,
    )

    target_dir = build_dir / target_name
    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Copying %s to %s...", resolved, target_dir)

    file_count, ignored_count = _copy_tree_with_ytignore(resolved, target_dir, logger)

    log_success(logger, f"Copied {file_count} files from {source_path}")
    if ignored_count > 0:
        logger.debug("  Ignored %s files (matched .ytignore patterns)", ignored_count)
    return file_count


def _stage_config_has_nonempty_job(config: object) -> bool:
    if not isinstance(config, DictConfig):
        return False
    if "job" not in config:
        return False
    job = config.get("job")
    return bool(job and (not isinstance(job, dict) or len(job) > 0))


def _copy_stage_config_yaml(
    stage_dir: Path,
    build_dir: Path,
    stage_name: str,
    ignore_matcher: YTIgnoreMatcher,
    logger: logging.Logger,
) -> tuple[int, int]:
    """Copy ``config.yaml`` when present and not ignored. Returns (files_added, ignores_added)."""
    config_path = stage_dir / "config.yaml"
    if not config_path.exists():
        return 0, 0
    if ignore_matcher.should_ignore(config_path):
        logger.debug(
            "  Ignoring config: %s/config.yaml (matched .ytignore)",
            stage_name,
        )
        return 0, 1
    try:
        config = OmegaConf.load(config_path)
        if _stage_config_has_nonempty_job(config):
            target_config = build_dir / "stages" / stage_name / "config.yaml"
            target_config.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(config_path, target_config)
            logger.debug("  Copied config: %s/config.yaml", stage_name)
            return 1, 0
    except (
        OSError,
        OmegaConfBaseException,
        TypeError,
        ValueError,
        yaml.YAMLError,
    ) as exc:
        logger.debug("Skipping invalid config %s: %s", config_path, exc)
    return 0, 0


@dataclass(frozen=True)
class _StageSrcCopyParams:
    """Inputs for copying a stage ``src/`` tree into the local build directory."""

    stage_dir: Path
    src_dir: Path
    build_dir: Path
    stage_name: str
    ignore_matcher: YTIgnoreMatcher
    logger: logging.Logger


def _copy_stage_src_directory(params: _StageSrcCopyParams) -> tuple[int, int]:
    """Copy ``src/`` tree. Returns (files_added, ignores_added)."""
    target_src = params.build_dir / "stages" / params.stage_name / "src"
    target_src.mkdir(parents=True, exist_ok=True)
    file_count = 0
    ignored_count = 0
    for source_file in params.src_dir.rglob("*"):
        if not source_file.is_file():
            continue
        if params.ignore_matcher.should_ignore(source_file):
            params.logger.debug(
                "  Ignoring file: %s (matched .ytignore)",
                source_file.relative_to(params.stage_dir),
            )
            ignored_count += 1
            continue
        rel_path = source_file.relative_to(params.src_dir)
        target_file = target_src / rel_path
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target_file)
        file_count += 1
    params.logger.debug("  Copied %s files from %s/src/", file_count, params.stage_name)
    if ignored_count > 0:
        params.logger.debug(
            "  Ignored %s files from %s/ (matched .ytignore patterns)",
            ignored_count,
            params.stage_name,
        )
    return file_count, ignored_count


def _copy_stage_to_build_dir(
    build_dir: Path,
    stage_dir: Path,
    logger: logging.Logger,
) -> int:
    """Copy a single stage's code and config to local build directory.

    Respects .ytignore patterns if present in the stage directory.

    Args:
        build_dir: Local build directory path
        stage_dir: Path to stage directory (e.g., stages/run_map/)
        logger: Logger instance

    Returns:
        Number of files copied

    """
    stage_name = stage_dir.name
    file_count = 0
    ignored_count = 0

    ignore_matcher = YTIgnoreMatcher(stage_dir)

    fc_cfg, ig_cfg = _copy_stage_config_yaml(
        stage_dir,
        build_dir,
        stage_name,
        ignore_matcher,
        logger,
    )
    file_count += fc_cfg
    ignored_count += ig_cfg

    src_dir = stage_dir / "src"
    if src_dir.exists():
        fc_src, ig_src = _copy_stage_src_directory(
            _StageSrcCopyParams(
                stage_dir=stage_dir,
                src_dir=src_dir,
                build_dir=build_dir,
                stage_name=stage_name,
                ignore_matcher=ignore_matcher,
                logger=logger,
            ),
        )
        file_count += fc_src
        ignored_count += ig_src

    return file_count


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
