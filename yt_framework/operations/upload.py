"""Build `source.tar.gz`, merge extra modules/paths, and push artifacts to Cypress."""

import importlib
import logging
import shutil
import tarfile
from pathlib import Path
from typing import Literal

from omegaconf import OmegaConf

from yt_framework.utils import log_header, log_success
from yt_framework.utils.ignore import YTIgnoreMatcher
from yt_framework.yt.client_base import BaseYTClient

# Marker for the implicit ytjobs framework package in target conflict checks
_IMPLICIT_YTJOBS_SOURCE = "implicit (framework)"

# Default build code directory name
_BUILD_CODE_DIR = ".build"


def _get_ytjobs_dir() -> Path:
    """Get ytjobs package directory dynamically."""
    import ytjobs

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

    # Initialize .ytignore matcher for ytjobs directory
    ignore_matcher = YTIgnoreMatcher(ytjobs_dir)

    file_count = 0
    ignored_count = 0

    for source_file in ytjobs_dir.rglob("*"):
        if source_file.is_file():
            # Check if file should be ignored
            if ignore_matcher.should_ignore(source_file):
                logger.debug(
                    "Ignoring file (matched .ytignore): %s",
                    source_file.relative_to(ytjobs_dir),
                )
                ignored_count += 1
                continue

            rel_path = source_file.relative_to(ytjobs_dir)
            target_file = target_dir / rel_path
            target_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, target_file)
            file_count += 1

    log_success(logger, f"Copied {file_count} ytjobs files")
    if ignored_count > 0:
        logger.debug("  Ignored %s files (matched .ytignore patterns)", ignored_count)
    return file_count


def _resolve_upload_target(source: str, target: str | None, pipeline_dir: Path) -> str:
    """Resolve target name for upload_paths entry.

    Args:
        source: Source path from config
        target: Optional target from config
        pipeline_dir: Pipeline directory (unused, for API consistency)

    Returns:
        Target name (from config or derived from source basename)

    """
    if target is not None and str(target).strip():
        return str(target).strip()
    return Path(source).name


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
    reserved = {"stages", "ytjobs"}

    targets: list[tuple[str, str]] = []  # (target, source_description)

    # ytjobs is implicit
    targets.append(("ytjobs", _IMPLICIT_YTJOBS_SOURCE))

    # upload_modules (use top-level package as target; my_package.sub -> my_package)
    for mod in upload_modules or []:
        top_level = mod.split(".")[0]
        targets.append((top_level, f"upload_modules[{mod}]"))

    # upload_paths (validate source is within pipeline_dir)
    pipeline_dir_resolved = pipeline_dir.resolve()
    for i, entry in enumerate(upload_paths or []):
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
        targets.append((resolved_target, f"upload_paths[{source}]"))

    # Check reserved
    for target, source_desc in targets:
        if target in reserved and source_desc != _IMPLICIT_YTJOBS_SOURCE:
            msg = (
                f"Reserved target name '{target}' cannot be used. "
                f"Reserved names: stages, ytjobs."
            )
            raise ValueError(msg)

    # Check conflicts
    seen: dict[str, str] = {}
    for target, source_desc in targets:
        if target in seen and seen[target] != source_desc:
            sources = f"{seen[target]}, {source_desc}"
            msg = f"Upload target conflict: '{target}' is used by multiple sources: {sources}."
            raise ValueError(msg)
        seen[target] = source_desc


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
    try:
        module = importlib.import_module(top_level)
    except ImportError as e:
        msg = f"Failed to import module '{module_name}' (top-level: {top_level}): {e}."
        raise ValueError(msg) from e

    if getattr(module, "__file__", None) is None:
        msg = (
            f"Module '{module_name}' has no __file__ (namespace or non-file package). "
            "Only file-based packages are supported."
        )
        raise ValueError(msg)

    # Use __path__ for packages; reject single-file modules (which would copy
    # the entire containing directory, e.g. site-packages)
    if hasattr(module, "__path__"):
        source_dir = Path(module.__path__[0]).resolve()
        if not source_dir.is_dir():
            msg = f"Module '{module_name}' has invalid __path__: {source_dir} is not a directory."
            raise ValueError(msg)
    else:
        msg = (
            f"Module '{module_name}' is a single-file module, not a package. "
            "upload_modules supports only packages (directories with __init__.py). "
            "Single-file modules would copy the entire containing directory."
        )
        raise ValueError(msg)

    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Copying module %s to %s...", module_name, target_dir)

    ignore_matcher = YTIgnoreMatcher(source_dir)
    file_count = 0
    ignored_count = 0

    for source_file in source_dir.rglob("*"):
        if source_file.is_file():
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

    target_dir = build_dir / target_name
    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Copying %s to %s...", resolved, target_dir)

    ignore_matcher = YTIgnoreMatcher(resolved)
    file_count = 0
    ignored_count = 0

    for source_file in resolved.rglob("*"):
        if source_file.is_file():
            if ignore_matcher.should_ignore(source_file):
                logger.debug(
                    "Ignoring file (matched .ytignore): %s",
                    source_file.relative_to(resolved),
                )
                ignored_count += 1
                continue

            rel_path = source_file.relative_to(resolved)
            target_file = target_dir / rel_path
            target_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, target_file)
            file_count += 1

    log_success(logger, f"Copied {file_count} files from {source_path}")
    if ignored_count > 0:
        logger.debug("  Ignored %s files (matched .ytignore patterns)", ignored_count)
    return file_count


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

    # Initialize .ytignore matcher for stage directory
    ignore_matcher = YTIgnoreMatcher(stage_dir)
    ignored_count = 0

    # Copy config.yaml if it exists and has job section
    config_path = stage_dir / "config.yaml"
    if config_path.exists():
        # Check if config should be ignored
        if not ignore_matcher.should_ignore(config_path):
            try:
                config = OmegaConf.load(config_path)
                if (
                    "job" in config
                    and config.job
                    and (not isinstance(config.job, dict) or len(config.job) > 0)
                ):
                    target_config = build_dir / "stages" / stage_name / "config.yaml"
                    target_config.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(config_path, target_config)
                    file_count += 1
                    logger.debug("  Copied config: %s/config.yaml", stage_name)
            except Exception:
                # If config parsing fails, skip it
                pass
        else:
            logger.debug(
                "  Ignoring config: %s/config.yaml (matched .ytignore)", stage_name
            )
            ignored_count += 1

    # Copy src directory
    src_dir = stage_dir / "src"
    if src_dir.exists():
        target_src = build_dir / "stages" / stage_name / "src"
        target_src.mkdir(parents=True, exist_ok=True)

        for source_file in src_dir.rglob("*"):
            if source_file.is_file():
                # Check if file should be ignored
                if ignore_matcher.should_ignore(source_file):
                    logger.debug(
                        "  Ignoring file: %s (matched .ytignore)",
                        source_file.relative_to(stage_dir),
                    )
                    ignored_count += 1
                    continue

                rel_path = source_file.relative_to(src_dir)
                target_file = target_src / rel_path
                target_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_file, target_file)
                file_count += 1

        logger.debug("  Copied %s files from %s/src/", file_count, stage_name)
        if ignored_count > 0:
            logger.debug(
                "  Ignored %s files from %s/ (matched .ytignore patterns)",
                ignored_count,
                stage_name,
            )

    return file_count


def _bash_wrapper_script_body(
    stage_name: str, python_script_relative: str, label: str
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


def _load_stage_job_section(stage_dir: Path, logger: logging.Logger) -> dict:
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
    except Exception as e:
        logger.warning("Could not read %s for wrapper script paths: %s", cfg_path, e)
        return {}


def _resolve_map_reduce_command_scripts(
    stage_dir: Path,
    logger: logging.Logger,
) -> tuple[str | None, str | None]:
    """Resolve mapper/reducer Python entrypoints under stages/<name>/src/ for map-reduce
    command mode wrappers. Uses ``job.map_reduce_command`` when set.
    """
    job = _load_stage_job_section(stage_dir, logger)
    mrc = job.get("map_reduce_command") or {}
    if not isinstance(mrc, dict):
        mrc = {}
    mapper = str(mrc.get("mapper_script") or "mapper.py")
    reducer = mrc.get("reducer_script")
    if reducer is not None:
        reducer = str(reducer)
    src = stage_dir / "src"
    if reducer is None:
        for candidate in (
            "reducer.py",
            "reducer_mds.py",
            "reducer_main.py",
            "reducer_index.py",
        ):
            if (src / candidate).is_file():
                reducer = candidate
                break
    if not (src / mapper).is_file():
        logger.debug("No %s at %s — skipping map_reduce_mapper wrapper", mapper, src)
        return None, None
    if not reducer or not (src / reducer).is_file():
        logger.debug("No reducer script resolved for map-reduce in %s", src)
        return mapper, None
    return mapper, reducer


def _resolve_reduce_command_script(
    stage_dir: Path, logger: logging.Logger
) -> str | None:
    """Reducer entrypoint for reduce-only command mode (``job.reduce_command``)."""
    job = _load_stage_job_section(stage_dir, logger)
    rc = job.get("reduce_command") or {}
    if not isinstance(rc, dict):
        rc = {}
    explicit = rc.get("reducer_script")
    if explicit:
        name = str(explicit)
        if (stage_dir / "src" / name).is_file():
            return name
        logger.warning(
            "reduce_command.reducer_script %s not found under %s",
            name,
            stage_dir / "src",
        )
    src = stage_dir / "src"
    for candidate in (
        "reducer.py",
        "reducer_index.py",
        "reducer_mds.py",
        "reducer_main.py",
    ):
        if (src / candidate).is_file():
            return candidate
    return None


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
        stage_name, script_path, f"{operation_type} operation"
    )
    _write_wrapper_file(
        build_dir,
        f"operation_wrapper_{stage_name}_{operation_type}.sh",
        body,
        logger,
    )


def _create_map_reduce_command_wrappers(
    stage_name: str,
    stage_dir: Path,
    build_dir: Path,
    logger: logging.Logger,
) -> None:
    """Wrappers for ``tar_command_bootstrap`` map-reduce (JSON stdin/stdout legs)."""
    mapper_f, reducer_f = _resolve_map_reduce_command_scripts(stage_dir, logger)
    if not mapper_f:
        return
    m_rel = f"stages/{stage_name}/src/{mapper_f}"
    body_m = _bash_wrapper_script_body(
        stage_name, m_rel, "map-reduce mapper (command mode)"
    )
    _write_wrapper_file(
        build_dir,
        f"operation_wrapper_{stage_name}_map_reduce_mapper.sh",
        body_m,
        logger,
    )
    if reducer_f:
        r_rel = f"stages/{stage_name}/src/{reducer_f}"
        body_r = _bash_wrapper_script_body(
            stage_name, r_rel, "map-reduce reducer (command mode)"
        )
        _write_wrapper_file(
            build_dir,
            f"operation_wrapper_{stage_name}_map_reduce_reducer.sh",
            body_r,
            logger,
        )
    else:
        logger.warning(
            "Stage %s: map_reduce_mapper wrapper created but no reducer script — "
            "map-reduce command mode will fail until job.map_reduce_command.reducer_script is set",
            stage_name,
        )


def _create_reduce_command_wrapper(
    stage_name: str,
    stage_dir: Path,
    build_dir: Path,
    logger: logging.Logger,
) -> None:
    """Wrapper for reduce-only operations with ``tar_command_bootstrap``."""
    red = _resolve_reduce_command_script(stage_dir, logger)
    if not red:
        return
    r_rel = f"stages/{stage_name}/src/{red}"
    body = _bash_wrapper_script_body(stage_name, r_rel, "reduce (command mode)")
    _write_wrapper_file(
        build_dir,
        f"operation_wrapper_{stage_name}_reduce.sh",
        body,
        logger,
    )


def _create_wrappers_for_stage(
    stage_name: str,
    stage_dir: Path,
    build_dir: Path,
    logger: logging.Logger,
) -> None:
    """Create wrapper scripts for a stage based on what operation types it has.

    Args:
        stage_name: Name of the stage
        stage_dir: Path to stage directory
        build_dir: Local build directory path
        logger: Logger instance

    Returns:
        None

    """
    src_dir = stage_dir / "src"

    if not src_dir.exists():
        return

    # Check which operation types this stage supports
    has_mapper = (src_dir / "mapper.py").is_file() or bool(
        list(src_dir.glob("partition_*.py"))
    )
    has_vanilla = (src_dir / "vanilla.py").exists()

    # Create wrapper for each operation type found
    if has_mapper:
        _create_unified_wrapper_script(
            stage_name=stage_name,
            operation_type="map",
            build_dir=build_dir,
            logger=logger,
        )

    if has_vanilla:
        _create_unified_wrapper_script(
            stage_name=stage_name,
            operation_type="vanilla",
            build_dir=build_dir,
            logger=logger,
        )

    if has_mapper:
        _create_map_reduce_command_wrappers(
            stage_name=stage_name,
            stage_dir=stage_dir,
            build_dir=build_dir,
            logger=logger,
        )
        _create_reduce_command_wrapper(
            stage_name=stage_name,
            stage_dir=stage_dir,
            build_dir=build_dir,
            logger=logger,
        )


def build_code_locally(
    build_dir: Path,
    pipeline_dir: Path,
    logger: logging.Logger,
    create_wrappers: bool = False,
    upload_modules: list[str] | None = None,
    upload_paths: list[dict[str, str]] | None = None,
) -> int:
    """Build all code in a local build directory.

    Copies ytjobs package, optional custom modules/paths, and all stages' code
    to the build directory, preserving the same structure as would be uploaded to YT.

    Args:
        build_dir: Local build directory path
        pipeline_dir: Path to pipeline directory
        logger: Logger instance
        create_wrappers: If True, create wrapper scripts for all stages
        upload_modules: Optional list of module names to upload
        upload_paths: Optional list of {source, target?} for local paths

    Returns:
        Total number of files copied

    """
    log_header(logger, "Code Build", f"Build directory: {build_dir}")

    # Create build directory
    build_dir.mkdir(parents=True)

    # Validate and resolve upload config
    _validate_upload_config(
        upload_modules=upload_modules,
        upload_paths=upload_paths,
        pipeline_dir=pipeline_dir,
    )

    # Copy ytjobs package (implicit, always)
    ytjobs_files = _copy_ytjobs_to_build_dir(
        build_dir=build_dir,
        logger=logger,
    )

    # Copy upload_modules (use top-level package name for target; dotted paths
    # e.g. my_package.submodule become build_dir/my_package/ with full tree)
    module_files = 0
    for mod in upload_modules or []:
        top_level = mod.split(".")[0]
        module_files += _copy_module_to_build_dir(
            module_name=mod,
            target_dir=build_dir / top_level,
            logger=logger,
        )

    # Copy upload_paths
    path_files = 0
    for entry in upload_paths or []:
        source = entry["source"]
        target = _resolve_upload_target(
            source=source,
            target=entry.get("target"),
            pipeline_dir=pipeline_dir,
        )
        path_files += _copy_path_to_build_dir(
            source_path=source,
            target_name=target,
            build_dir=build_dir,
            pipeline_dir=pipeline_dir,
            logger=logger,
        )

    # Copy all stages
    stages_dir = pipeline_dir / "stages"
    stage_files = 0
    stage_dirs_list = []

    for stage_dir in stages_dir.iterdir():
        if stage_dir.is_dir() and (stage_dir / "src").exists():
            stage_name = stage_dir.name
            stage_dirs_list.append((stage_name, stage_dir))
            files_copied = _copy_stage_to_build_dir(
                build_dir=build_dir,
                stage_dir=stage_dir,
                logger=logger,
            )
            stage_files += files_copied

    # Create wrapper scripts if requested (for tar archive mode)
    if create_wrappers:
        for stage_name, stage_dir in stage_dirs_list:
            _create_wrappers_for_stage(
                stage_name=stage_name,
                stage_dir=stage_dir,
                build_dir=build_dir,
                logger=logger,
            )
        logger.debug("Created wrapper scripts for %s stages", len(stage_dirs_list))

    total_files = ytjobs_files + module_files + path_files + stage_files
    log_success(logger, f"Code build completed: {total_files} total files")
    return total_files


def create_code_archive(
    build_dir: Path,
    archive_path: Path,
    logger: logging.Logger,
) -> None:
    """Create a tar.gz archive from the build directory.

    Args:
        build_dir: Local build directory path
        archive_path: Path where the archive should be created
        logger: Logger instance

    Returns:
        None

    """
    log_header(logger, "Code Archive", f"Creating archive: {archive_path}")

    # Ensure parent directory exists
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    with tarfile.open(archive_path, "w:gz") as tar:
        # Add all files from build directory
        for file_path in build_dir.rglob("*"):
            if file_path.is_file():
                arcname = file_path.relative_to(build_dir)
                tar.add(file_path, arcname=arcname, recursive=False)

    archive_size_mb = archive_path.stat().st_size / (1024 * 1024)
    log_success(logger, f"Archive created: {archive_size_mb:.2f} MB")


def upload_code_archive(
    yt_client: BaseYTClient,
    archive_path: Path,
    build_folder: str,
    logger: logging.Logger,
) -> None:
    """Upload code archive to YT.

    Args:
        yt_client: YT client instance
        archive_path: Local path to the tar.gz archive
        build_folder: YT build folder path
        logger: Logger instance

    Returns:
        None

    """
    log_header(logger, "Uploading Code Archive to YT")
    logger.info("Build folder: %s", build_folder)

    archive_name = archive_path.name
    archive_yt_path = f"{build_folder}/{archive_name}"

    yt_client.upload_file(
        local_path=archive_path,
        yt_path=archive_yt_path,
        create_parent_dir=True,  # Create build folder if it doesn't exist
    )

    log_success(logger, f"Archive uploaded: {archive_yt_path}")


def _resolve_build_code_dir(
    pipeline_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Resolve build code directory path.

    Args:
        pipeline_dir: Path to pipeline directory
        logger: Logger instance

    Returns:
        Resolved build code directory path

    """
    build_code_dir = pipeline_dir / _BUILD_CODE_DIR
    logger.debug("Using build directory: %s", build_code_dir)

    # Ensure build directory exists
    if build_code_dir.exists():
        shutil.rmtree(build_code_dir)
    build_code_dir.mkdir(parents=True)
    return build_code_dir


def upload_all_code(
    yt_client: BaseYTClient,
    build_folder: str,
    pipeline_dir: Path,
    logger: logging.Logger,
    upload_modules: list[str] | None = None,
    upload_paths: list[dict[str, str]] | None = None,
) -> None:
    """Upload all code to YT: ytjobs package, optional custom modules/paths, and stages.

    Builds code locally, creates tar archive, and uploads it to YT.

    Args:
        yt_client: YT client instance
        build_folder: YT build folder path
        pipeline_dir: Path to pipeline directory
        logger: Logger instance
        upload_modules: Optional list of module names to upload
        upload_paths: Optional list of {source, target?} for local paths

    Returns:
        None

    """
    log_header(
        logger, "Code Upload", f"Tar archive mode | Build folder: {build_folder}"
    )

    # Resolve build directory path
    build_code_dir = _resolve_build_code_dir(pipeline_dir=pipeline_dir, logger=logger)

    # Build code locally (including wrapper scripts for tar archive mode)
    build_dir = build_code_dir / "source"
    build_code_locally(
        build_dir=build_dir,
        pipeline_dir=pipeline_dir,
        logger=logger,
        create_wrappers=True,
        upload_modules=upload_modules,
        upload_paths=upload_paths,
    )

    # Create archive
    archive_path = build_code_dir / "source.tar.gz"
    create_code_archive(
        build_dir=build_dir,
        archive_path=archive_path,
        logger=logger,
    )

    # Upload archive
    upload_code_archive(
        yt_client=yt_client,
        archive_path=archive_path,
        build_folder=build_folder,
        logger=logger,
    )

    log_success(logger, "Code upload completed")
