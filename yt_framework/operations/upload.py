"""
Upload Operations
=================

Operations for uploading code and configuration files to YT.
"""

import importlib
import logging
import shutil
import tarfile
from pathlib import Path
from typing import Dict, List, Optional, Literal, Tuple

from yt_framework.utils import log_header, log_success
from yt_framework.yt.client_base import BaseYTClient
from omegaconf import OmegaConf
from yt_framework.utils.ignore import YTIgnoreMatcher


def _get_ytjobs_dir() -> Path:
    """Get ytjobs package directory dynamically."""
    import ytjobs

    return Path(ytjobs.__file__).parent


def _copy_ytjobs_to_build_dir(
    build_dir: Path,
    logger: logging.Logger,
) -> int:
    """
    Copy ytjobs package to local build directory.

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

    logger.info(f"Copying ytjobs package to {target_dir}...")

    # Initialize .ytignore matcher for ytjobs directory
    ignore_matcher = YTIgnoreMatcher(ytjobs_dir)

    file_count = 0
    ignored_count = 0

    for source_file in ytjobs_dir.rglob("*"):
        if source_file.is_file():
            # Check if file should be ignored
            if ignore_matcher.should_ignore(source_file):
                logger.debug(
                    f"Ignoring file (matched .ytignore): {source_file.relative_to(ytjobs_dir)}"
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
        logger.debug(f"  Ignored {ignored_count} files (matched .ytignore patterns)")
    return file_count


def _resolve_upload_target(source: str, target: Optional[str], pipeline_dir: Path) -> str:
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
    upload_modules: Optional[List[str]],
    upload_paths: Optional[List[Dict[str, str]]],
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

    targets: List[Tuple[str, str]] = []  # (target, source_description)

    # ytjobs is implicit
    targets.append(("ytjobs", "implicit (framework)"))

    # upload_modules
    for mod in upload_modules or []:
        targets.append((mod, f"upload_modules[{mod}]"))

    # upload_paths
    for i, entry in enumerate(upload_paths or []):
        if "source" not in entry:
            raise ValueError("upload_paths entry missing required 'source' key.")
        source = entry.get("source", "")
        target = entry.get("target")
        resolved_target = _resolve_upload_target(source, target, pipeline_dir)
        targets.append((resolved_target, f"upload_paths[{source}]"))

    # Check reserved
    for target, source_desc in targets:
        if target in reserved and source_desc != "implicit (framework)":
            raise ValueError(
                f"Reserved target name '{target}' cannot be used. "
                f"Reserved names: stages, ytjobs."
            )

    # Check conflicts
    seen: Dict[str, str] = {}
    for target, source_desc in targets:
        if target in seen and seen[target] != source_desc:
            sources = f"{seen[target]}, {source_desc}"
            raise ValueError(
                f"Upload target conflict: '{target}' is used by multiple sources: {sources}."
            )
        seen[target] = source_desc


def _copy_module_to_build_dir(
    module_name: str,
    target_dir: Path,
    logger: logging.Logger,
) -> int:
    """Copy an importable module/package to build directory.

    Respects .ytignore patterns if present in the module directory.

    Args:
        module_name: Python module name to import
        target_dir: Target directory in build
        logger: Logger instance

    Returns:
        Number of files copied

    Raises:
        ValueError: If module cannot be imported
    """
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ValueError(f"Failed to import module '{module_name}': {e}.") from e

    source_dir = Path(module.__file__).parent
    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Copying module {module_name} to {target_dir}...")

    ignore_matcher = YTIgnoreMatcher(source_dir)
    file_count = 0
    ignored_count = 0

    for source_file in source_dir.rglob("*"):
        if source_file.is_file():
            if ignore_matcher.should_ignore(source_file):
                logger.debug(
                    f"Ignoring file (matched .ytignore): {source_file.relative_to(source_dir)}"
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
        logger.debug(f"  Ignored {ignored_count} files (matched .ytignore patterns)")
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
        FileNotFoundError: If source does not exist
        ValueError: If source is not a directory
    """
    resolved = (pipeline_dir / source_path).resolve()

    if not resolved.exists():
        raise FileNotFoundError(
            f"Upload path source does not exist: {resolved}."
        )

    if not resolved.is_dir():
        raise ValueError(
            f"Upload path source must be a directory: {resolved}."
        )

    target_dir = build_dir / target_name
    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Copying {resolved} to {target_dir}...")

    ignore_matcher = YTIgnoreMatcher(resolved)
    file_count = 0
    ignored_count = 0

    for source_file in resolved.rglob("*"):
        if source_file.is_file():
            if ignore_matcher.should_ignore(source_file):
                logger.debug(
                    f"Ignoring file (matched .ytignore): {source_file.relative_to(resolved)}"
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
        logger.debug(f"  Ignored {ignored_count} files (matched .ytignore patterns)")
    return file_count


def _copy_stage_to_build_dir(
    build_dir: Path,
    stage_dir: Path,
    logger: logging.Logger,
) -> int:
    """
    Copy a single stage's code and config to local build directory.

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
                    logger.debug(f"  Copied config: {stage_name}/config.yaml")
            except Exception:
                # If config parsing fails, skip it
                pass
        else:
            logger.debug(
                f"  Ignoring config: {stage_name}/config.yaml (matched .ytignore)"
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
                        f"  Ignoring file: {source_file.relative_to(stage_dir)} (matched .ytignore)"
                    )
                    ignored_count += 1
                    continue

                rel_path = source_file.relative_to(src_dir)
                target_file = target_src / rel_path
                target_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_file, target_file)
                file_count += 1

        logger.debug(f"  Copied {file_count} files from {stage_name}/src/")
        if ignored_count > 0:
            logger.debug(
                f"  Ignored {ignored_count} files from {stage_name}/ (matched .ytignore patterns)"
            )

    return file_count


def _create_unified_wrapper_script(
    stage_name: str,
    operation_type: Literal["map", "vanilla"],
    build_dir: Path,
    logger: logging.Logger,
) -> None:
    """
    Create unified wrapper script for map or vanilla operations.

    The wrapper script:
    1. Extracts code.tar.gz archive
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
    # Determine script to execute
    if operation_type == "map":
        script_path = f"stages/{stage_name}/src/mapper.py"
    else:  # vanilla
        script_path = f"stages/{stage_name}/src/vanilla.py"

    # Check if requirements.txt exists in the stage directory
    requirements_path = f"stages/{stage_name}/requirements.txt"

    bash_script = f"""
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

# Execute the {operation_type} operation
python3 {script_path}
"""

    # Unified naming convention for both operation types
    wrapper_path = build_dir / f"operation_wrapper_{stage_name}_{operation_type}.sh"
    wrapper_path.write_text(bash_script)
    wrapper_path.chmod(0o755)
    logger.debug(f"Created wrapper script: {wrapper_path}")


def _create_wrappers_for_stage(
    stage_name: str,
    stage_dir: Path,
    build_dir: Path,
    logger: logging.Logger,
) -> None:
    """
    Create wrapper scripts for a stage based on what operation types it has.

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
    has_mapper = (src_dir / "mapper.py").exists()
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


def build_code_locally(
    build_dir: Path,
    pipeline_dir: Path,
    logger: logging.Logger,
    create_wrappers: bool = False,
    upload_modules: Optional[List[str]] = None,
    upload_paths: Optional[List[Dict[str, str]]] = None,
) -> int:
    """
    Build all code in a local build directory.

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
    build_dir.mkdir(parents=True, exist_ok=True)

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

    # Copy upload_modules
    module_files = 0
    for mod in upload_modules or []:
        module_files += _copy_module_to_build_dir(
            module_name=mod,
            target_dir=build_dir / mod,
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
        logger.debug(f"Created wrapper scripts for {len(stage_dirs_list)} stages")

    total_files = ytjobs_files + module_files + path_files + stage_files
    log_success(logger, f"Code build completed: {total_files} total files")
    return total_files


def create_code_archive(
    build_dir: Path,
    archive_path: Path,
    logger: logging.Logger,
) -> None:
    """
    Create a tar.gz archive from the build directory.

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
    """
    Upload code archive to YT.

    Args:
        yt_client: YT client instance
        archive_path: Local path to the tar.gz archive
        build_folder: YT build folder path
        logger: Logger instance

    Returns:
        None
    """
    log_header(logger, "Uploading Code Archive to YT")
    logger.info(f"Build folder: {build_folder}")

    archive_yt_path = f"{build_folder}/code.tar.gz"

    yt_client.upload_file(
        local_path=archive_path,
        yt_path=archive_yt_path,
        create_parent_dir=True,  # Create build folder if it doesn't exist
    )

    log_success(logger, f"Archive uploaded: {archive_yt_path}")


def _resolve_build_code_dir(
    build_code_dir: Optional[Path],
    pipeline_dir: Path,
    logger: logging.Logger,
) -> Path:
    """
    Resolve build code directory path.

    Args:
        build_code_dir: Optional path to local build directory
        pipeline_dir: Path to pipeline directory
        logger: Logger instance

    Returns:
        Resolved build code directory path
    """
    if build_code_dir is None:
        # Default to pipeline_dir/.build
        build_code_dir = pipeline_dir / ".build"
        logger.debug(f"Using default build directory: {build_code_dir}")
    else:
        build_code_dir = Path(build_code_dir).resolve()
        logger.debug(f"Using specified build directory: {build_code_dir}")

    # Ensure build directory exists
    build_code_dir.mkdir(parents=True, exist_ok=True)
    return build_code_dir


def upload_code_as_archive(
    yt_client: BaseYTClient,
    build_folder: str,
    pipeline_dir: Path,
    logger: logging.Logger,
    build_code_dir: Optional[Path] = None,
    upload_modules: Optional[List[str]] = None,
    upload_paths: Optional[List[Dict[str, str]]] = None,
) -> None:
    """
    Upload code to YT as a tar archive.

    Builds code locally, creates tar archive, and uploads it to YT.

    Args:
        yt_client: YT client instance
        build_folder: YT build folder path
        pipeline_dir: Path to pipeline directory
        logger: Logger instance
        build_code_dir: Optional path to local build directory. If None, creates
                       a directory inside pipeline_dir
        upload_modules: Optional list of module names to upload
        upload_paths: Optional list of {source, target?} for local paths

    Returns:
        None
    """
    log_header(
        logger, "Code Upload", f"Tar archive mode | Build folder: {build_folder}"
    )

    # Resolve build directory path
    build_code_dir = _resolve_build_code_dir(
        build_code_dir=build_code_dir,
        pipeline_dir=pipeline_dir,
        logger=logger,
    )

    # Build code locally (including wrapper scripts for tar archive mode)
    build_dir = build_code_dir / "build"
    build_code_locally(
        build_dir=build_dir,
        pipeline_dir=pipeline_dir,
        logger=logger,
        create_wrappers=True,
        upload_modules=upload_modules,
        upload_paths=upload_paths,
    )

    # Create archive
    archive_path = build_code_dir / "code.tar.gz"
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

    log_success(logger, "Code upload completed (tar archive mode)")


def upload_all_code(
    yt_client: BaseYTClient,
    build_folder: str,
    pipeline_dir: Path,
    logger: logging.Logger,
    build_code_dir: Optional[Path] = None,
    upload_modules: Optional[List[str]] = None,
    upload_paths: Optional[List[Dict[str, str]]] = None,
) -> None:
    """
    Upload all code to YT: ytjobs package, optional custom modules/paths, and stages.

    This is the main entry point for code upload operations.
    Code is always uploaded as a tar archive.

    Args:
        yt_client: YT client instance
        build_folder: YT build folder path
        pipeline_dir: Path to pipeline directory
        logger: Logger instance
        build_code_dir: Optional path to local build directory. If None, creates
                       a directory inside pipeline_dir
        upload_modules: Optional list of module names to upload
        upload_paths: Optional list of {source, target?} for local paths

    Returns:
        None
    """
    upload_code_as_archive(
        yt_client=yt_client,
        build_folder=build_folder,
        pipeline_dir=pipeline_dir,
        logger=logger,
        build_code_dir=build_code_dir,
        upload_modules=upload_modules,
        upload_paths=upload_paths,
    )
