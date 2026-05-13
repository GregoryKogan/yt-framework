"""Upload pipeline: wrappers, local build orchestration, and archive upload."""

# pyright: reportPrivateUsage=false

import logging
import shutil
import tarfile
from pathlib import Path

from yt_framework.operations._internal.upload_helpers import (
    _BUILD_CODE_DIR,
    _bash_wrapper_script_body,
    _copy_module_to_build_dir,
    _copy_path_to_build_dir,
    _copy_stage_to_build_dir,
    _copy_ytjobs_to_build_dir,
    _create_unified_wrapper_script,
    _resolve_map_reduce_command_scripts,
    _resolve_reduce_command_script,
    _resolve_upload_target,
    _validate_upload_config,
    _write_wrapper_file,
)
from yt_framework.utils import log_header, log_success
from yt_framework.yt.client_base import BaseYTClient


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
        stage_name,
        m_rel,
        "map-reduce mapper (command mode)",
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
            stage_name,
            r_rel,
            "map-reduce reducer (command mode)",
        )
        _write_wrapper_file(
            build_dir,
            f"operation_wrapper_{stage_name}_map_reduce_reducer.sh",
            body_r,
            logger,
        )
    else:
        logger.warning(
            "Stage %s: map_reduce_mapper wrapper created but no reducer script — map-reduce command mode will fail until job.map_reduce_command.reducer_script is set",
            stage_name,
        )


def _create_reduce_command_wrapper(
    stage_name: str,
    stage_dir: Path,
    build_dir: Path,
    logger: logging.Logger,
) -> None:
    """Create the reduce-only shell wrapper when ``tar_command_bootstrap`` is enabled."""
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


def _stage_has_mapper_entrypoints(src_dir: Path) -> bool:
    if (src_dir / "mapper.py").is_file():
        return True
    return bool(list(src_dir.glob("partition_*.py")))


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

    has_mapper = _stage_has_mapper_entrypoints(src_dir)
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


def _copy_all_upload_modules(
    *,
    build_dir: Path,
    upload_modules: list[str] | None,
    logger: logging.Logger,
) -> int:
    total = 0
    for mod in upload_modules or []:
        top_level = mod.split(".", maxsplit=1)[0]
        total += _copy_module_to_build_dir(
            module_name=mod,
            target_dir=build_dir / top_level,
            logger=logger,
        )
    return total


def _copy_all_upload_paths(
    *,
    build_dir: Path,
    upload_paths: list[dict[str, str]] | None,
    pipeline_dir: Path,
    logger: logging.Logger,
) -> int:
    total = 0
    for entry in upload_paths or []:
        source = entry["source"]
        target = _resolve_upload_target(
            source=source,
            target=entry.get("target"),
            _pipeline_dir=pipeline_dir,
        )
        total += _copy_path_to_build_dir(
            source_path=source,
            target_name=target,
            build_dir=build_dir,
            pipeline_dir=pipeline_dir,
            logger=logger,
        )
    return total


def _copy_all_stages_to_build(
    *,
    build_dir: Path,
    stages_dir: Path,
    logger: logging.Logger,
) -> tuple[int, list[tuple[str, Path]]]:
    stage_files = 0
    stage_dirs_list: list[tuple[str, Path]] = []
    for stage_dir in stages_dir.iterdir():
        if not stage_dir.is_dir() or not (stage_dir / "src").exists():
            continue
        stage_dirs_list.append((stage_dir.name, stage_dir))
        stage_files += _copy_stage_to_build_dir(
            build_dir=build_dir,
            stage_dir=stage_dir,
            logger=logger,
        )
    return stage_files, stage_dirs_list


def build_code_locally(
    build_dir: Path,
    pipeline_dir: Path,
    logger: logging.Logger,
    create_wrappers: bool = False,  # noqa: FBT001,FBT002
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

    ytjobs_files = _copy_ytjobs_to_build_dir(
        build_dir=build_dir,
        logger=logger,
    )

    module_files = _copy_all_upload_modules(
        build_dir=build_dir,
        upload_modules=upload_modules,
        logger=logger,
    )

    path_files = _copy_all_upload_paths(
        build_dir=build_dir,
        upload_paths=upload_paths,
        pipeline_dir=pipeline_dir,
        logger=logger,
    )

    # Copy all stages
    stages_dir = pipeline_dir / "stages"
    stage_files, stage_dirs_list = _copy_all_stages_to_build(
        build_dir=build_dir,
        stages_dir=stages_dir,
        logger=logger,
    )

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
        logger,
        "Code Upload",
        f"Tar archive mode | Build folder: {build_folder}",
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
