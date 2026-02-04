"""
Dependency Management
====================

Build dependency file lists for YT map operations.
Dependencies are files that need to be mounted in the YT job sandbox.
"""

import logging
from pathlib import Path
from typing import List, Tuple, Optional


def _get_ytjobs_dir() -> Path:
    """Get ytjobs package directory dynamically."""
    import ytjobs

    return Path(ytjobs.__file__).parent


def build_stage_dependencies(
    build_folder: str,
    stage_dir: Path,
    logger: logging.Logger,
) -> List[Tuple[str, str]]:
    """
    Build dependency list for a single stage.

    Includes:
    - config.yaml (if exists locally)
    - All .py files from src/ directory

    Args:
        build_folder: YT build folder path
        stage_dir: Path to stage directory (e.g., stages/run_map/)
        logger: Logger instance

    Returns:
        List of (yt_path, local_path) tuples
    """
    stage_dir_name = stage_dir.name
    dependency_files: List[Tuple[str, str]] = []

    # Add config.yaml if it exists locally
    config_local_path = stage_dir / "config.yaml"
    if config_local_path.exists():
        config_yt_path = f"{build_folder}/stages/{stage_dir_name}/config.yaml"
        # Mount config.yaml at stages/{stage_dir_name}/config.yaml to match directory structure
        config_local_name = f"stages/{stage_dir_name}/config.yaml"
        dependency_files.append((config_yt_path, config_local_name))
        logger.debug(f"  Added config: {config_local_name}")

    # Add all Python files from src/ directory
    src_dir = stage_dir / "src"
    if src_dir.exists():
        for py_file in src_dir.rglob("*.py"):
            rel_path = py_file.relative_to(src_dir)
            yt_path = f"{build_folder}/stages/{stage_dir_name}/src/{rel_path}".replace(
                "\\", "/"
            )
            local_path = f"stages/{stage_dir_name}/src/{rel_path}".replace("\\", "/")
            dependency_files.append((yt_path, local_path))
            logger.debug(f"  Added stage file: {local_path}")

    logger.info(f"Stage dependencies: {len(dependency_files)} files")
    return dependency_files


def build_ytjobs_dependencies(
    build_folder: str,
    logger: logging.Logger,
) -> List[Tuple[str, str]]:
    """
    Build dependency list for ytjobs package.

    Includes all .py files from ytjobs/ directory.

    Args:
        build_folder: YT build folder path
        logger: Logger instance

    Returns:
        List of (yt_path, local_path) tuples
    """
    ytjobs_dir = _get_ytjobs_dir()
    dependency_files: List[Tuple[str, str]] = []

    for file in ytjobs_dir.rglob("*.py"):
        rel_path = file.relative_to(ytjobs_dir)
        yt_path = f"{build_folder}/ytjobs/{rel_path}".replace("\\", "/")
        local_path = f"ytjobs/{rel_path}".replace("\\", "/")
        dependency_files.append((yt_path, local_path))

    logger.info(f"Ytjobs dependencies: {len(dependency_files)} files")
    return dependency_files


def add_checkpoint(
    dependencies: List[Tuple[str, str]],
    model_name: Optional[str],
    checkpoint_base: Optional[str],
    logger: logging.Logger,
) -> List[Tuple[str, str]]:
    """
    Add checkpoint file to dependencies if configured.

    Args:
        dependencies: List of (yt_path, local_path) tuples
        model_name: Optional model name for checkpoint
        checkpoint_base: Optional checkpoint base path in YT
        logger: Logger instance

    Returns:
        Updated dependency list (new list with checkpoint added, or same list)
    """
    if model_name and checkpoint_base:
        checkpoint_file_path = f"{checkpoint_base}/{model_name}"
        # Create new list to avoid mutating input
        updated_files = dependencies + [(checkpoint_file_path, model_name)]
        logger.info(
            f"✓ Checkpoint will be mounted: {checkpoint_file_path} → {model_name}"
        )
        return updated_files
    elif model_name:
        logger.warning(
            f"model_name is set ({model_name}) but checkpoint_base is not configured. "
            f"Checkpoint will not be mounted - model may download from internet."
        )
    elif checkpoint_base:
        logger.debug(
            "checkpoint_base is set but no model_name specified - checkpoint mounting skipped"
        )

    return dependencies


def build_vanilla_dependencies(
    build_folder: str,
    stage_dir: Path,
    model_name: Optional[str],
    checkpoint_base: Optional[str],
    logger: logging.Logger,
) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Build complete dependency list for a vanilla operation.

    Combines:
    - Stage files (config + src/)
    - Ytjobs package

    Args:
        build_folder: YT build folder path
        stage_dir: Path to stage directory
        model_name: Optional model name for checkpoint
        checkpoint_base: Optional checkpoint base path in YT
        logger: Logger instance

    Returns:
        Tuple of (script_path, dependency_files)
        - script_path: Path to vanilla.py in YT
        - dependency_files: Complete list of dependencies
    """
    stage_dir_name = stage_dir.name
    script_path = f"{build_folder}/stages/{stage_dir_name}/src/vanilla.py"

    # Build stage dependencies
    stage_deps = build_stage_dependencies(
        build_folder=build_folder,
        stage_dir=stage_dir,
        logger=logger,
    )

    # Build ytjobs dependencies
    ytjobs_deps = build_ytjobs_dependencies(
        build_folder=build_folder,
        logger=logger,
    )

    # Add checkpoint if configured
    all_deps = add_checkpoint(
        dependencies=stage_deps + ytjobs_deps,
        model_name=model_name,
        checkpoint_base=checkpoint_base,
        logger=logger,
    )

    logger.info(f"Total dependencies: {len(all_deps)} files")
    return script_path, all_deps


def build_map_dependencies(
    build_folder: str,
    stage_dir: Path,
    model_name: Optional[str],
    checkpoint_base: Optional[str],
    logger: logging.Logger,
) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Build complete dependency list for a map operation.

    Combines:
    - Stage files (config + src/)
    - Ytjobs package
    - Checkpoint (if configured)

    Args:
        build_folder: YT build folder path
        stage_dir: Path to stage directory
        model_name: Optional model name for checkpoint
        checkpoint_base: Optional checkpoint base path in YT
        logger: Logger instance

    Returns:
        Tuple of (mapper_path, dependency_files)
        - mapper_path: Path to mapper.py in YT
        - dependency_files: Complete list of dependencies
    """
    stage_dir_name = stage_dir.name
    mapper_path = f"{build_folder}/stages/{stage_dir_name}/src/mapper.py"

    # Build stage dependencies
    stage_deps = build_stage_dependencies(
        build_folder=build_folder,
        stage_dir=stage_dir,
        logger=logger,
    )

    # Build ytjobs dependencies
    ytjobs_deps = build_ytjobs_dependencies(
        build_folder=build_folder,
        logger=logger,
    )

    # Combine dependencies
    all_deps = stage_deps + ytjobs_deps

    # Add checkpoint if configured
    all_deps = add_checkpoint(
        dependencies=all_deps,
        model_name=model_name,
        checkpoint_base=checkpoint_base,
        logger=logger,
    )

    logger.info(f"Total dependencies: {len(all_deps)} files")
    return mapper_path, all_deps
