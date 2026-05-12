"""Internal helpers for ``YTDevClient`` (cyclomatic-complexity isolation)."""

from __future__ import annotations

import importlib
import logging
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from omegaconf import DictConfig, ListConfig, OmegaConf

import yt_framework
import ytjobs
from yt_framework.yt.dev_simulator import (
    DuckDBSimulator,
    extract_output_table,
    extract_table_references,
)


class _WriteTableFn(Protocol):
    def __call__(
        self, table: str, rows: list[dict[str, Any]], *, append: bool
    ) -> None: ...


def dev_columns_from_first_row(
    rows: list[dict[str, Any]], table_path: str
) -> list[str]:
    """Column names from first row, excluding internal ``_`` keys when possible."""
    if not rows:
        msg = f"Table {table_path} is empty, cannot determine columns"
        raise ValueError(msg)
    columns = list(rows[0].keys())
    columns = [col for col in columns if not col.startswith("_")]
    if not columns:
        columns = list(rows[0].keys())
    return columns


def dev_run_yql_simulation(
    *,
    query_with_max_row_weight: str,
    dev_dir: Path,
    logger: logging.Logger,
    table_local_path: Callable[[str], Path],
    write_table: _WriteTableFn,
) -> None:
    """Execute DuckDB-backed YQL simulation (no ``self``)."""
    simulator = DuckDBSimulator(dev_dir=dev_dir, logger=logger)
    try:
        input_tables = extract_table_references(query_with_max_row_weight)
        output_table = extract_output_table(query_with_max_row_weight)
        logger.debug("Input tables: %s", input_tables)
        logger.debug("Output table: %s", output_table)
        for table_path in input_tables:
            local_path = table_local_path(table_path)
            if local_path.exists():
                simulator.load_table(table_path, local_path)
            else:
                logger.warning("Input table not found: %s", local_path)
        results, _ = simulator.execute_yql(query_with_max_row_weight)
        if output_table and results:
            write_table(output_table, results, append=False)
            logger.info("Wrote %s rows to %s", len(results), output_table)
        logger.info("✓ YQL query executed successfully")
    finally:
        simulator.close()


def _dev_append_distinct_pythonpath_root(
    pp_parts: list[str], *, package: object
) -> None:
    mod_file = getattr(package, "__file__", None)
    if not mod_file:
        return
    root = str(Path(mod_file).resolve().parent.parent)
    if root not in pp_parts:
        pp_parts.append(root)


def dev_pythonpath_entries_for_dev_env(
    pipeline_dir: Path, env_merged: dict[str, str]
) -> list[str]:
    """Ordered PYTHONPATH segments for dev subprocesses."""
    pp_parts = [str(pipeline_dir)]
    _dev_append_distinct_pythonpath_root(pp_parts, package=yt_framework)
    _dev_append_distinct_pythonpath_root(pp_parts, package=ytjobs)
    if env_merged.get("PYTHONPATH"):
        pp_parts.append(env_merged["PYTHONPATH"])
    return pp_parts


def dev_find_checkpoint_in_operations(stage_config: DictConfig) -> str | None:
    """Scan ``client.operations.*.checkpoint`` for a local checkpoint path."""
    operations = OmegaConf.select(stage_config, "client.operations")
    if not operations or not isinstance(operations, (DictConfig, dict)):
        return None
    for op_name in operations:
        checkpoint_path = (
            f"client.operations.{op_name}.checkpoint.local_checkpoint_path"
        )
        local_checkpoint = OmegaConf.select(stage_config, checkpoint_path)
        if local_checkpoint:
            return str(local_checkpoint)
    return None


def dev_find_checkpoint_in_config(stage_config: DictConfig | ListConfig) -> str | None:
    """Resolve ``local_checkpoint_path`` from stage OmegaConf."""
    if not isinstance(stage_config, DictConfig):
        return None
    local_checkpoint = OmegaConf.select(stage_config, "client.local_checkpoint_path")
    if local_checkpoint:
        return str(local_checkpoint)
    return dev_find_checkpoint_in_operations(stage_config)


def dev_try_checkpoint_from_stage_config_file(
    stage_config_path: Path,
    logger: logging.Logger,
    find_in_config: Callable[[DictConfig | ListConfig], str | None],
) -> str | None:
    """Load one ``config.yaml`` and return an existing resolved checkpoint path, if any."""
    try:
        stage_cfg = OmegaConf.load(stage_config_path)
        if not isinstance(stage_cfg, DictConfig):
            return None
        local_checkpoint = find_in_config(stage_cfg)
        if not local_checkpoint:
            return None
        checkpoint_path = Path(local_checkpoint).resolve()
        if checkpoint_path.exists():
            logger.debug("  Dev: found local_checkpoint_path: %s", checkpoint_path)
            return str(checkpoint_path)
    except Exception as e:  # noqa: BLE001
        logger.debug("  Dev: error reading %s: %s", stage_config_path, e)
    return None


def _dev_stage_config_path_if_any(stage_dir: Path) -> Path | None:
    if not stage_dir.is_dir():
        return None
    candidate = stage_dir / "config.yaml"
    return candidate if candidate.exists() else None


def dev_scan_stages_dir_for_checkpoint(
    stages_dir: Path,
    logger: logging.Logger,
    find_in_config: Callable[[DictConfig | ListConfig], str | None],
) -> str | None:
    """Best-effort scan of ``stages/*/config.yaml`` for a usable checkpoint path."""
    try:
        for stage_dir in stages_dir.iterdir():
            stage_config_path = _dev_stage_config_path_if_any(stage_dir)
            if stage_config_path is None:
                continue
            found = dev_try_checkpoint_from_stage_config_file(
                stage_config_path,
                logger,
                find_in_config,
            )
            if found:
                return found
    except Exception as e:  # noqa: BLE001
        logger.debug("  Dev: error scanning stages directory: %s", e)
    return None


def _dev_first_existing_stage_config(stages_dir: Path) -> Path | None:
    for stage_dir in stages_dir.iterdir():
        path = _dev_stage_config_path_if_any(stage_dir)
        if path is not None:
            return path
    return None


def dev_apply_first_stage_checkpoint_fallback(
    stages_dir: Path,
    env_merged: dict[str, str],
    logger: logging.Logger,
    merge_checkpoint_env: Callable[[object, dict[str, str]], None],
) -> None:
    """Set ``JOB_CONFIG_PATH`` from the first stage config when present."""
    try:
        stage_config_path = _dev_first_existing_stage_config(stages_dir)
        if stage_config_path is None:
            return
        env_merged["JOB_CONFIG_PATH"] = str(stage_config_path)
        try:
            stage_cfg = OmegaConf.load(stage_config_path)
            merge_checkpoint_env(stage_cfg, env_merged)
        except Exception as e:  # noqa: BLE001
            logger.warning("  Dev: failed to load checkpoint config: %s", e)
    except Exception as e:  # noqa: BLE001
        logger.debug("  Dev: could not setup checkpoint config: %s", e)


def _dev_regex_rewrite_build_segment(
    vanilla_job: str,
    first_path_token: str,
) -> str | None:
    yt_path_pattern = r"//[^/\s]+(?:/[^/\s]+)*/build/" + re.escape(first_path_token)
    local_command = re.sub(yt_path_pattern, first_path_token, vanilla_job)
    if local_command != vanilla_job:
        return local_command
    return None


def _dev_fallback_replace_build_segment(
    vanilla_job: str,
    build_parts: list[str],
    first_path_token: str,
    logger: logging.Logger | None,
) -> str:
    yt_full_path = "/build/".join(build_parts)
    if yt_full_path not in vanilla_job:
        return vanilla_job
    replaced = vanilla_job.replace(yt_full_path, first_path_token)
    if replaced != vanilla_job and logger is not None:
        logger.debug(
            "  Dev: converted command (fallback): %s -> %s",
            vanilla_job,
            replaced,
        )
    return replaced


def dev_rewrite_build_path_in_command(
    vanilla_job: str,
    *,
    build_split_parts: int,
    logger: logging.Logger | None = None,
) -> str:
    """Rewrite ``//…/build/…`` fragments in a dev vanilla command string."""
    if "/build/" not in vanilla_job:
        return vanilla_job
    build_parts = vanilla_job.split("/build/", 1)
    if len(build_parts) != build_split_parts:
        return vanilla_job
    local_path = build_parts[1].strip()
    tokens = local_path.split()
    first_path_token = tokens[0] if tokens else ""
    regex_out = _dev_regex_rewrite_build_segment(vanilla_job, first_path_token)
    if regex_out is not None:
        return regex_out
    return _dev_fallback_replace_build_segment(
        vanilla_job,
        build_parts,
        first_path_token,
        logger,
    )


def _dev_copy_map_output_to_table(
    *,
    proc_returncode: int,
    sandbox_output: Path,
    append: bool,
    output_table_local_path: Path,
) -> None:
    if proc_returncode != 0 or not sandbox_output.exists():
        return
    if append and output_table_local_path.exists():
        with (
            output_table_local_path.open("ab") as out,
            sandbox_output.open("rb") as sand,
        ):
            out.write(sand.read())
    else:
        shutil.copy2(sandbox_output, output_table_local_path)


def dev_run_map_subprocess(
    *,
    mapper_job: str,
    sandbox_dir: Path,
    sandbox_input: Path,
    sandbox_output: Path,
    env_merged: dict[str, str],
    logs_path: Path,
    append: bool,
    output_table_local_path: Path,
) -> tuple[int, str]:
    """Run mapper bash in sandbox; copy JSONL output when exit code is 0."""
    with (
        sandbox_input.open() as fin,
        sandbox_output.open("w") as fout,
        logs_path.open("w") as ferr,
    ):
        proc = subprocess.run(  # noqa: S603
            ["/bin/bash", "-c", mapper_job],
            stdin=fin,
            stdout=fout,
            stderr=ferr,
            env=env_merged,
            cwd=str(sandbox_dir),
            check=False,
            shell=False,
        )
    _dev_copy_map_output_to_table(
        proc_returncode=proc.returncode,
        sandbox_output=sandbox_output,
        append=append,
        output_table_local_path=output_table_local_path,
    )
    err_hint = f"Stderr written to {logs_path}" if proc.returncode != 0 else ""
    return proc.returncode, err_hint


def dev_import_ytjobs_dir() -> Path | None:
    """Return ``ytjobs`` package directory, or ``None`` if not importable."""
    yj_mod = sys.modules.get("ytjobs")
    if yj_mod is None:
        try:
            yj_mod = importlib.import_module("ytjobs")
        except ImportError:
            return None
    yj_file = getattr(yj_mod, "__file__", None)
    if not yj_file:
        return None
    return Path(yj_file).parent


def dev_resolve_ytjobs_source(local_name: str) -> Path | None:
    """Resolve ``ytjobs/…`` dependency to an on-disk file under the installed package."""
    if not local_name.startswith("ytjobs/"):
        return None
    ytjobs_dir = dev_import_ytjobs_dir()
    if ytjobs_dir is None:
        return None
    rel = local_name.replace("ytjobs/", "")
    source_file = ytjobs_dir / rel
    if source_file.exists():
        return source_file
    return None


def dev_try_upload_one_dependency(
    *,
    yt_path: str,
    local_name: str,
    sandbox_dir: Path,
    local_checkpoint_path: str | None,
    try_checkpoint: Callable[..., bool],
    try_tarball: Callable[..., bool],
    try_regular: Callable[..., bool],
    logger: logging.Logger,
) -> None:
    """Copy one dependency into the sandbox when a source is found."""
    copied = try_checkpoint(
        yt_path=yt_path,
        local_name=local_name,
        sandbox_dir=sandbox_dir,
        local_checkpoint_path=local_checkpoint_path,
    )
    if not copied:
        copied = try_tarball(
            yt_path=yt_path,
            local_name=local_name,
            sandbox_dir=sandbox_dir,
        )
    if not copied:
        copied = try_regular(local_name=local_name, sandbox_dir=sandbox_dir)
    if not copied:
        logger.debug(
            "  Dev: skipping file %s -> %s (not found locally)",
            yt_path,
            local_name,
        )


def dev_run_vanilla_subprocess(
    *,
    local_command: str,
    sandbox_dir: Path,
    env_merged: dict[str, str],
    logs_path: Path,
) -> tuple[int, str]:
    with logs_path.open("w") as ferr:
        proc = subprocess.run(  # noqa: S603
            ["/bin/bash", "-c", local_command],
            stderr=ferr,
            env=env_merged,
            cwd=str(sandbox_dir),
            check=False,
            shell=False,
        )
    err_hint = f"Output written to {logs_path}" if proc.returncode != 0 else ""
    return proc.returncode, err_hint


__all__ = [
    "dev_apply_first_stage_checkpoint_fallback",
    "dev_columns_from_first_row",
    "dev_find_checkpoint_in_config",
    "dev_import_ytjobs_dir",
    "dev_pythonpath_entries_for_dev_env",
    "dev_resolve_ytjobs_source",
    "dev_rewrite_build_path_in_command",
    "dev_run_map_subprocess",
    "dev_run_vanilla_subprocess",
    "dev_run_yql_simulation",
    "dev_scan_stages_dir_for_checkpoint",
    "dev_try_upload_one_dependency",
]
