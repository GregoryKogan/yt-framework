"""Tar archive + bash bootstrap helpers for command-string YT jobs.

Kept in a separate module so the feature can be reverted or disabled in one place.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging


def bootstrap_shell_run_wrapper(
    archive_name: str,
    wrapper_filename: str,
    logger: logging.Logger,
) -> str:
    """Build the inner bash snippet: extract tarball and execute a wrapper script in cwd.

    Same pattern as map tar mode (see TarArchiveDependencyBuilder._create_bootstrap_command).
    """
    logger.debug("Creating tar bootstrap for wrapper %s", wrapper_filename)
    return f"""set -e
tar -xzf {archive_name}
./{wrapper_filename}
"""


def wrap_bootstrap_as_bash_c(bootstrap_command: str) -> str:
    """Quote bootstrap for ``bash -c '...'`` (same escaping as map operation)."""
    escaped = bootstrap_command.replace("'", "'\"'\"'")
    return f"bash -c '{escaped}'"


def map_reduce_wrapper_names(
    stage_name: str,
) -> tuple[str, str]:
    """Wrapper filenames inside the tarball for map-reduce command legs."""
    return (
        f"operation_wrapper_{stage_name}_map_reduce_mapper.sh",
        f"operation_wrapper_{stage_name}_map_reduce_reducer.sh",
    )


def reduce_wrapper_name(stage_name: str) -> str:
    """Wrapper filename for reduce-only command leg."""
    return f"operation_wrapper_{stage_name}_reduce.sh"
