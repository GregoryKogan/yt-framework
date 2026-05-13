"""Frozen submit specs for YT operation entry points.

Callers build a spec and pass it to ``BaseYTClient.run_*_submit`` so the public
``run_*`` keyword surface stays available while internal code stays typed and
compact.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from yt.wrapper.schema import TableSchema

    from .operation_resources import OperationResources


def file_pairs_tuple(files: list[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    """Snapshot file path pairs into an immutable tuple."""
    return tuple((str(yt), str(local)) for yt, local in files)


def env_pairs_tuple(env: dict[str, str]) -> tuple[tuple[str, str], ...]:
    """Snapshot environment mapping (insertion order preserved)."""
    return tuple(env.items())


def extras_tuple(kwargs: dict[str, object]) -> tuple[tuple[str, object], ...]:
    """Freeze passthrough kwargs for yt.wrapper / spec builder."""
    return tuple(kwargs.items())


def docker_auth_tuple(
    docker_auth: dict[str, str] | None,
) -> tuple[tuple[str, str], ...] | None:
    """Return ``None`` or a frozen copy of ``docker_auth`` key/value pairs."""
    if docker_auth is None:
        return None
    return tuple((str(k), str(v)) for k, v in docker_auth.items())


@dataclass(frozen=True)
class MapSubmitSpec:
    """Arguments for ``run_map_submit`` / prod map submission."""

    command: object
    input_table: str
    output_table: str
    files: tuple[tuple[str, str], ...]
    resources: OperationResources
    env: tuple[tuple[str, str], ...]
    output_schema: TableSchema | None = None
    max_failed_jobs: int = 1
    docker_auth: tuple[tuple[str, str], ...] | None = None
    job: object | None = None
    append: bool = False
    extras: tuple[tuple[str, Any], ...] = ()

    def files_list(self) -> list[tuple[str, str]]:
        """Return file pairs as a mutable list for yt.wrapper APIs."""
        return list(self.files)

    def env_dict(self) -> dict[str, str]:
        """Return env entries as a mutable dict."""
        return dict(self.env)

    def extras_dict(self) -> dict[str, Any]:
        """Return frozen extras as a mutable dict."""
        return dict(self.extras)

    def docker_auth_dict(self) -> dict[str, str] | None:
        """Return docker auth as a mutable dict, or ``None`` if unset."""
        if self.docker_auth is None:
            return None
        return dict(self.docker_auth)


@dataclass(frozen=True)
class MapReduceSubmitSpec:
    """Arguments for ``run_map_reduce_submit``."""

    mapper: object
    reducer: object
    input_table: str
    output_table: str
    reduce_by: tuple[str, ...]
    files: tuple[tuple[str, str], ...]
    resources: OperationResources
    env: tuple[tuple[str, str], ...]
    sort_by: tuple[str, ...] | None = None
    output_schema: TableSchema | None = None
    max_failed_jobs: int = 1
    docker_auth: tuple[tuple[str, str], ...] | None = None
    map_job: object | None = None
    reduce_job: object | None = None
    extras: tuple[tuple[str, Any], ...] = ()

    def files_list(self) -> list[tuple[str, str]]:
        """Return file pairs as a mutable list for yt.wrapper APIs."""
        return list(self.files)

    def env_dict(self) -> dict[str, str]:
        """Return env entries as a mutable dict."""
        return dict(self.env)

    def extras_dict(self) -> dict[str, Any]:
        """Return frozen extras as a mutable dict."""
        return dict(self.extras)

    def docker_auth_dict(self) -> dict[str, str] | None:
        """Return docker auth as a mutable dict, or ``None`` if unset."""
        if self.docker_auth is None:
            return None
        return dict(self.docker_auth)

    def reduce_by_list(self) -> list[str]:
        """Return reduce-by columns as a mutable list."""
        return list(self.reduce_by)

    def sort_by_list(self) -> list[str] | None:
        """Return sort-by columns, or ``None`` if not specified."""
        if self.sort_by is None:
            return None
        return list(self.sort_by)


@dataclass(frozen=True)
class ReduceSubmitSpec:
    """Arguments for ``run_reduce_submit``."""

    reducer: object
    input_table: str
    output_table: str
    reduce_by: tuple[str, ...]
    files: tuple[tuple[str, str], ...]
    resources: OperationResources
    env: tuple[tuple[str, str], ...]
    output_schema: TableSchema | None = None
    max_failed_jobs: int = 1
    docker_auth: tuple[tuple[str, str], ...] | None = None
    job: object | None = None
    extras: tuple[tuple[str, Any], ...] = ()

    def files_list(self) -> list[tuple[str, str]]:
        """Return file pairs as a mutable list for yt.wrapper APIs."""
        return list(self.files)

    def env_dict(self) -> dict[str, str]:
        """Return env entries as a mutable dict."""
        return dict(self.env)

    def extras_dict(self) -> dict[str, Any]:
        """Return frozen extras as a mutable dict."""
        return dict(self.extras)

    def docker_auth_dict(self) -> dict[str, str] | None:
        """Return docker auth as a mutable dict, or ``None`` if unset."""
        if self.docker_auth is None:
            return None
        return dict(self.docker_auth)

    def reduce_by_list(self) -> list[str]:
        """Return reduce-by columns as a mutable list."""
        return list(self.reduce_by)


@dataclass(frozen=True)
class VanillaSubmitSpec:
    """Arguments for ``run_vanilla_submit``."""

    command: object
    files: tuple[tuple[str, str], ...]
    env: tuple[tuple[str, str], ...]
    task_name: str
    resources: OperationResources
    job: object | None = None
    max_failed_jobs: int = 1
    docker_auth: tuple[tuple[str, str], ...] | None = None
    extras: tuple[tuple[str, Any], ...] = ()

    def files_list(self) -> list[tuple[str, str]]:
        """Return file pairs as a mutable list for yt.wrapper APIs."""
        return list(self.files)

    def env_dict(self) -> dict[str, str]:
        """Return env entries as a mutable dict."""
        return dict(self.env)

    def extras_dict(self) -> dict[str, Any]:
        """Return frozen extras as a mutable dict."""
        return dict(self.extras)

    def docker_auth_dict(self) -> dict[str, str] | None:
        """Return docker auth as a mutable dict, or ``None`` if unset."""
        if self.docker_auth is None:
            return None
        return dict(self.docker_auth)
