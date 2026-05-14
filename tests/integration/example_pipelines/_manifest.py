"""Load and validate ``examples/manifest.yaml``."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import yaml

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
_MANIFEST_PATH: Final[Path] = _REPO_ROOT / "examples" / "manifest.yaml"
_EXAMPLES_DIR: Final[Path] = _REPO_ROOT / "examples"


@dataclass(frozen=True)
class ExamplePipelineSpec:
    """One catalogued example tree under ``examples/<slug>/``."""

    slug: str
    ci_tier: str
    requirements: tuple[str, ...]
    commands: tuple[tuple[str, ...], ...]


def repo_root() -> Path:
    """Repository root (directory containing ``examples/``)."""
    return _REPO_ROOT


def manifest_path() -> Path:
    return _MANIFEST_PATH


def examples_dir() -> Path:
    return _EXAMPLES_DIR


def discover_pipeline_dirs() -> frozenset[str]:
    """Slugs under ``examples/*/`` that contain ``pipeline.py``."""
    slugs: set[str] = set()
    if not _EXAMPLES_DIR.is_dir():
        return frozenset()
    for child in _EXAMPLES_DIR.iterdir():
        if child.is_dir() and (child / "pipeline.py").is_file():
            slugs.add(child.name)
    return frozenset(slugs)


def load_manifest_specs() -> tuple[ExamplePipelineSpec, ...]:
    raw = yaml.safe_load(_MANIFEST_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = "manifest root must be a mapping"
        raise TypeError(msg)
    entries = raw.get("pipelines")
    if not isinstance(entries, list):
        msg = "manifest.pipelines must be a list"
        raise TypeError(msg)
    specs: list[ExamplePipelineSpec] = []
    for i, item in enumerate(entries):
        if not isinstance(item, dict):
            msg = f"manifest.pipelines[{i}] must be a mapping"
            raise TypeError(msg)
        slug = _req_str(item, "slug", i)
        ci_tier = _req_str(item, "ci_tier", i)
        if ci_tier not in ("always", "cluster_optional", "manual"):
            msg = f"manifest.pipelines[{i}].ci_tier must be always, cluster_optional, or manual"
            raise ValueError(msg)
        reqs_raw = item.get("requirements", [])
        if not isinstance(reqs_raw, list):
            msg = f"manifest.pipelines[{i}].requirements must be a list of strings"
            raise TypeError(msg)
        if not all(isinstance(x, str) for x in reqs_raw):
            msg = f"manifest.pipelines[{i}].requirements must be a list of strings"
            raise ValueError(msg)
        cmds_raw = item.get("commands")
        if not isinstance(cmds_raw, list):
            msg = f"manifest.pipelines[{i}].commands must be a non-empty list"
            raise TypeError(msg)
        if not cmds_raw:
            msg = f"manifest.pipelines[{i}].commands must be a non-empty list"
            raise ValueError(msg)
        commands: list[tuple[str, ...]] = []
        for j, cmd in enumerate(cmds_raw):
            if not isinstance(cmd, list):
                msg = f"manifest.pipelines[{i}].commands[{j}] must be a non-empty list of strings"
                raise TypeError(msg)
            if not cmd:
                msg = f"manifest.pipelines[{i}].commands[{j}] must be a non-empty list of strings"
                raise ValueError(msg)
            if not all(isinstance(x, str) for x in cmd):
                msg = f"manifest.pipelines[{i}].commands[{j}] must be a non-empty list of strings"
                raise TypeError(msg)
            commands.append(tuple(cmd))
        specs.append(
            ExamplePipelineSpec(
                slug=slug,
                ci_tier=ci_tier,
                requirements=tuple(reqs_raw),
                commands=tuple(commands),
            )
        )
    return tuple(specs)


def _req_str(item: dict[str, Any], key: str, index: int) -> str:
    v = item.get(key)
    if not isinstance(v, str):
        msg = f"manifest.pipelines[{index}].{key} must be a non-empty string"
        raise TypeError(msg)
    if not v.strip():
        msg = f"manifest.pipelines[{index}].{key} must be a non-empty string"
        raise ValueError(msg)
    return v.strip()
