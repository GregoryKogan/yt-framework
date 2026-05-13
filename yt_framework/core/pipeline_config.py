"""OmegaConf normalization helpers for pipeline and upload configuration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from omegaconf import DictConfig, ListConfig, OmegaConf


def _single_nonempty_module_name(raw: str) -> list[str]:
    s = raw.strip()
    return [s] if s else []


def _upload_modules_from_sequence(
    raw: list[object] | tuple[object, ...] | ListConfig,
) -> list[str]:
    return [str(m).strip() for m in raw if str(m).strip()]


def normalize_upload_modules(raw: object) -> list[str]:
    """Normalize upload_modules config: accept list, tuple, or single string."""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, ListConfig)):
        return _upload_modules_from_sequence(raw)
    if isinstance(raw, str):
        return _single_nonempty_module_name(raw)
    msg = "upload_modules must be a list of module names or a single string."
    raise ValueError(msg)


def _coerce_upload_path_mapping(idx: int, element: object) -> dict[str, str]:
    item: object = element
    if isinstance(item, DictConfig):
        item = OmegaConf.to_container(item, resolve=True)
    if not isinstance(item, Mapping):
        msg = (
            f"upload_paths[{idx}] must be a mapping with at least a 'source' key, "
            f"got {type(item).__name__!r}."
        )
        raise TypeError(msg)
    if "source" not in item:
        msg = f"upload_paths[{idx}] is missing required 'source' key."
        raise ValueError(msg)
    return {k: str(v) for k, v in item.items()}


def normalize_upload_paths(raw: object) -> list[dict[str, str]]:
    """Normalize upload_paths config: must be a list of {source, target?} mappings."""
    if raw is None:
        return []

    if not isinstance(raw, (list, tuple, ListConfig)):
        msg = "upload_paths must be a list of {source, target?} dicts."
        raise TypeError(msg)

    return [_coerce_upload_path_mapping(i, el) for i, el in enumerate(raw)]


def yt_mode_from_pipeline_config(raw: object) -> Literal["prod", "dev"] | None:
    """Coerce ``pipeline.mode`` to a literal prod/dev or None (caller may default)."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if s == "prod":
        return "prod"
    if s == "dev":
        return "dev"
    msg = f"pipeline.mode must be 'prod' or 'dev', got {raw!r}"
    raise ValueError(msg)


def pickling_dict_from_config(pickling_cfg: object) -> dict[str, Any]:
    """Return a plain dict for ``create_yt_client(..., pickling=...)``."""
    if not pickling_cfg:
        return {}
    raw = OmegaConf.to_container(pickling_cfg, resolve=True)
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    msg = (
        "pipeline.pickling must be a mapping-compatible config, "
        f"got {type(raw).__name__}"
    )
    raise TypeError(msg)


def _enabled_from_sequence(
    enabled: list[object] | tuple[object, ...] | ListConfig,
) -> list[str]:
    return [str(x) for x in enabled]


def enabled_stage_names(enabled: object) -> list[str]:
    """Normalize ``stages.enabled_stages`` to a list of directory names."""
    if enabled is None:
        return []
    if isinstance(enabled, (list, tuple, ListConfig)):
        return _enabled_from_sequence(enabled)
    if isinstance(enabled, str):
        s = enabled.strip()
        return [s] if s else []
    return [str(enabled)]
