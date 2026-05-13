"""Guardrails for import direction under ``yt_framework/operations`` and ``yt_framework/yt``."""

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def test_operations_sources_do_not_import_yt_framework_core() -> None:
    root = _REPO_ROOT / "yt_framework" / "operations"
    forbidden = re.compile(
        r"""^\s*(from\s+yt_framework\.core(\s|\.)|import\s+yt_framework\.core)""",
    )
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for i, line in enumerate(text.splitlines(), start=1):
            if forbidden.search(line):
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{i}:{line.strip()}")
    assert not violations, (
        "operations must not import yt_framework.core:\n"
        + "\n".join(
            violations,
        )
    )


def test_operations_sources_import_yt_only_via_clients_package() -> None:
    """Operation drivers use the public client surface, not ``yt.support`` or ``yt.factory``."""
    root = _REPO_ROOT / "yt_framework" / "operations"
    violations: list[str] = []
    from_yt = re.compile(r"^\s*from\s+yt_framework\.yt\b")
    from_yt_clients = re.compile(r"^\s*from\s+yt_framework\.yt\.clients\b")
    import_yt = re.compile(r"^\s*import\s+yt_framework\.yt\b")
    for path in sorted(root.rglob("*.py")):
        for i, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            bad_from_yt = from_yt.match(line) and not from_yt_clients.match(line)
            bad_import_yt = import_yt.match(line)
            if bad_from_yt or bad_import_yt:
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{i}:{line.strip()}")
    assert not violations, (
        "operations must import YT only via yt_framework.yt.clients:\n"
        + "\n".join(violations)
    )


def test_operations_sources_do_not_import_yt_clients_client_split() -> None:
    """Keep mixins/runtime out of operation drivers (public client API only)."""
    root = _REPO_ROOT / "yt_framework" / "operations"
    forbidden = re.compile(
        r"^\s*(from\s+yt_framework\.yt\.clients\._client_split|"
        r"import\s+yt_framework\.yt\.clients\._client_split)",
    )
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        for i, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if forbidden.search(line):
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{i}:{line.strip()}")
    assert not violations, (
        "operations must not import yt_framework.yt.clients._client_split:\n"
        + "\n".join(violations)
    )


def test_yt_sources_do_not_import_core_or_operations() -> None:
    """Adapter layer must not reach up into orchestration or stage drivers."""
    root = _REPO_ROOT / "yt_framework" / "yt"
    forbidden = re.compile(
        r"^\s*(from\s+yt_framework\.(core|operations)(\s|\.)|"
        r"import\s+yt_framework\.(core|operations)(\s|$|\.))",
    )
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        for i, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if forbidden.search(line):
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{i}:{line.strip()}")
    assert not violations, (
        "yt_framework.yt must not import yt_framework.core or yt_framework.operations:\n"
        + "\n".join(violations)
    )
