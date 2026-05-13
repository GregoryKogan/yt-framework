"""Guardrails for import direction under ``yt_framework/operations``."""

import re
from pathlib import Path


def test_operations_sources_do_not_import_yt_framework_core() -> None:
    root = Path(__file__).resolve().parents[1] / "yt_framework" / "operations"
    forbidden = re.compile(
        r"""^\s*(from\s+yt_framework\.core(\s|\.)|import\s+yt_framework\.core)""",
    )
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for i, line in enumerate(text.splitlines(), start=1):
            if forbidden.search(line):
                violations.append(
                    f"{path.relative_to(root.parents[1])}:{i}:{line.strip()}"
                )
    assert not violations, (
        "operations must not import yt_framework.core:\n"
        + "\n".join(
            violations,
        )
    )
