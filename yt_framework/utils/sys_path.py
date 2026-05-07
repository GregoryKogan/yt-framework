"""sys.path helpers for stage driver-side imports.

Stages that import from their own ``src/`` directory should use
:func:`stage_src_path` instead of manually manipulating ``sys.path``.
This ensures each stage's ``src/`` is at the front of the path for the
duration of the import block and is removed cleanly afterwards,
preventing cross-stage module shadowing in multi-stage pipelines.

Example::

    from yt_framework.utils.sys_path import stage_src_path

    class MyStage(BaseStage):
        def run(self, debug):
            with stage_src_path(self.stage_dir):
                from my_mapper import MyMapper
                from my_reducer import MyReducer
            ...
"""

import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def stage_src_path(stage_dir: Path) -> Generator[None, None, None]:
    """Context manager that prepends ``<stage_dir>/src`` to ``sys.path``.

    Re-inserts the directory at position 0 even if it was already present
    (guaranteeing priority), and removes it on exit to avoid polluting the
    path for subsequent stages running in the same process.

    Args:
        stage_dir: Path to the stage directory (``self.stage_dir`` from
            :class:`~yt_framework.core.stage.BaseStage`).

    """
    src = str(stage_dir / "src")
    if src in sys.path:
        sys.path.remove(src)
    sys.path.insert(0, src)
    try:
        yield
    finally:
        if src in sys.path:
            sys.path.remove(src)
