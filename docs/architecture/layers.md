# Package layers

The library is split so **pipeline code** sits at the top, **operation drivers** sit in the middle, and **YT-facing adapters** sit closer to the foundation. [Tach](https://github.com/tach-org/tach) encodes allowed imports in `tach.toml` at the repo root: each `yt_framework.*` subtree lists `depends_on`, layer ordering, `layers_explicit_depends_on`, unused-edge detection (`exact`), and no circular first-party cycles. CI runs `tach check` and `tach check-external` against runtime dependencies in `pyproject.toml`.

## Direction of imports

Roughly:

1. **Foundation** (`yt_framework.utils`, `yt_framework.job_command`, `yt_framework.typed_jobs`, and the empty `yt_framework` namespace) — must not import `core`, `operations`, or `yt`.
2. **`yt_framework.yt`** — factory and package `__init__` only depend on **`yt_framework.yt.clients`**.
3. **`yt_framework.yt.support`** — max row weight, dev simulator, prod/dev runtime helpers, secure-env splitting, and shared `OperationResources` dataclass. Depends only on `yt_framework` and `ytjobs` (plus third-party libs). Nothing here may import `yt_framework.yt.clients` or the pipeline layers above YT.
4. **`yt_framework.yt.clients`** — `BaseYTClient`, dev/prod clients, YQL request types under `clients.yql`, mixins under `clients._client_split`, and the public operation specs. Depends on `support`, `job_command`, and `utils`.
5. **`yt_framework.operations`** — map/vanilla/map-reduce drivers, upload, S3 helpers. Declares **`yt_framework.yt.clients`** only (not `yt.support` or `yt.factory`). Must **not** import `yt_framework.core` (also covered by `tests/test_architecture_boundaries.py`). Type-only imports still count toward Tach (`ignore_type_checking_imports = false`).
6. **`yt_framework.core`** — `BasePipeline`, stage discovery/registry, `BaseStage`, concrete `PipelineStageDependencies`. Imports `operations`, `utils`, `yt` (factory entry), and `yt.clients` for types used by the pipeline.

`StageDependencies`, `StageContext`, and related injection types live in `yt_framework.operations.stage_contracts` so operation helpers do not import `core` just for those types.

## Jobs package

`ytjobs` stays on the job side of the boundary: it must not import `yt_framework` (same `tach.toml` rules).

## Checks beyond Tach

Pre-commit also runs strict BasedPyright, Ruff, Xenon, Vulture, and small repo policies (file length, directory width, binding-word limits) described in `CONTRIBUTING.md` at the repository root. `tests/test_architecture_boundaries.py` adds a few grep-style rules (for example: `operations` imports YT only via `yt_framework.yt.clients`; `yt` must not import `core` or `operations`).
