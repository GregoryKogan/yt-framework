# Package layers

The library is split so **pipeline code** sits at the top, **operation drivers** sit in the middle, and **YT-facing adapters** sit closer to the foundation. [Tach](https://github.com/tach-org/tach) encodes allowed imports in `tach.toml` at the repo root: each `yt_framework.*` subtree lists `depends_on`, layer ordering, `layers_explicit_depends_on`, unused-edge detection (`exact`), and no circular first-party cycles. CI runs `tach check` and `tach check-external` against runtime dependencies in `pyproject.toml`.

**Canonical contract:** treat `tach.toml` plus this page as the source of truth for who may import whom. Other checks echo the same rules in different forms (see below).

## Direction of imports

Roughly:

1. **Foundation** (`yt_framework.utils`, `yt_framework.job_command`, `yt_framework.typed_jobs`, and the mostly empty `yt_framework` namespace) — must not import `core`, `operations`, or `yt`.
2. **`yt_framework.yt`** — factory and package `__init__` only depend on **`yt_framework.yt.clients`**.
3. **`yt_framework.yt.support`** — max row weight, dev simulator, prod/dev runtime helpers, secure-env splitting, and shared `OperationResources` dataclass. Depends on `yt_framework` (for example `yt_framework._layout` for PYTHONPATH roots) and may load `ytjobs` dynamically where needed. Nothing here may import `yt_framework.yt.clients` or the pipeline layers above YT.
4. **`yt_framework.yt.clients`** — `BaseYTClient`, dev/prod clients, YQL request types under `clients.yql`, mixins under `clients._client_split`, and the public operation specs. Depends on `support`, `job_command`, and `utils`. Must not import `yt_framework.contracts` (contracts depend on this client surface instead).
5. **`yt_framework.contracts`** — `StageDependencies` and `StageContext` for stage injection. Depends on **`yt_framework.yt.clients`** (for the `BaseYTClient` type used in the protocol). Must not import `core` or `operations`. Lets `core` depend on shared stage types without importing the whole `operations` package for that alone.
6. **`yt_framework.operations`** — map/vanilla/map-reduce drivers, upload, S3 helpers. Declares **`yt_framework.yt.clients`**, **`yt_framework.contracts`**, and finer Tach modules under `operations.command_ops`, `operations.common`, and `operations._internal` where those subtrees have their own `depends_on`. Must **not** import `yt_framework.core`. Type-only imports still count toward Tach (`ignore_type_checking_imports = false`).
7. **`yt_framework.core`** — `BasePipeline`, stage discovery/registry, `BaseStage`, concrete `PipelineStageDependencies`. Imports `operations`, `contracts`, `utils`, `yt` (factory entry), and `yt.clients` for types used by the pipeline.

`yt_framework.operations.stage_contracts` remains a **thin re-export** of `yt_framework.contracts` for older import paths.

## Jobs package

`ytjobs` stays on the job side of the boundary: it must not import `yt_framework` (same `tach.toml` rules).

## Checks beyond Tach

Pre-commit also runs strict BasedPyright, Ruff, Xenon, Vulture, and small repo policies (file length, directory width, binding-word limits) described in `CONTRIBUTING.md` at the repository root.

`tests/test_architecture_boundaries.py` applies a few **line-based** greps over `yt_framework/operations` and `yt_framework/yt`. That overlaps Tach for some edges (for example `operations` must not import `core`). The duplication is intentional: Tach owns the full graph, while the tests fail with filenames and lines when someone bypasses the usual import layout. If you change boundaries, update **both** `tach.toml` and those tests when the rule is still something you want to guarantee in prose-friendly form.

## Thin `core` orchestration

Pipeline orchestration is split across `pipeline.py`, `pipeline_cli.py`, `pipeline_config.py`, registry, discovery, and dependencies so a single file stays under the repo policy limit (`[tool.yt_framework.pre_commit.max_file_lines]` in `pyproject.toml`, currently 550 lines for `yt_framework` and `ytjobs`). Xenon (pre-commit) also discourages letting any one module absorb the whole flow.
