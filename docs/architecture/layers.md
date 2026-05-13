# Package layers

The library is split so **pipeline code** sits at the top, **operation drivers** sit in the middle, and **YT-facing adapters** sit closer to the foundation. [Tach](https://github.com/tach-org/tach) encodes allowed imports in `tach.toml` at the repo root: each `yt_framework.*` subtree lists `depends_on`, layer ordering, `layers_explicit_depends_on`, unused-edge detection (`exact`), and no circular first-party cycles. CI runs `tach check` and `tach check-external` against runtime dependencies in `pyproject.toml`.

## Direction of imports

Roughly:

1. **Foundation** (`yt_framework.utils`, `yt_framework.job_command`, `yt_framework.typed_jobs`, and the empty `yt_framework` namespace) — must not import `core`, `operations`, or `yt`.
2. **`yt_framework.yt`** — factory, `yt_framework.yt.clients` (ports and specs), dev/prod clients, mixins, and runtime helpers. Operation drivers should import **ports and specs** from `yt_framework.yt.clients.*` instead of reaching into mixins when possible.
3. **`yt_framework.operations`** — map/vanilla/map-reduce drivers, upload, S3 helpers. Depends on `yt_framework.yt` (and `job_command`, `utils`, `ytjobs.s3` per `tach.toml`). Must **not** import `yt_framework.core` (enforced in tests; Tach uses `ignore_type_checking_imports = false` so type-only imports count too).
4. **`yt_framework.core`** — `BasePipeline`, stage discovery/registry, `BaseStage`, concrete `PipelineStageDependencies`. Imports `operations`, `utils`, and `yt`.

`StageDependencies`, `StageContext`, and related injection types live in `yt_framework.operations.stage_contracts` so operation helpers do not import `core` just for those types.

## Jobs package

`ytjobs` stays on the job side of the boundary: it must not import `yt_framework` (same `tach.toml` rules).

## Checks beyond Tach

Pre-commit also runs strict BasedPyright, Ruff, Xenon, Vulture, and small repo policies (file length, directory width, binding-word limits) described in `CONTRIBUTING.md` at the repository root.
