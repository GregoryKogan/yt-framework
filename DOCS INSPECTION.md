# Documentation inspection report

```yaml
scope: /Users/gregorykogan/Desktop/yt-framework
inspection_type: static_review_code_vs_docs
excludes:
  - .pytest_cache/**
  - docs/_build/**
constraints:
  - no_runtime_sphinx_run_in_this_audit
  - prior_builds_reported_duplicate_ids_and_myst_xrefs_see_section_build_health
generated_for: llm_consumption
```

## Executive summary

User-facing Python APIs and YAML/config surfaces in `yt_framework` and `ytjobs` exceed what narrative guides and `docs/reference/api.md` systematically cover. Several **exported** operations helpers have **zero** prose documentation and **no** `automodule` section. **Tokenizer artifact** configuration and **sort** operations are prominent gaps. **Environment variables** set only in job sandboxes (`TOKENIZER_*`, `YT_STAGE_NAME`) are largely undocumented. **CONTRIBUTING.md** and **docs/index.md** reference sections lag behind the newer **YT jobs** reference page. Some Sphinx/MyST issues (broken cross-refs, duplicate section IDs, directive warnings) indicate **stale or fragile** doc sources—not necessarily unused files.

---

## Taxonomy (use when triaging)

| type            | meaning                                                                 |
|-----------------|-------------------------------------------------------------------------|
| `coverage_gap`  | Feature exists for users; no or insufficient end-user documentation     |
| `api_gap`       | Public or exported symbol not in Sphinx API reference where expected    |
| `stale`         | Doc text or index claims completeness but reality drifted               |
| `fragile_build` | Likely Sphinx/docutils warning or error under normal build              |
| `orphan_nav`    | Page exists but weak discoverability from main landing paths            |

---

## Severity-ranked findings

### HIGH — `coverage_gap` + `api_gap`

1. **`run_sort` / `client.operations.sort`**
   - **Code**: `/Users/gregorykogan/Desktop/yt-framework/yt_framework/operations/sort.py` — public `run_sort`; exported from `/Users/gregorykogan/Desktop/yt-framework/yt_framework/operations/__init__.py` (`__all__` includes `"run_sort"`).
   - **Docs**: No matches under `/Users/gregorykogan/Desktop/yt-framework/docs/` for `run_sort`, `operations.sort`, or sort-operation narrative. Not in `/Users/gregorykogan/Desktop/yt-framework/docs/operations/index.md` toctree or comparison tables.
   - **API reference**: `/Users/gregorykogan/Desktop/yt-framework/docs/reference/api.md` has **no** `.. automodule:: yt_framework.operations.sort`.
   - **Recommendation**: Add operations guide section (or page), YAML examples, and `automodule` for `yt_framework.operations.sort`.

2. **`tokenizer_artifact` subsystem**
   - **Code**: `/Users/gregorykogan/Desktop/yt-framework/yt_framework/operations/tokenizer_artifact.py`; wired from `/Users/gregorykogan/Desktop/yt-framework/yt_framework/operations/common.py`, `/Users/gregorykogan/Desktop/yt-framework/yt_framework/operations/dependency_strategy.py`, map/map_reduce/vanilla operation paths; `init_tokenizer_artifact_directory` is **exported** in `yt_framework.operations.__all__`.
   - **Docs**: No matches in `/Users/gregorykogan/Desktop/yt-framework/docs/` for `tokenizer_artifact`, `TOKENIZER_ARTIFACT`, or `init_tokenizer_artifact_directory`. `/Users/gregorykogan/Desktop/yt-framework/docs/advanced/checkpoints.md` discusses `job.tokenizer_name` as a **second checkpoint-style file** but **not** the `tokenizer_artifact:` YAML block, tarball layout, or upload semantics.
   - **API reference**: No `automodule` for `yt_framework.operations.tokenizer_artifact`.
   - **Recommendation**: Dedicated subsection (checkpoints vs tokenizer tarball artifact), config schema reference, env vars (see below), and optional API autodoc.

3. **`run_map_reduce` / `run_reduce` API reference**
   - **Code**: `/Users/gregorykogan/Desktop/yt-framework/yt_framework/operations/map_reduce.py` — primary entry points for TypedJob and command-mode map-reduce/reduce; heavily referenced from narrative `/Users/gregorykogan/Desktop/yt-framework/docs/operations/map-reduce-typed-jobs.md` and `/Users/gregorykogan/Desktop/yt-framework/docs/operations/command-mode-map-reduce.md`.
   - **Docs narrative**: Covered conceptually.
   - **API reference**: `/Users/gregorykogan/Desktop/yt-framework/docs/reference/api.md` has **no** `.. automodule:: yt_framework.operations.map_reduce` (unlike `map`, `vanilla`, `s3`, `table`, `checkpoint`).
   - **Recommendation**: Add `automodule:: yt_framework.operations.map_reduce` (or curated `:members:`) so parameters and edge cases match source.

4. **`yt_framework.typed_jobs` API reference**
   - **Code**: `/Users/gregorykogan/Desktop/yt-framework/yt_framework/typed_jobs/stage_bootstrap.py` — `StageBootstrapTypedJob` is the public export in `/Users/gregorykogan/Desktop/yt-framework/yt_framework/typed_jobs/__init__.py`.
   - **Docs**: Explained in `/Users/gregorykogan/Desktop/yt-framework/docs/operations/map-reduce-typed-jobs.md`.
   - **API reference**: No `automodule:: yt_framework.typed_jobs` in `/Users/gregorykogan/Desktop/yt-framework/docs/reference/api.md`.
   - **Recommendation**: Add automodule for `yt_framework.typed_jobs` or `stage_bootstrap` with `:members:` focused on `StageBootstrapTypedJob`.

5. **Table helpers vs client table API**
   - **Code**: `/Users/gregorykogan/Desktop/yt-framework/yt_framework/operations/table.py` — `get_row_count`, `read_table`, `download_table`; exported from `yt_framework.operations`.
   - **Docs**: Autodoc exists in `/Users/gregorykogan/Desktop/yt-framework/docs/reference/api.md`. Prose guides use `self.deps.yt_client.read_table(...)` in examples; **no** dedicated “Table operations” guide under `/Users/gregorykogan/Desktop/yt-framework/docs/operations/` and **not** listed in `/Users/gregorykogan/Desktop/yt-framework/docs/operations/index.md` operation cards (unlike Map, Vanilla, YQL, S3).
   - **Recommendation**: Either add a short operations page clarifying when to use helpers vs `yt_client`, or explicitly document table helpers inside `pipelines-and-stages` / `yql` / troubleshooting.

### MEDIUM — `coverage_gap` / `stale`

6. **Upload and dependency builder exports**
   - **Code**: `/Users/gregorykogan/Desktop/yt-framework/yt_framework/operations/__init__.py` exports `upload_all_code`, `build_stage_dependencies`, `build_ytjobs_dependencies`, `build_map_dependencies`, `add_checkpoint`, `build_environment`, `prepare_docker_auth`.
   - **Docs**: `/Users/gregorykogan/Desktop/yt-framework/docs/advanced/code-upload.md` describes behavior at a high level; **no** doc matches for `upload_all_code`, `build_stage_dependencies`, or `build_ytjobs_dependencies` under `/Users/gregorykogan/Desktop/yt-framework/docs/`.
   - **API reference**: No automodule for `yt_framework.operations.upload`, `dependencies`, or `common`.
   - **Recommendation**: Extend code-upload doc with “programmatic API” pointers or add API sections for advanced integrators.

7. **Core `StageDependencies` protocol**
   - **Code**: `/Users/gregorykogan/Desktop/yt-framework/yt_framework/core/dependencies.py` — user-relevant for understanding `self.deps`.
   - **API reference**: Not autodoc’d in `/Users/gregorykogan/Desktop/yt-framework/docs/reference/api.md`.
   - **Recommendation**: Optional automodule or a short subsection in `/Users/gregorykogan/Desktop/yt-framework/docs/pipelines-and-stages.md`.

8. **`docs/advanced/multiple-operations.md` scope**
   - **Content**: Dominated by `run_map` + `run_vanilla` patterns (`grep` shows no `run_map_reduce`, YQL client methods, or `run_sort`).
   - **Gap**: Title promises “multiple operations” generally; YQL-heavy and map-reduce chaining are underrepresented vs real `yt_framework.operations` surface.
   - **Recommendation**: Add at least one YQL-in-stage example and cross-links to map-reduce docs.

9. **Undocumented job / dev environment variables (framework)**
   - **Code references**:
     - `/Users/gregorykogan/Desktop/yt-framework/yt_framework/typed_jobs/stage_bootstrap.py`: `TOKENIZER_ARTIFACT_FILE`, `TOKENIZER_ARTIFACT_DIR`, `TOKENIZER_ARTIFACT_NAME`, `YT_STAGE_NAME`, sets `JOB_CONFIG_PATH`.
     - `/Users/gregorykogan/Desktop/yt-framework/yt_framework/yt/client_dev.py`: `YT_PIPELINE_DIR`.
   - **Docs**: `JOB_CONFIG_PATH` appears in typed-job and code-upload docs; **no** doc hits for `TOKENIZER_ARTIFACT_*`, `YT_STAGE_NAME`, or `YT_PIPELINE_DIR` under `/Users/gregorykogan/Desktop/yt-framework/docs/` (search performed via repository grep).
   - **Recommendation**: Single “Environment variables” reference table (dev vs prod vs job sandbox).

10. **`docs/index.md` “Reference” subsection**
    - **Issue**: Lists `/Users/gregorykogan/Desktop/yt-framework/docs/reference/api.md` and troubleshooting only; **omits** `/Users/gregorykogan/Desktop/yt-framework/docs/reference/ytjobs.md` though the file is in the root `toctree` (`reference/ytjobs`).
    - **Type**: `stale` / inconsistent discoverability.

11. **`CONTRIBUTING.md` documentation section**
    - **Path**: `/Users/gregorykogan/Desktop/yt-framework/CONTRIBUTING.md` (section “Documentation”).
    - **Issue**: States API reference is `/Users/gregorykogan/Desktop/yt-framework/docs/reference/api.md` only; does not mention `/Users/gregorykogan/Desktop/yt-framework/docs/reference/ytjobs.md` or job-library split.
    - **Type**: `stale`.

12. **`docs/reference/api.md` opening claim**
    - **Text**: “detailed API documentation for **all** YT Framework modules” and autogenerated from docstrings.
    - **Reality**: Large user-facing modules omitted (see findings 1–5, 7). **Type**: `stale` / overstated.

### LOW — `fragile_build` / maintenance debt

13. **Sphinx / MyST health (from prior full builds; not re-run here)**
    - Duplicate object descriptions for several `automodule` targets in `reference/api`.
    - **ERROR** class issue: duplicate section ID `table-operations` in `yt_framework.operations.table` docstring vs API page (docutils).
    - MyST cross-reference targets reported missing (e.g. paths like `../operations/`, `configuration/cluster-requirements.md` from some advanced pages).
    - Unknown directives: `card-grid` in some `.md.rst` intermediates (theme/extension mismatch).
    - **Recommendation**: Run `make -C docs html` with warnings logged; fix xrefs; resolve `table` module docstring heading clash; align MyST links with actual file layout.

14. **Docstring quality in autodoc’d framework code**
    - Prior tooling reported issues in `/Users/gregorykogan/Desktop/yt-framework/yt_framework/core/pipeline.py` and `/Users/gregorykogan/Desktop/yt-framework/yt_framework/core/discovery.py` docstrings (RST parsing warnings). **Type**: `fragile_build`.

---

## Coverage matrix (concise)

Legend: **guide** = narrative ops/config doc; **api** = `docs/reference/api.md` automodule; **ytjobs** = `docs/reference/ytjobs.md`.

| surface | guide | api | notes |
|---------|-------|-----|-------|
| `ytjobs` | partial (cross-links + dedicated page) | N/A (use ytjobs page) | Recently added; good baseline |
| Map | yes | yes (`operations.map`) | |
| Vanilla | yes | yes | |
| YQL | yes | via YT client modules | |
| S3 (framework `operations.s3`) | yes | yes | Job-side `S3Client` on `reference/ytjobs.md` |
| Table helpers | partial (examples use client) | yes | No ops index card |
| Checkpoint (framework) | yes | yes | |
| Tokenizer artifact | no | no | Code is production-relevant |
| Sort (`run_sort`) | no | no | Exported public API |
| Map-reduce / reduce | yes | no | Should add automodule |
| Typed jobs | yes | no | Should add automodule |
| Code upload / deps builders | partial | no | Advanced users lack API |

---

## Examples vs documentation

All example directories under `/Users/gregorykogan/Desktop/yt-framework/examples/` have `README.md` except verify none missing:

- Listed in `/Users/gregorykogan/Desktop/yt-framework/README.md` and `/Users/gregorykogan/Desktop/yt-framework/docs/index.md` example rosters: `01`–`10`, `environment_log`, `video_gpu`.
- **Gap**: Examples demonstrate behaviors (e.g. map-reduce, GPU) that do not map 1:1 to missing guides (sort, tokenizer_artifact)—examples are not a substitute for reference docs.

---

## Dead or “orphan” documentation

Strict **orphan** (file not linked): not fully verified without a Sphinx orphan report. Navigation observations:

- **Not in root `toctree`** (`/Users/gregorykogan/Desktop/yt-framework/docs/index.md`): `pipelines-and-stages.md`, `dev-vs-prod.md` are **not** top-level entries; they **are** included via `/Users/gregorykogan/Desktop/yt-framework/docs/configuration/index.md` `toctree` (`../dev-vs-prod`, `../pipelines-and-stages`). So they are **reachable**, not dead.
- **`.pytest_cache/README.md`**: tooling artifact; ignore for product docs.

**Dead / misleading content** (conceptual):

- Phrases such as “complete API” / “all modules” where automodule list is partial (**stale**, not file deletion).

---

## PyPI / install naming (doc accuracy)

- **Install**: `pip install yt-framework` (README, PyPI).
- **Import**: `import yt_framework`, `import ytjobs`.
- **Distribution metadata**: `/Users/gregorykogan/Desktop/yt-framework/pyproject.toml` `name = "yt_framework"`.
- New contributors may be confused; README already clarifies; optional improvement is a one-liner in `docs/index.md` installation.

---

## Suggested next actions (ordered)

1. Document **sort** and add **automodule** for `yt_framework.operations.sort`.
2. Document **tokenizer_artifact** (YAML + env vars + relationship to checkpoints) and add **automodule** for `tokenizer_artifact` or curated members.
3. Add **automodule** for `yt_framework.operations.map_reduce` and `yt_framework.typed_jobs`.
4. Add **environment variable reference** appendix (sandbox vs dev).
5. Update **CONTRIBUTING.md** and **docs/index.md** Reference list to include **YT jobs** reference.
6. Soften or fix **api.md** “all modules” claim; fix **table** docstring duplicate heading for Sphinx.
7. Run full **`make -C docs html`** and burn down **MyST xref** and **card-grid** warnings.

---

## Files intentionally not treated as product docs

- `/Users/gregorykogan/Desktop/yt-framework/.pytest_cache/README.md`
- Build output under `/Users/gregorykogan/Desktop/yt-framework/docs/_build/` (if present)

---

## Verification commands (for humans or agents re-validating)

```bash
# Optional: confirm gaps with ripgrep
rg -n "run_sort|tokenizer_artifact|run_map_reduce" docs/

# Docs build (requires Python >= 3.11 and pip install -e ".[docs]")
make -C docs html
```

End of report.

---

## Follow-up resolution (manual)

The items below were addressed in the repository after this audit (narrative guides, `docs/reference/api.md` expansions, `docs/reference/environment-variables.md`, MyST link fixes, and docstring/RST fixes for Sphinx). Re-run `make -C docs html` to validate. Remaining noise may include `card-grid` directive warnings (sphinx-design vs MyST), `plaintext` lexer warnings in code fences, and duplicate `automodule` object descriptions until further API ref splitting.
