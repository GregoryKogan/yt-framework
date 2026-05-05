# API reference

`yt_framework` symbols below are generated from Python docstrings. Job-side entrypoints (`ytjobs`) have a [separate page](ytjobs.md).

```{tip}
**Navigation**

Use the sidebar headings for modules. Anything not listed is still in the source tree; use `help(module)` locally.
```

## Layout

- **Core** — pipeline, stage, registry, discovery, `self.deps` injection
- **Operations** — map, vanilla, map-reduce/reduce, S3 helpers, table helpers, checkpoint upload, sort, tokenizer artifact wiring
- **Typed jobs** — `StageBootstrapTypedJob`
- **YT** — client factory, dev and prod clients (YQL helpers live on the clients)
- **Utils** — env files, logging setup, ignore patterns
- **Packaging** — upload helpers shared by operations
- **`ytjobs`** — [Job-side reference](ytjobs.md)

```{note}
**Narrative docs**

How-to guides under `docs/operations`, `docs/configuration`, and `docs/advanced` spell out YAML and stage patterns; this page is the raw API surface.
```

## Core Modules

### Pipeline

```{eval-rst}
.. automodule:: yt_framework.core.pipeline
   :members:
   :undoc-members:
   :show-inheritance:
```

### Stage

```{eval-rst}
.. automodule:: yt_framework.core.stage
   :members:
   :undoc-members:
   :show-inheritance:
```

### Registry

```{eval-rst}
.. automodule:: yt_framework.core.registry
   :members:
   :undoc-members:
   :show-inheritance:
```

### Discovery

```{eval-rst}
.. automodule:: yt_framework.core.discovery
   :members:
   :undoc-members:
   :show-inheritance:
```

### Core injection (`self.deps`)

```{eval-rst}
.. automodule:: yt_framework.core.dependencies
   :members:
   :undoc-members:
   :show-inheritance:
```

## Operations

### Map Operations

```{eval-rst}
.. automodule:: yt_framework.operations.map
   :members:
   :undoc-members:
   :show-inheritance:
```

### Vanilla Operations

```{eval-rst}
.. automodule:: yt_framework.operations.vanilla
   :members:
   :undoc-members:
   :show-inheritance:
```

### Map-reduce and reduce

```{eval-rst}
.. automodule:: yt_framework.operations.map_reduce
   :members:
   :undoc-members:
   :show-inheritance:
```

### YQL Operations

YQL operations are methods on the YT client. See the **YT Client** sections below for `join_tables`, `filter_table`, `select_columns`, `group_by_aggregate`, `union_tables`, `distinct`, `sort_table`, and `limit_table`.

```{note}
**YQL Operations Location**

YQL operations are implemented as methods on `BaseYTClient` and its subclasses (`YTDevClient` and `YTProdClient`). They are not in a separate operations module.
```

### S3 Operations

```{eval-rst}
.. automodule:: yt_framework.operations.s3
   :members:
   :undoc-members:
   :show-inheritance:
```

### Table operations

```{eval-rst}
.. automodule:: yt_framework.operations.table
   :members:
   :undoc-members:
   :show-inheritance:
```

### Checkpoint Operations

```{eval-rst}
.. automodule:: yt_framework.operations.checkpoint
   :members:
   :undoc-members:
   :show-inheritance:
```

### Sort operations

```{eval-rst}
.. automodule:: yt_framework.operations.sort
   :members:
   :undoc-members:
   :show-inheritance:
```

### Tokenizer artifact

```{eval-rst}
.. automodule:: yt_framework.operations.tokenizer_artifact
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: _tar_directory, _prepare_local_archive
```

## Typed jobs

```{eval-rst}
.. automodule:: yt_framework.typed_jobs
   :members:
   :undoc-members:
   :show-inheritance:
```

## Utilities

### Environment

```{eval-rst}
.. automodule:: yt_framework.utils.env
   :members:
   :undoc-members:
   :show-inheritance:
```

### Logging

```{eval-rst}
.. automodule:: yt_framework.utils.logging
   :members:
   :undoc-members:
   :show-inheritance:
```

### Ignore Patterns

```{eval-rst}
.. automodule:: yt_framework.utils.ignore
   :members:
   :undoc-members:
   :show-inheritance:
```

## Packaging and operation helpers (advanced)

These modules support code upload, file dependencies, and environment wiring. Most pipelines rely on defaults; use the API when extending the framework.

### Upload and local build

```{eval-rst}
.. automodule:: yt_framework.operations.upload
   :members: upload_all_code, build_code_locally, create_code_archive, upload_code_archive
   :show-inheritance:
```

### Stage dependency file lists

```{eval-rst}
.. automodule:: yt_framework.operations.dependencies
   :members: build_stage_dependencies, build_ytjobs_dependencies, build_map_dependencies, build_vanilla_dependencies, add_checkpoint
   :show-inheritance:
```

### Shared operation utilities

```{eval-rst}
.. automodule:: yt_framework.operations.common
   :members: build_environment, prepare_docker_auth, extract_operation_resources, collect_passthrough_kwargs, build_operation_environment, extract_docker_auth_from_operation_config, extract_max_failed_jobs
   :show-inheritance:
```

## YT Client

### Client Factory

```{eval-rst}
.. automodule:: yt_framework.yt.factory
   :members:
   :undoc-members:
   :show-inheritance:
```

### Dev Client

```{eval-rst}
.. automodule:: yt_framework.yt.client_dev
   :members:
   :undoc-members:
   :show-inheritance:
```

### Production Client

```{eval-rst}
.. automodule:: yt_framework.yt.client_prod
   :members:
   :undoc-members:
   :show-inheritance:
```
