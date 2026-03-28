# YT jobs library (`ytjobs`)

The **`ytjobs`** package is part of the same distribution as `yt_framework` and is intended to run **inside YTsaurus jobs** (mappers, reducers, command-mode scripts): code you import from uploaded job bundles, not from the orchestration process on your laptop.

- **`yt_framework`**: pipeline layout, stage discovery, YT clients in dev/prod, operation configs.
- **`ytjobs`**: small, job-safe helpers—JSON stdin/stdout mappers, S3 access from workers, stderr logging, `JOB_CONFIG_PATH`, and Cypress checkpoint I/O via `yt.wrapper`.

```{note}
**Two “checkpoint” ideas**

[Checkpoint management](../advanced/checkpoints.md) describes **model files** the framework uploads and mounts in operations. **`ytjobs.checkpoint`** is separate: save/load **byte blobs and JSON state** under Cypress paths inside a running job. See the API sections below.
```

Cluster Docker images must include dependencies your job code uses (for example `boto3` for `ytjobs.s3`, `ytsaurus-client` for `ytjobs.checkpoint`). See [Cluster requirements](../configuration/cluster-requirements.md).

Convenience imports match the package `__all__`:

```python
from ytjobs import (
    S3Client,
    get_logger,
    log_with_extra,
    redirect_stdout_to_stderr,
    get_config_path,
    read_input_rows,
    StreamMapper,
    BatchMapper,
)
```

The submodules below are the source of truth for API details (generated from docstrings).

## Mapper utilities (`ytjobs.mapper`)

```{eval-rst}
.. automodule:: ytjobs.mapper
   :members:
   :undoc-members:
   :show-inheritance:
```

## S3 client (`ytjobs.s3.client`)

```{eval-rst}
.. automodule:: ytjobs.s3.client
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: _decode_http_chunked_if_present
```

## Logging (`ytjobs.logging`)

```{eval-rst}
.. automodule:: ytjobs.logging
   :members:
   :undoc-members:
   :show-inheritance:
```

## Job config (`ytjobs.config`)

```{eval-rst}
.. automodule:: ytjobs.config
   :members:
   :undoc-members:
   :show-inheritance:
```

## Cypress checkpoints (`ytjobs.checkpoint`)

```{eval-rst}
.. automodule:: ytjobs.checkpoint
   :members:
   :undoc-members:
   :show-inheritance:
```
