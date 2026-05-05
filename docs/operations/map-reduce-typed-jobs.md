# Map-reduce: TypedJob mapper/reducer

```{note}
For JSON stdin/stdout command-mode mappers, see [YT jobs library (`ytjobs`)](../reference/ytjobs.md) (e.g. `BatchMapper` vs TypedJob pipelines).
```

## Quick start

### 1. Define your mapper and reducer

Create `stages/<stage>/src/jobs.py` (or any file under `src/`):

```python
from dataclasses import dataclass
from typing import Iterable
import yt.wrapper as yt
from yt_framework.typed_jobs import StageBootstrapTypedJob


@dataclass
class InputRow:
    key: str
    value: int


@dataclass
class IntermediateRow:
    key: str
    value: int


@dataclass
class OutputRow:
    key: str
    total: int


# Mapper: bare yt.TypedJob — all imports are module-level and cloudpickle-safe.
# yt_dataclass instances are serialized by cloudpickle together with the class.
class MyMapper(yt.TypedJob):
    def __call__(self, row: InputRow) -> Iterable[IntermediateRow]:
        yield IntermediateRow(key=row.key, value=row.value)


# Reducer: StageBootstrapTypedJob — imports heavy libs inside __call__ at
# runtime, after the sandbox extracts source.tar.gz and sets up sys.path.
class MyReducer(StageBootstrapTypedJob):
    def __call__(self, rows: yt.RowIterator[IntermediateRow]) -> Iterable[OutputRow]:
        key = None
        total = 0
        for row in rows:
            key = row.key
            total += row.value
        if key is not None:
            yield OutputRow(key=key, total=total)
```

### 2. Call from your stage

```python
from yt_framework.operations import run_map_reduce
from yt_framework.utils.sys_path import stage_src_path

class MyStage(BaseStage):
    def run(self, debug):
        with stage_src_path(self.stage_dir):
            from jobs import MyMapper, MyReducer

        run_map_reduce(
            context=self.context,
            operation_config=self.context.config.client.operations.map_reduce,
            map_job=MyMapper(),
            reduce_job=MyReducer(),
        )
        return debug
```

### 3. Stage config (`config.yaml`)

```yaml
client:
  operations:
    map_reduce:
      input_table: //tmp/my_pipeline/input
      output_table: //tmp/my_pipeline/output
      reduce_by: [key]
      max_row_weight: 128M  # optional override, default is 128M
      resources:
        pool: my_pool
        memory_limit_gb: 4
        cpu_limit: 2
```

`StageBootstrapTypedJob` automatically extracts `source.tar.gz` in the worker sandbox, adds the archive root and `stages/<stage>/src` to `sys.path`, and sets `JOB_CONFIG_PATH` — so `MyReducer` can import any code from `src/` without extra setup.

---

## Bare `yt.TypedJob` vs `StageBootstrapTypedJob`

Choose based on what the job does inside `__call__`:

| Scenario | Recommended base class |
|---|---|
| All imports are **module-level** and the classes involved are `@yt_dataclass` or plain Python objects cloudpickle can serialize | `yt.TypedJob` |
| `__call__` does **runtime imports** from `src/` (e.g. heavy ML libraries, custom modules under `stages/<stage>/src/`) | `StageBootstrapTypedJob` |
| Job needs access to `JOB_CONFIG_PATH` or other sandbox bootstrapping | `StageBootstrapTypedJob` |

**Why mappers often use bare `yt.TypedJob`:**
Mappers typically do lightweight filtering or partitioning using only `@yt_dataclass` row types. These classes are serialized by cloudpickle along with the job instance, so no sandbox setup is needed.

**Why reducers often need `StageBootstrapTypedJob`:**
Reducers typically import tokenizers, model wrappers, or other heavy dependencies at call time. These cannot be cloudpickled at submission time — they must be imported fresh inside the sandbox after `source.tar.gz` is extracted.

## Driver module uploads and pickling controls

YTsaurus `TypedJob` submission uses Python pickling. In addition to YT Framework's explicit `source.tar.gz` archive, the YTsaurus Python client may build an automatic modules archive from modules already present in the driver process `sys.modules`.

That automatic archive is independent from `pipeline.upload_modules`. For example, `upload_modules: [my_lib]` controls what YT Framework copies into `source.tar.gz`; it does not by itself prevent the YTsaurus client from also uploading imported site-packages into `tmpfs/modules`.

For Docker-based jobs, prefer using the Docker image for installed packages and `source.tar.gz` only for project code. Configure this at pipeline level:

```yaml
pipeline:
  mode: "prod"
  build_folder: "//path/to/build"
  upload_modules:
    - my_pipeline_lib
  pickling:
    ignore_system_modules: true
    # disable_module_upload: true
```

- `ignore_system_modules: true` skips stdlib and installed site-packages from the automatic modules archive. This prevents shadow copies of packages such as `certifi`, `boto3`, or `importlib` from overriding the Docker image inside the worker sandbox.
- `disable_module_upload: true` disables automatic module uploads completely. Use it only when the Docker image plus `source.tar.gz` contain everything the job imports at runtime.

`StageBootstrapTypedJob` still extracts `source.tar.gz` on the worker and adds the archive root plus `stages/<stage>/src` to `sys.path`; these flags only control the YTsaurus client's extra pickled modules archive.

### Example: bare `yt.TypedJob` mapper (cloudpickle-safe)

```python
import yt.wrapper as yt
from yt_framework.typed_jobs import StageBootstrapTypedJob

@yt.yt_dataclass
@dataclass
class InputRow:
    doc_id: str
    text: str

@yt.yt_dataclass
@dataclass
class PartitionRow:
    reduce_key: str
    sample_order: int
    text: str

class PartitionMapper(yt.TypedJob):
    """Pure partition logic — no heavy imports, cloudpickle-safe."""
    def __call__(self, row: InputRow) -> Iterable[PartitionRow]:
        yield PartitionRow(reduce_key=row.doc_id[:4], sample_order=0, text=row.text)
```

### Example: `StageBootstrapTypedJob` reducer (runtime imports)

```python
class TokenizerReducer(StageBootstrapTypedJob):
    def __call__(self, rows: yt.RowIterator[PartitionRow]) -> Iterable[OutputRow]:
        # Imported at runtime inside the sandbox after source.tar.gz is extracted.
        from eo_tokenizer_lib import build_tokenizer
        tokenizer = build_tokenizer(os.environ["MODEL_NAME"])
        for row in rows:
            tokens = tokenizer.encode(row.text)
            yield OutputRow(tokens=tokens)
```

---

## `typed_reduce_row_iterator_io`

Some documentation suggests aligning map-reduce reducer I/O with standalone reduce. **Many production clusters (including setups where reduce jobs in map-reduce do not support the `row_index` control attribute) will reject the spec** with an error such as:

> `"row_index" control attribute is not supported by "reduce" jobs in map-reduce operation`

Keep `typed_reduce_row_iterator_io: false` (default) unless your cluster explicitly supports it.

## Mapper batch I/O (`RowIterator`)

For **native typed batch input on the map leg**, implement the mapper as:

```python
def __call__(self, rows: RowIterator[InRow]) -> Iterable[OutRow]:
    for row in rows:
        ...
```

This matches reducer-side `RowIterator[...]` batching. Non-TypedJob pipelines can use `ytjobs.mapper.BatchMapper` for JSON stdin/stdout batching instead.

## Operation description

A string `operation_description` in stage config is both logged at submission time
and forwarded to the YT operation spec as the `title` field, making it visible in
the YT monitoring UI.  To attach structured metadata instead, pass a dict:

```yaml
# config.yaml
client:
  operations:
    map_reduce:
      operation_description: "my_pipeline: tokenize (PartitionMapper + TokenizerReducer)"
```
