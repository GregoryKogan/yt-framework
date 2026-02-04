# API Reference

Complete API reference for YT Framework classes and functions.

## Pipeline Classes

### BasePipeline

Base class for all pipelines.

**Location:** `yt_framework.core.pipeline.BasePipeline`

**Methods:**

#### `__init__(config, pipeline_dir, log_level=logging.INFO)`

Initialize the pipeline.

**Parameters:**

- `config` (DictConfig): Configuration object (OmegaConf DictConfig)
- `pipeline_dir` (Path): Path to pipeline directory
- `log_level` (int): Logging level (default: INFO)

#### `setup() -> None`

Hook for pipeline-specific initialization. Override to register stages and initialize custom clients.

#### `set_stage_registry(registry: StageRegistry) -> None`

Set the stage registry for this pipeline.

**Parameters:**

- `registry` (StageRegistry): StageRegistry instance with registered stages

#### `upload_code(build_folder: Optional[str] = None) -> None`

Upload code to YT build folder. Only uploads if stages need code execution.

**Parameters:**

- `build_folder` (Optional[str]): YT build folder path. If None, uses `config.pipeline.build_folder`

#### `run() -> None`

Run the pipeline by executing enabled stages.

#### `main(cls, argv=None) -> None`

CLI entry point for the pipeline. Class method.

**Usage:**

```python
class MyPipeline(BasePipeline):
    def setup(self):
        registry = StageRegistry()
        registry.add_stage(MyStage)
        self.set_stage_registry(registry)

if __name__ == "__main__":
    MyPipeline.main()
```

### DefaultPipeline

Pipeline with automatic stage discovery.

**Location:** `yt_framework.core.pipeline.DefaultPipeline`

**Usage:**

```python
from yt_framework.core.pipeline import DefaultPipeline

if __name__ == "__main__":
    DefaultPipeline.main()
```

Automatically discovers and registers stages from `stages/` directory.

## Stage Classes

### BaseStage

Abstract base class for all pipeline stages.

**Location:** `yt_framework.core.stage.BaseStage`

**Properties:**

- `self.deps`: Stage dependencies (yt_client, pipeline_config, configs_dir)
- `self.logger`: Logger instance
- `self.config`: Stage configuration (loaded from `config.yaml`)
- `self.name`: Stage name (from directory name)
- `self.stage_dir`: Path to stage directory
- `self.context`: Stage context object

**Methods:**

#### `__init__(deps: StageDependencies, logger: logging.Logger) -> None`

Initialize stage with injected dependencies.

**Parameters:**

- `deps` (StageDependencies): Injected dependencies
- `logger` (logging.Logger): Logger instance

#### `run(debug: DebugContext) -> DebugContext`

Execute the stage. Must be implemented by subclasses.

**Parameters:**

- `debug` (DebugContext): Shared context dictionary from previous stages

**Returns:**

- `DebugContext`: Dictionary with stage results

**Example:**

```python
class MyStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        # Stage logic
        return debug
```

## Operations

### run_map

Run a map operation.

**Location:** `yt_framework.operations.map.run_map`

**Signature:**

```python
def run_map(
    context: StageContext,
    operation_config: DictConfig,
    output_schema: Optional[TableSchema] = None,
) -> bool
```

**Parameters:**

- `context` (StageContext): Stage context
- `operation_config` (DictConfig): Map operation configuration
- `output_schema` (Optional[TableSchema]): Optional output table schema

**Returns:**

- `bool`: True if operation succeeded, False otherwise

**Example:**

```python
from yt_framework.operations.map import run_map

success = run_map(
    context=self.context,
    operation_config=self.config.client.operations.map,
)
```

### run_vanilla

Run a vanilla operation.

**Location:** `yt_framework.operations.vanilla.run_vanilla`

**Signature:**

```python
def run_vanilla(
    context: StageContext,
    operation_config: DictConfig,
) -> bool
```

**Parameters:**

- `context` (StageContext): Stage context
- `operation_config` (DictConfig): Vanilla operation configuration

**Returns:**

- `bool`: True if operation succeeded, False otherwise

**Example:**

```python
from yt_framework.operations.vanilla import run_vanilla

success = run_vanilla(
    context=self.context,
    operation_config=self.config.client.operations.vanilla,
)
```

### S3 Operations

#### list_s3_files

List files in an S3 bucket.

**Location:** `yt_framework.operations.s3.list_s3_files`

**Signature:**

```python
def list_s3_files(
    s3_client: S3Client,
    bucket: str,
    prefix: str,
    logger: logging.Logger,
    extension: Optional[str] = None,
    max_files: Optional[int] = None,
) -> List[str]
```

**Parameters:**

- `s3_client` (S3Client): S3 client instance
- `bucket` (str): S3 bucket name
- `prefix` (str): S3 prefix to filter (can be empty string `""` to list all files)
- `logger` (logging.Logger): Logger instance
- `extension` (Optional[str]): File extension filter (e.g., ".json")
- `max_files` (Optional[int]): Maximum number of files to return

**Returns:**

- `List[str]`: List of S3 paths

#### save_s3_paths_to_table

Save S3 file paths to a YT table.

**Location:** `yt_framework.operations.s3.save_s3_paths_to_table`

**Signature:**

```python
def save_s3_paths_to_table(
    yt_client: BaseYTClient,
    bucket: str,
    paths: List[str],
    output_table: str,
    logger: logging.Logger,
) -> None
```

**Parameters:**

- `yt_client` (BaseYTClient): YT client instance
- `bucket` (str): S3 bucket name
- `paths` (List[str]): List of S3 paths
- `output_table` (str): YT table path to write
- `logger` (logging.Logger): Logger instance

## YT Client

### BaseYTClient

Abstract base class for YT client implementations.

**Location:** `yt_framework.yt.client_base.BaseYTClient`

**Methods:**

#### Table Operations

- `write_table(table_path, rows, append=False, replication_factor=1) -> None`
- `read_table(table_path) -> List[Dict[str, Any]]`
- `row_count(table_path) -> int`
- `exists(path) -> bool`
- `create_path(path, node_type="map_node") -> None`

#### YQL Operations

- `join_tables(left_table, right_table, output_table, on, how="left", select_columns=None, dry_run=False) -> Optional[str]`
  - `on`: Can be `str` (same column name), `List[str]` (multiple columns), or `Dict[str, str]` (different column names)
- `filter_table(input_table, output_table, condition, dry_run=False) -> Optional[str]`
- `select_columns(input_table, output_table, columns, dry_run=False) -> Optional[str]`
- `group_by_aggregate(input_table, output_table, group_by, aggregations, dry_run=False) -> Optional[str]`
  - `group_by`: Can be `str` or `List[str]`
  - `aggregations`: Dict mapping output column names to aggregation functions (can be `str` or `Tuple[str, str]`)
- `union_tables(tables, output_table, dry_run=False) -> Optional[str]`
- `distinct(input_table, output_table, columns=None, dry_run=False) -> Optional[str]`
- `sort_table(input_table, output_table, order_by, ascending=True, dry_run=False) -> Optional[str]`
  - `order_by`: Can be `str` or `List[str]`
- `limit_table(input_table, output_table, limit, dry_run=False) -> Optional[str]`

#### File Operations

- `upload_file(local_path, yt_path, create_parent_dir=False) -> None`
- `upload_directory(local_dir, yt_dir, pattern="*") -> List[str]`

### create_yt_client

Factory function to create YT client.

**Location:** `yt_framework.yt.factory.create_yt_client`

**Signature:**

```python
def create_yt_client(
    logger: Optional[logging.Logger] = None,
    mode: Optional[Literal["prod", "dev"]] = "dev",
    pipeline_dir: Optional[Union[Path, str]] = None,
    secrets: Optional[Dict[str, str]] = None,
) -> BaseYTClient
```

**Parameters:**

- `logger` (Optional[logging.Logger]): Logger instance
- `mode` (Optional[Literal["prod", "dev"]]): Execution mode (default: "dev")
- `pipeline_dir` (Optional[Union[Path, str]]): Pipeline directory (required for dev mode)
- `secrets` (Optional[Dict[str, str]]): YT credentials dictionary

**Returns:**

- `BaseYTClient`: YT client instance (YTProdClient or YTDevClient)

**Example:**

```python
from yt_framework.yt.factory import create_yt_client

yt_client = create_yt_client(
    logger=logger,
    mode="prod",
    secrets={"YT_PROXY": "...", "YT_TOKEN": "..."},
)
```

## Utilities

### load_secrets

Load secrets from environment file.

**Location:** `yt_framework.utils.env.load_secrets`

**Signature:**

```python
def load_secrets(secrets_dir: Path, env_file: str = "secrets.env") -> Dict[str, str]
```

**Parameters:**

- `secrets_dir` (Path): Directory containing the secrets.env file
- `env_file` (str): Name of the environment file (default: "secrets.env")

**Returns:**

- `Dict[str, str]`: Dictionary of secrets. Returns an empty dictionary if the file doesn't exist.

**Example:**

```python
from yt_framework.utils.env import load_secrets

secrets = load_secrets(self.deps.configs_dir)
yt_proxy = secrets.get("YT_PROXY")
```

### setup_logging

Setup logging for pipeline or stage.

**Location:** `yt_framework.utils.logging.setup_logging`

**Signature:**

```python
def setup_logging(
    level: int = logging.INFO,
    name: Optional[str] = None,
    use_colors: bool = True,
) -> logging.Logger
```

**Parameters:**

- `level` (int): Logging level (default: INFO)
- `name` (Optional[str]): Logger name
- `use_colors` (bool): Whether to use colored output (default: True)

**Returns:**

- `logging.Logger`: Logger instance

### log_header

Log a formatted header.

**Location:** `yt_framework.utils.logging.log_header`

**Signature:**

```python
def log_header(
    logger: logging.Logger,
    title: str,
    context: Optional[str] = None,
) -> None
```

**Parameters:**

- `logger` (logging.Logger): Logger instance
- `title` (str): Section title (will be wrapped in brackets)
- `context` (Optional[str]): Optional additional context information

### log_operation

Log an operation start.

**Location:** `yt_framework.utils.logging.log_operation`

**Signature:**

```python
def log_operation(
    logger: logging.Logger,
    message: str,
) -> None
```

### log_success

Log a success message.

**Location:** `yt_framework.utils.logging.log_success`

**Signature:**

```python
def log_success(
    logger: logging.Logger,
    message: str,
) -> None
```

## Checkpoint Operations

### init_checkpoint_directory

Initialize checkpoint directory in YT.

**Location:** `yt_framework.operations.checkpoint.init_checkpoint_directory`

**Signature:**

```python
def init_checkpoint_directory(
    context: StageContext,
    checkpoint_config: DictConfig,
) -> None
```

**Parameters:**

- `context` (StageContext): Stage context
- `checkpoint_config` (DictConfig): Checkpoint configuration

**Raises:**

- `FileNotFoundError`: If required checkpoint not found

## Stage Registry

### StageRegistry

Registry for managing stages.

**Location:** `yt_framework.core.registry.StageRegistry`

**Methods:**

#### `add_stage(stage_class: Type[BaseStage]) -> StageRegistry`

Add a stage to the registry.

**Parameters:**

- `stage_class` (Type[BaseStage]): Stage class to register

**Returns:**

- `StageRegistry`: Self for method chaining

#### `get_stage(stage_name: str) -> Type[BaseStage]`

Get stage class by name.

**Parameters:**

- `stage_name` (str): Stage name

**Returns:**

- `Type[BaseStage]`: Stage class

#### `has_stage(stage_name: str) -> bool`

Check if stage is registered.

**Parameters:**

- `stage_name` (str): Stage name

**Returns:**

- `bool`: True if stage is registered

#### `get_all_stages() -> Dict[str, Type[BaseStage]]`

Get all registered stages.

**Returns:**

- `Dict[str, Type[BaseStage]]`: Dictionary mapping stage names to classes

## Type Definitions

### DebugContext

Type alias for shared context dictionary.

**Location:** `yt_framework.core.pipeline.DebugContext`

**Type:** `Dict[str, Any]`

### StageContext

Dataclass containing stage context.

**Location:** `yt_framework.core.stage.StageContext`

**Fields:**

- `name` (str): Stage name
- `config` (DictConfig): Stage configuration
- `stage_dir` (Path): Stage directory path
- `logger` (logging.Logger): Logger instance
- `deps` (StageDependencies): Stage dependencies

### StageDependencies

Dataclass containing stage dependencies.

**Location:** `yt_framework.core.dependencies.StageDependencies`

**Fields:**

- `yt_client` (BaseYTClient): YT client instance
- `pipeline_config` (DictConfig): Pipeline configuration
- `configs_dir` (Path): Path to configs directory

## Constants

### OperationResources

Dataclass for operation resource configuration.

**Location:** `yt_framework.yt.client_base.OperationResources`

**Note:** In configuration files, use `memory_limit_gb` (not `memory_gb`). The framework automatically maps `memory_limit_gb` from config to `memory_gb` in this dataclass.

**Fields:**

- `pool` (str): YT pool name (default: "default")
- `pool_tree` (Optional[str]): Pool tree (default: None)
- `docker_image` (Optional[str]): Docker image (default: None)
- `memory_gb` (int): Memory in GB (default: 4). **In config files, use `memory_limit_gb`**
- `cpu_limit` (int): CPU cores (default: 2)
- `gpu_limit` (int): GPU count (default: 0)
- `job_count` (int): Number of jobs (default: 1)
- `user_slots` (Optional[int]): User slots limit (default: None)
