"""Pipeline base classes, CLI entry point (``main``), and code upload orchestration.

Subclass ``BasePipeline``, implement ``setup()`` to register stages with
``StageRegistry``, then run via ``Pipeline.main()`` from ``pipeline.py``.
``DefaultPipeline`` auto-discovers stages under ``stages/*/stage.py``.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from omegaconf import DictConfig

from yt_framework.core.debug_context import DebugContext
from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.core.discovery import discover_stages
from yt_framework.core.pipeline_cli import (
    build_pipeline_cli_parser,
    load_dict_config_or_exit,
    read_pipeline_mode_for_header,
    resolve_pipeline_config_path,
    run_pipeline_instance_or_exit,
)
from yt_framework.core.pipeline_config import (
    enabled_stage_names,
    normalize_upload_modules,
    normalize_upload_paths,
    pickling_dict_from_config,
    yt_mode_from_pipeline_config,
)
from yt_framework.core.registry import StageRegistry
from yt_framework.operations.upload import upload_all_code
from yt_framework.utils.env import load_secrets
from yt_framework.utils.logging import (
    log_header,
    log_operation,
    log_success,
    setup_logging,
)
from yt_framework.yt.factory import create_yt_client

__all__ = [
    "BasePipeline",
    "DebugContext",
    "DefaultPipeline",
    "_normalize_upload_modules",
    "_normalize_upload_paths",
    "normalize_upload_modules",
    "normalize_upload_paths",
]

_normalize_upload_modules = normalize_upload_modules
_normalize_upload_paths = normalize_upload_paths


class BasePipeline:
    """Base class for all pipelines.

    Provides common functionality:
    - CLI entry point via main() class method
    - Code upload to YT build folder
    - YT client initialization
    - Default stage execution loop

    Subclasses must:
    - In setup(), create StageRegistry and register stages via set_stage_registry()

    Subclasses may override:
    - setup(): Register stages and initialize pipeline-specific clients (S3, etc.)
    - run(): Custom execution flow (rare, most use default)
    """

    def __init__(
        self,
        config: DictConfig,
        pipeline_dir: Path,
        log_level: int = logging.INFO,
    ) -> None:
        """Initialize the base pipeline.

        Args:
            config: Configuration object (OmegaConf DictConfig)
            pipeline_dir: Path to pipeline directory
            log_level: Logging level

        Returns:
            None

        Raises:
            ValueError: If pipeline directory does not exist.

        """
        self.config = config
        self.pipeline_dir = Path(pipeline_dir).resolve()
        self.pipeline_name = self.pipeline_dir.name

        # Store configs directory path (where secrets.env is located)
        self.configs_dir = self.pipeline_dir / "configs"

        # Verify pipeline directory exists
        if not self.pipeline_dir.exists():
            msg = f"Pipeline directory not found: {self.pipeline_dir}"
            raise ValueError(msg)

        # Set up logger with custom name based on class
        self.logger = setup_logging(level=log_level, name=self.__class__.__name__)

        # Load secrets from secrets.env file (for YT credentials)
        secrets = load_secrets(self.configs_dir)

        # Initialize YT client
        mode = yt_mode_from_pipeline_config(self.config.pipeline.get("mode"))
        pickling_dict = pickling_dict_from_config(self.config.pipeline.get("pickling"))
        self.yt = create_yt_client(
            logger=self.logger,
            mode=mode,
            pipeline_dir=self.pipeline_dir,
            secrets=secrets or None,
            pickling=pickling_dict,
        )

        # Initialize stage registry (set by setup())
        self._stage_registry: StageRegistry | None = None

        # Call setup hook for pipeline-specific initialization
        self.setup()

    def setup(self) -> None:
        """Run pipeline-specific initialization.

        Override this method in subclasses to:
        1. Register stages using StageRegistry and set_stage_registry()
        2. Initialize custom clients (e.g., S3 client, database connections, etc.)

        This method is called automatically after base initialization.

        Returns:
            None

        """

    def set_stage_registry(self, registry: StageRegistry) -> None:
        """Set the stage registry for this pipeline.

        Args:
            registry: StageRegistry instance with registered stages

        Returns:
            None

        """
        self._stage_registry = registry

    def create_stage_dependencies(self) -> PipelineStageDependencies:
        """Create stage dependencies for injection.

        This method creates a dependency container with only what stages need,
        following the Interface Segregation Principle.

        Returns:
            PipelineStageDependencies with yt_client, pipeline_config, configs_dir

        """
        return PipelineStageDependencies(
            yt_client=self.yt,
            pipeline_config=self.config,
            configs_dir=self.configs_dir,
        )

    def _stages_need_code_execution(self) -> bool:
        """Check if any enabled stages need code execution on YT.

        Stages need code execution if they have src/mapper.py or src/vanilla.py files.

        Returns:
            True if any enabled stage needs code execution, False otherwise

        """
        enabled_stages = enabled_stage_names(self.config.stages.enabled_stages)
        if not enabled_stages:
            return False

        stages_dir = self.pipeline_dir / "stages"
        for stage_name in enabled_stages:
            stage_dir = stages_dir / stage_name
            if not stage_dir.exists():
                continue

            src_dir = stage_dir / "src"
            if src_dir.exists():
                return True

        return False

    def _resolve_upload_build_folder(self, build_folder: str | None) -> str:
        if build_folder is not None:
            return build_folder
        bf_raw = self.config.pipeline.get("build_folder")
        resolved = None if bf_raw is None else str(bf_raw).strip() or None
        if resolved:
            return resolved
        msg = (
            "build_folder not found in [pipeline] config section. "
            "Stages with src/ directory require code execution on YT. "
            'Add: build_folder = "//path/to/build/folder"'
        )
        raise ValueError(msg)

    def upload_code(self, build_folder: str | None = None) -> None:
        """Upload code to YT build folder.

        Only uploads code if any enabled stages need code execution on YT.
        If no stages need code execution, this method does nothing.

        Args:
            build_folder: Optional YT build folder path. If None, uses
                         config.pipeline.build_folder

        Returns:
            None

        Raises:
            ValueError: If build_folder is required but not provided in config.

        """
        # Check if any stages need code execution
        if not self._stages_need_code_execution():
            self.logger.debug(
                "No stages require code execution on YT - skipping code upload",
            )
            return

        resolved_folder = self._resolve_upload_build_folder(build_folder)

        # Get upload_modules and upload_paths from config
        upload_modules = normalize_upload_modules(
            self.config.pipeline.get("upload_modules"),
        )
        upload_paths = normalize_upload_paths(self.config.pipeline.get("upload_paths"))

        upload_all_code(
            yt_client=self.yt,
            build_folder=resolved_folder,
            pipeline_dir=self.pipeline_dir,
            logger=self.logger,
            upload_modules=upload_modules,
            upload_paths=upload_paths,
        )

    def run(self) -> None:
        """Run the pipeline by executing enabled stages.

        Default implementation:
        1. Upload code to YT (only if stages need code execution)
        2. Get enabled stages from config
        3. Execute stages in order using stage registry
        4. Pass context between stages

        Override this method only if you need completely custom execution flow.

        Returns:
            None

        Raises:
            ValueError: If no enabled_stages found in config or unknown stage name.
            AttributeError: If stage registry is not set in setup().

        """
        # Upload code once
        self.upload_code()

        # Get enabled stages from config
        enabled_stages = enabled_stage_names(self.config.stages.enabled_stages)
        if not enabled_stages:
            msg = (
                "No enabled_stages found in stages config section. "
                'Add: enabled_stages: ["stage1", "stage2", "stage3"]'
            )
            raise ValueError(msg)

        log_header(
            self.logger,
            "Pipeline",
            f"Starting execution | Stages: {enabled_stages}",
        )

        # Verify stage registry is set
        if self._stage_registry is None:
            msg = (
                f"{self.__class__.__name__}.setup() must create and set stage registry. "
                "Example: self.set_stage_registry(StageRegistry().add_stage(MyStage))"
            )
            raise AttributeError(msg)

        # Execute stages in order
        # Note: 'context' here is the shared data dict passed between stages
        context: DebugContext = {}

        # Create dependencies once for all stages (separate from context!)
        stage_deps = self.create_stage_dependencies()

        for stage_name in enabled_stages:
            if not self._stage_registry.has_stage(stage_name):
                available = list(self._stage_registry.get_all_stages().keys())
                msg = f"Unknown stage: {stage_name}. Available stages: {available}"
                raise ValueError(msg)

            # Instantiate and run stage
            stage_class = self._stage_registry.get_stage(stage_name)
            stage = stage_class(
                deps=stage_deps,  # Inject dependencies, NOT pipeline
                logger=self.logger,
            )

            log_operation(self.logger, f"Stage: {stage.name}")

            # Run stage - pass context dict to run() method (unchanged behavior)
            context = stage.run(context)

            log_success(self.logger, f"Stage completed: {stage.name}")

    @classmethod
    def main(cls, argv: list[str] | None = None) -> None:
        """CLI entry point for the pipeline.

        Handles:
        - Argument parsing (--config option)
        - Config file loading
        - Pipeline instantiation
        - Error handling and exit codes

        Args:
            argv: Optional command-line arguments. If None, uses sys.argv.

        Returns:
            None (exits with code 0 on success, 1 on failure)

        Usage:
            python pipeline.py
            python pipeline.py --config configs/custom.yaml

        """
        logger = setup_logging(level=logging.INFO, name=cls.__name__)

        parser = build_pipeline_cli_parser(cls.__name__)
        args = parser.parse_args(argv)
        pipeline_dir = Path(sys.argv[0]).parent.resolve()

        config_path = resolve_pipeline_config_path(pipeline_dir, args.config)
        if not config_path.exists():
            logger.error("Config file not found: %s", config_path)
            sys.exit(1)

        mode = read_pipeline_mode_for_header(config_path, logger)

        config_rel_path = (
            config_path.relative_to(pipeline_dir)
            if config_path.is_relative_to(pipeline_dir)
            else config_path
        )
        log_header(
            logger,
            cls.__name__,
            f"Pipeline: {pipeline_dir} | Config: {config_rel_path} | Mode: {mode}",
        )

        config = load_dict_config_or_exit(config_path, logger)
        run_pipeline_instance_or_exit(cls, config, pipeline_dir, logger)


class DefaultPipeline(BasePipeline):
    """Pipeline with automatic stage discovery.

    Discovers ``BaseStage`` subclasses from ``stages/<name>/stage.py`` and
    registers them. Run with ``DefaultPipeline.main()`` from ``pipeline.py``.
    Which stages execute is still controlled by ``enabled_stages`` in config.
    """

    def setup(self) -> None:
        """Automatically discover and register stages from the ``stages`` directory.

        Looks for ``stage.py`` under each ``stages/<name>/`` folder and registers
        every ``BaseStage`` subclass found.

        Returns:
            None

        """
        # Automatically discover stages
        stage_classes = discover_stages(
            pipeline_dir=self.pipeline_dir,
            logger=self.logger,
        )

        # Register all discovered stages
        registry = StageRegistry()
        for stage_class in stage_classes:
            registry.add_stage(stage_class)

        self.set_stage_registry(registry)

        # Log discovered stages (already logged by discover_stages, but keep for consistency)
        if not stage_classes:
            self.logger.warning("No stages discovered - check stages/ directory")
