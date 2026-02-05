"""
Provides common functionality for all pipelines, including code upload methods,
CLI entry point, and stage execution.

To create a new pipeline:
    1. Inherit from BasePipeline
    2. In setup(), create StageRegistry and register stages
    3. Optionally override run() for custom execution flow (rare)

Example:
    class Pipeline(BasePipeline):
        def setup(self):
            self.set_stage_registry(
                StageRegistry()
                .add_stage(MyStage)
            )

    if __name__ == "__main__":
        Pipeline.main()
"""

import sys
import argparse
import logging
import traceback
from pathlib import Path
from typing import Optional, Dict, Any, cast, TypeAlias

from omegaconf import OmegaConf, DictConfig
from yt_framework.yt.factory import create_yt_client
from yt_framework.utils.logging import (
    setup_logging,
    log_header,
    log_operation,
    log_success,
)
from yt_framework.utils.env import load_secrets
from yt_framework.core.registry import StageRegistry
from yt_framework.core.dependencies import PipelineStageDependencies
from yt_framework.operations.upload import upload_all_code


DebugContext: TypeAlias = Dict[str, Any]


class BasePipeline:
    """
    Base class for all pipelines.

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
    ):
        """
        Initialize the base pipeline.

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
            raise ValueError(f"Pipeline directory not found: {self.pipeline_dir}")

        # Set up logger with custom name based on class
        self.logger = setup_logging(level=log_level, name=self.__class__.__name__)

        # Load secrets from secrets.env file (for YT credentials)
        secrets = load_secrets(self.configs_dir)

        # Initialize YT client
        self.yt = create_yt_client(
            logger=self.logger,
            mode=self.config.pipeline.get("mode"),
            pipeline_dir=self.pipeline_dir,
            secrets=secrets if secrets else None,
        )

        # Initialize stage registry (set by setup())
        self._stage_registry: Optional[StageRegistry] = None

        # Call setup hook for pipeline-specific initialization
        self.setup()

    def setup(self) -> None:
        """
        Hook for pipeline-specific initialization.

        Override this method in subclasses to:
        1. Register stages using StageRegistry and set_stage_registry()
        2. Initialize custom clients (e.g., S3 client, database connections, etc.)

        This method is called automatically after base initialization.

        Returns:
            None
        """
        pass

    def set_stage_registry(self, registry: StageRegistry) -> None:
        """
        Set the stage registry for this pipeline.

        Args:
            registry: StageRegistry instance with registered stages

        Returns:
            None
        """
        self._stage_registry = registry

    def create_stage_dependencies(self) -> PipelineStageDependencies:
        """
        Create stage dependencies for injection.

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
        """
        Check if any enabled stages need code execution on YT.

        Stages need code execution if they have src/mapper.py or src/vanilla.py files.

        Returns:
            True if any enabled stage needs code execution, False otherwise
        """
        enabled_stages = self.config.stages.enabled_stages
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

    def upload_code(self, build_folder: Optional[str] = None) -> None:
        """
        Upload code to YT build folder.

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
                "No stages require code execution on YT - skipping code upload"
            )
            return

        # Only require build_folder if code execution is needed
        if build_folder is None:
            build_folder = self.config.pipeline.get("build_folder")
            if not build_folder:
                raise ValueError(
                    "build_folder not found in [pipeline] config section. "
                    "Stages with src/ directory require code execution on YT. "
                    'Add: build_folder = "//path/to/build/folder"'
                )

        # Get build code directory from config (optional)
        build_code_dir_str = self.config.pipeline.get("build_code_dir")
        build_code_dir = None
        if build_code_dir_str:
            build_code_dir = Path(build_code_dir_str)
            if not build_code_dir.is_absolute():
                # If relative path, make it relative to pipeline directory
                build_code_dir = self.pipeline_dir / build_code_dir

        upload_all_code(
            yt_client=self.yt,
            build_folder=build_folder,
            pipeline_dir=self.pipeline_dir,
            logger=self.logger,
            build_code_dir=build_code_dir,
        )

    def run(self) -> None:
        """
        Run the pipeline by executing enabled stages.

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
        enabled_stages = self.config.stages.enabled_stages
        if not enabled_stages:
            raise ValueError(
                "No enabled_stages found in stages config section. "
                'Add: enabled_stages: ["stage1", "stage2", "stage3"]'
            )

        log_header(
            self.logger, "Pipeline", f"Starting execution | Stages: {enabled_stages}"
        )

        # Verify stage registry is set
        if self._stage_registry is None:
            raise AttributeError(
                f"{self.__class__.__name__}.setup() must create and set stage registry. "
                "Example: self.set_stage_registry(StageRegistry().add_stage(MyStage))"
            )

        # Execute stages in order
        # Note: 'context' here is the shared data dict passed between stages
        context: DebugContext = {}

        # Create dependencies once for all stages (separate from context!)
        stage_deps = self.create_stage_dependencies()

        for stage_name in enabled_stages:
            if not self._stage_registry.has_stage(stage_name):
                available = list(self._stage_registry.get_all_stages().keys())
                raise ValueError(
                    f"Unknown stage: {stage_name}. " f"Available stages: {available}"
                )

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
    def main(cls, argv=None) -> None:
        """
        CLI entry point for the pipeline.

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

        parser = argparse.ArgumentParser(description=f"Run {cls.__name__}")
        parser.add_argument(
            "--config",
            type=str,
            default="configs/config.yaml",
            help="Path to config file (default: configs/config.yaml)",
        )
        args = parser.parse_args(argv)

        # Resolve pipeline directory (where pipeline.py is located)
        # sys.argv[0] is the script being run (pipeline.py)
        pipeline_dir = Path(sys.argv[0]).parent.resolve()

        # Resolve config path
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = pipeline_dir / config_path

        if not config_path.exists():
            logger.error(f"Config file not found: {config_path}")
            sys.exit(1)

        # Determine mode for logging
        try:
            temp_config = OmegaConf.load(config_path)
            mode = (
                temp_config.pipeline.get("mode", "dev")
                if isinstance(temp_config, DictConfig)
                else "dev"
            )
        except Exception as e:
            logger.error(f"Failed to determine mode: {e}")
            mode = "dev"

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

        # Load configuration
        try:
            loaded_config = OmegaConf.load(config_path)
            # Ensure it's a DictConfig (not ListConfig)
            if not isinstance(loaded_config, DictConfig):
                logger.error(
                    f"Config file must contain a dictionary, got {type(loaded_config).__name__}"
                )
                sys.exit(1)
            config = cast(DictConfig, loaded_config)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            traceback.print_exc()
            sys.exit(1)

        # Initialize and run pipeline
        try:
            pipeline = cls(config=config, pipeline_dir=pipeline_dir)
            pipeline.run()
            log_success(logger, "Pipeline completed successfully")
            sys.exit(0)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            traceback.print_exc()
            sys.exit(1)


class DefaultPipeline(BasePipeline):
    """
    Pipeline with automatic stage discovery.

    Automatically discovers and registers all stages from the stages/ directory.
    No need to manually import or register stages - just put them in stages/
    and they'll be automatically found.

    Usage:
        # pipeline.py
        from yt_framework.core.pipeline import DefaultPipeline

        if __name__ == "__main__":
            DefaultPipeline.main()

    The stages to run are still controlled by the enabled_stages configuration.
    """

    def setup(self) -> None:
        """
        Automatically discover and register stages from stages/ directory.

        Looks for all stage.py files in stages/*/ subdirectories and
        automatically imports and registers any BaseStage subclasses found.

        Returns:
            None
        """
        from yt_framework.core.discovery import discover_stages

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
