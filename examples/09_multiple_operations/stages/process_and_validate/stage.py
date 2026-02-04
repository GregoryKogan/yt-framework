from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.map import run_map
from yt_framework.operations.vanilla import run_vanilla
from yt_framework.utils.logging import log_header


class ProcessAndValidateStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        log_header(self.logger, "Process and Validate", "Running map then vanilla")

        # =====================================================================
        # Step 1: Process operation
        # =====================================================================

        self.logger.info("Step 1: Running map operation...")
        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.process,
        )
        if not success:
            raise RuntimeError("Process operation failed")

        output_table = self.config.client.operations.process.output_table
        row_count = self.deps.yt_client.row_count(output_table)
        self.logger.info(f"Process operation completed: {row_count} rows processed")

        # =====================================================================
        # Step 2: Validate operation
        # =====================================================================

        self.logger.info("Step 2: Running vanilla operation...")
        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.validate,
        )
        if not success:
            raise RuntimeError("Validate operation failed")

        self.logger.info("Validate operation completed")

        self.logger.info("Both operations completed successfully")
        
        return debug
