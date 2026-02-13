from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations import run_vanilla


class UseCustomStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Starting vanilla operation with custom module...")

        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.vanilla,
        )

        if not success:
            raise RuntimeError("Vanilla operation failed")

        self.logger.info("Vanilla operation completed successfully")

        return debug
