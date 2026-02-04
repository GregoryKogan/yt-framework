from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.map import run_map


class RunMapStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Starting map operation...")

        success = run_map(
            context=self.context,
            operation_config=self.config.client.operations.map,
        )

        if not success:
            raise RuntimeError("Map operation failed")

        row_count = self.deps.yt_client.row_count(
            self.config.client.operations.map.output_table
        )
        self.logger.info(f"✓ Map operation completed: {row_count} output rows")

        return debug
