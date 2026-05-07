from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.checkpoint import init_checkpoint_directory
from yt_framework.operations.map import run_map


class RunMapStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        init_checkpoint_directory(
            self.context, self.config.client.operations.map.checkpoint
        )

        if not run_map(self.context, self.config.client.operations.map):
            msg = "Map operation failed"
            raise RuntimeError(msg)

        return debug
