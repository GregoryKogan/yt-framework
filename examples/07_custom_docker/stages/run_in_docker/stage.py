from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations import run_vanilla


class RunInDockerStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Starting operation with custom Docker image...")
        self.logger.info(
            "Image: %s", self.config.client.operations.vanilla.resources.docker_image
        )

        success = run_vanilla(
            context=self.context,
            operation_config=self.config.client.operations.vanilla,
        )

        if not success:
            msg = "Docker operation failed"
            raise RuntimeError(msg)

        self.logger.info(
            "Docker operation completed successfully - check logs for whale ASCII art!"
        )

        return debug
