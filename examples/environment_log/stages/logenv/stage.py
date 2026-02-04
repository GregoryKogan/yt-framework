from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations import run_vanilla


class LogEnvStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:        
        if not run_vanilla(self.context, self.config.client.operations.vanilla):
            raise RuntimeError("Log environment operation failed")
    
        return debug
