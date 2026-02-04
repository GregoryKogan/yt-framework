from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage


class CreateInputStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Creating input table...")

        rows = [
            {"id": 1, "value": 10, "text": "first"},
            {"id": 2, "value": 20, "text": "second"},
            {"id": 3, "value": 30, "text": "third"},
        ]

        self.deps.yt_client.write_table(
            table_path=self.config.client.output_table,
            rows=rows,
        )

        self.logger.info(f"✓ Created input table: {len(rows)} rows")
        return debug
