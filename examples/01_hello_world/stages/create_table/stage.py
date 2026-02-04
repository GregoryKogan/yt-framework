from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage


class CreateTableStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Creating table...")

        rows = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 87},
            {"id": 3, "name": "Charlie", "score": 92},
        ]

        self.deps.yt_client.write_table(
            table_path=self.config.client.output_table,
            rows=rows,
        )

        self.logger.info(f"Created table: {len(rows)} rows | {self.config.client.output_table}")

        result = list(self.deps.yt_client.read_table(self.config.client.output_table))
        self.logger.info(f"Verified: read {len(result)} rows back")

        return debug
