from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage


class CreateInputStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Creating input table for map operation...")

        rows = [
            {"id": 1, "text": "hello world", "value": 10},
            {"id": 2, "text": "foo bar baz", "value": 20},
            {"id": 3, "text": "test string", "value": 30},
            {"id": 4, "text": "another example", "value": 40},
            {"id": 5, "text": "final row", "value": 50},
        ]

        self.deps.yt_client.write_table(
            table_path=self.config.client.input_table,
            rows=rows,
        )

        self.logger.info(
            f"Created input table with {len(rows)} rows: {self.config.client.input_table}"
        )

        return debug
