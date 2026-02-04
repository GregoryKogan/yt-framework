from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.utils.logging import log_header


class JoinTablesStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        log_header(self.logger, "Joining Tables with YQL")

        self.logger.info(f"Left table (results): {self.config.client.results_table}")
        self.logger.info(f"Right table (paths): {self.config.client.paths_table}")
        self.logger.info(f"Output table: {self.config.client.joined_table}")

        self.deps.yt_client.join_tables(
            left_table=self.config.client.results_table,
            right_table=self.config.client.paths_table,
            output_table=self.config.client.joined_table,
            on={"left": self.config.client.join_key, "right": "path"},
            how=self.config.client.join_type.lower(),
            select_columns=[
                "a.input_s3_path",
                "a.meta",
                "a.output_s3_path",
                "b.bucket AS joined_bucket"
            ]
        )

        self.logger.info("Join completed successfully")
        self.logger.info(f"Results saved to: {self.config.client.joined_table}")

        return debug
