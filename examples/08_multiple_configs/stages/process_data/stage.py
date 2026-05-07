from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.utils.logging import log_header


class ProcessDataStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        yt = self.deps.yt_client
        pipeline_config = self.deps.pipeline_config

        # Access custom config values from configs/config.yaml
        dataset_size = pipeline_config.dataset.size
        row_count = pipeline_config.dataset.row_count
        prefix = pipeline_config.dataset.get("prefix", "")

        log_header(self.logger, "Process Data", f"Using {dataset_size} dataset config")

        self.logger.info("Config: %s", dataset_size)
        self.logger.info("Row count: %s", row_count)
        if prefix:
            self.logger.info("Prefix: %s", prefix)

        # Generate data based on config
        rows = []
        for i in range(row_count):
            row = {
                "id": i + 1,
                "value": (i + 1) * 10,
                "dataset": dataset_size,
            }
            if prefix:
                row["name"] = f"{prefix}item_{i + 1}"
            else:
                row["name"] = f"item_{i + 1}"
            rows.append(row)

        # Write to output table
        output_table = self.config.client.output_table
        yt.write_table(output_table, rows)

        self.logger.info("✓ Created table with %s rows", len(rows))
        self.logger.info("  Table: %s", output_table)

        # Verify by reading back
        read_rows = list(yt.read_table(output_table))
        self.logger.info("✓ Verified: read %s rows back", len(read_rows))
        # Show sample data
        if read_rows:
            sample = read_rows[0]
            self.logger.info("  Sample row: %s", sample)

        return debug
