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

        self.logger.info(f"Config: {dataset_size}")
        self.logger.info(f"Row count: {row_count}")
        if prefix:
            self.logger.info(f"Prefix: {prefix}")

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

        self.logger.info(f"✓ Created table with {len(rows)} rows")
        self.logger.info(f"  Table: {output_table}")

        # Verify by reading back
        read_rows = list(yt.read_table(output_table))
        self.logger.info(f"✓ Verified: read {len(read_rows)} rows back")

        # Show sample data
        if read_rows:
            sample = read_rows[0]
            self.logger.info(f"  Sample row: {sample}")

        return debug
