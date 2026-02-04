from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.s3 import list_s3_files, save_s3_paths_to_table
from yt_framework.utils.env import load_secrets
from ytjobs.s3.client import S3Client


class ListS3Stage(BaseStage):
    def __init__(self, deps, logger):
        super().__init__(deps, logger)

        self.s3_client = S3Client.create(
            secrets=load_secrets(self.deps.configs_dir),
            client_type="download",  # or "upload" for write access
        )

    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Listing files from S3...")

        paths = list_s3_files(
            s3_client=self.s3_client,
            bucket=self.config.client.input_bucket,
            prefix=self.config.client.input_prefix,
            logger=self.logger,
            extension=self.config.client.get(
                "file_extension"
            ),  # Optional: filter by extension
            max_files=self.config.client.get("max_files"),  # Optional: limit results
        )

        if not paths:
            self.logger.warning("No files found in S3")
            return debug

        self.logger.info(f"Found {len(paths)} files")

        save_s3_paths_to_table(
            yt_client=self.deps.yt_client,
            bucket=self.config.client.input_bucket,
            paths=paths,
            output_table=self.config.client.output_table,
            logger=self.logger,
        )

        self.logger.info(
            f"Saved {len(paths)} paths to {self.config.client.output_table}"
        )

        return debug
