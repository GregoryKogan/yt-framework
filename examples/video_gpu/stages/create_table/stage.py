from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.operations.s3 import list_s3_files, save_s3_paths_to_table
from yt_framework.utils.logging import log_header
from yt_framework.utils.env import load_secrets
from ytjobs.s3.client import S3Client


class CreateTableStage(BaseStage):
    def __init__(self, deps, logger):
        super().__init__(deps, logger)

        self.s3_client = S3Client.create(
            secrets=load_secrets(self.deps.configs_dir),
            client_type="download",
        )

    def run(self, debug: DebugContext) -> DebugContext:
        log_header(self.logger, "Listing Files from S3")
        paths = list_s3_files(
            s3_client=self.s3_client,
            bucket=getattr(self.config, "client").input_bucket,
            prefix=getattr(self.config, "client").input_prefix,
            logger=self.logger,
            extension=getattr(self.config, "client").get("file_extension"),
            max_files=getattr(self.config, "client").get("max_files"),
        )

        if not paths:
            self.logger.warning("No files found in S3 - pipeline may have no work to do")
            return debug
        self.logger.info(f"Found {len(paths)} files")

        log_header(self.logger, "Saving Paths to YT Table")
        save_s3_paths_to_table(
            yt_client=self.deps.yt_client,
            bucket=self.config.client.input_bucket,
            paths=paths,
            output_table=self.config.client.paths_table,
            logger=self.logger,
        )
        self.logger.info(f"Saved {len(paths)} paths to {self.config.client.paths_table}")

        return debug
