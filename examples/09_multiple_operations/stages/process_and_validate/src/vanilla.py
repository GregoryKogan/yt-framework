#!/usr/bin/env python3

import logging

from omegaconf import OmegaConf

from ytjobs.config import get_config_path
from ytjobs.logging.logger import get_logger


def main() -> None:
    logger = get_logger("validate", level=logging.INFO)
    config = OmegaConf.load(get_config_path())

    # Get output table path from process operation config
    output_table = config.client.operations.process.output_table

    logger.info("=" * 50)
    logger.info("VALIDATION OPERATION STARTED")
    logger.info("=" * 50)
    logger.info("Validating processed table: %s", output_table)

    # Validate config values
    multiplier = config.job.multiplier
    prefix = config.job.prefix

    logger.info("Config validation:")
    logger.info("  Multiplier: %s", multiplier)
    logger.info("  Prefix: %s", prefix)

    logger.info("")
    logger.info("Simulating validation checks...")
    logger.info("Validation checks:")
    logger.info("  Multiplier is positive")
    logger.info("  Prefix is configured")
    logger.info("  Output table path is valid")

    logger.info("")
    logger.info("=" * 50)
    logger.info("VALIDATION OPERATION COMPLETED")
    logger.info("=" * 50)
    logger.info("All validation checks passed")


if __name__ == "__main__":
    main()
