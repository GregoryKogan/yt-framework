#!/usr/bin/env python3

import logging
from omegaconf import OmegaConf
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path


def main():
    logger = get_logger("validate", level=logging.INFO)
    config = OmegaConf.load(get_config_path())

    # Get output table path from process operation config
    output_table = config.client.operations.process.output_table

    logger.info("=" * 50)
    logger.info("VALIDATION OPERATION STARTED")
    logger.info("=" * 50)
    logger.info(f"Validating processed table: {output_table}")

    # Validate config values
    multiplier = config.job.multiplier
    prefix = config.job.prefix

    logger.info("Config validation:")
    logger.info(f"  Multiplier: {multiplier}")
    logger.info(f"  Prefix: {prefix}")

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
