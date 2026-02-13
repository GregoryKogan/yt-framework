#!/usr/bin/env python3
"""
Custom Upload Vanilla Script
============================

Demonstrates using a custom local package uploaded via upload_paths.
The my_utils package is copied from lib/my_utils to the job sandbox.
"""
import logging
from my_utils.helpers import greet  # pylint: disable=import-error

from omegaconf import OmegaConf
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path


def main():
    logger = get_logger("custom upload example", level=logging.INFO)

    logger.info("=" * 50)
    logger.info("CUSTOM UPLOAD VANILLA OPERATION STARTED")
    logger.info("=" * 50)

    # Load configuration
    config = OmegaConf.load(get_config_path())
    name = config.job.get("name", "World")

    # Use the custom module uploaded via upload_paths
    message = greet(name)
    logger.info(f"Custom greet() result: {message}")

    logger.info("")
    logger.info("=" * 50)
    logger.info("CUSTOM UPLOAD VANILLA OPERATION COMPLETED")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
