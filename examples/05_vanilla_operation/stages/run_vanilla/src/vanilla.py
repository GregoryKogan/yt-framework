#!/usr/bin/env python3
"""
Simple Vanilla Script
=====================

This script is executed as a standalone job on YT cluster.
Unlike mapper, it doesn't process stdin - it just runs once.
"""

import time
import logging
from omegaconf import OmegaConf
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path


def main():
    logger = get_logger("vanilla example", level=logging.INFO)

    logger.info("=" * 50)
    logger.info("VANILLA OPERATION STARTED")
    logger.info("=" * 50)

    # Load configuration
    config = OmegaConf.load(get_config_path())

    greeting = config.job.greeting
    iterations = config.job.iterations

    logger.info(f"Greeting: {greeting}")
    logger.info(f"Iterations: {iterations}")
    logger.info("")

    # Simulate some work
    for i in range(iterations):
        logger.info(f"Iteration {i + 1}/{iterations}: Processing...")
        time.sleep(0.5)  # Simulate work

    logger.info("")
    logger.info("=" * 50)
    logger.info("VANILLA OPERATION COMPLETED")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
