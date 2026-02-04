#!/usr/bin/env python3

import logging
import subprocess
from omegaconf import OmegaConf
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path


def main():
    logger = get_logger("custom_docker", level=logging.INFO)

    logger.info("=" * 60)
    logger.info("CUSTOM DOCKER OPERATION STARTED")
    logger.info("=" * 60)
    logger.info("")

    config = OmegaConf.load(get_config_path())

    try:
        result = subprocess.run(
            ["cowsay", config.job.message], capture_output=True, text=True, check=True
        )
        logger.info(f"\n{result.stdout}\n")
    except Exception as e:
        logger.error(f"Error running cowsay: {e}")
        logger.info("This job will run only in a custom Docker image!")
        logger.info("Custom Docker images are only used in production mode!")
        raise RuntimeError("This job will run only in a custom Docker image!")

    logger.info("")
    logger.info("=" * 60)
    logger.info("CUSTOM DOCKER OPERATION COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
