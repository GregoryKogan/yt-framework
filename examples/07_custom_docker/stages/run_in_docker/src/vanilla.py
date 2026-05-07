#!/usr/bin/env python3

import logging
import subprocess

from omegaconf import OmegaConf

from ytjobs.config import get_config_path
from ytjobs.logging.logger import get_logger


def main() -> None:
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
        logger.info("\n%s\n", result.stdout)
    except Exception:
        logger.exception("Error running cowsay")
        logger.info("This job will run only in a custom Docker image!")
        logger.info("Custom Docker images are only used in production mode!")
        msg = "This job will run only in a custom Docker image!"
        raise RuntimeError(msg)

    logger.info("")
    logger.info("=" * 60)
    logger.info("CUSTOM DOCKER OPERATION COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
