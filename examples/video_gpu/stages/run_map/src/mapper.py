#!/usr/bin/env python3
"""
GPU Mapper - Batch Processing with Multiprocessing
===================================================

Optimized for GPU nodes with batch processing.
Reads configuration from config.yaml file in YT sandbox.
"""

from omegaconf import OmegaConf
from ytjobs.mapper import BatchMapper
from ytjobs.logging.silencer import redirect_stdout_to_stderr
from ytjobs.config import get_config_path

with redirect_stdout_to_stderr():
    from stages.run_map.src.processor import process_video_batch

config = OmegaConf.load(get_config_path())


def main():
    """
    Main mapper function with batch processing.

    Uses BatchMapper to handle stdin/stdout boilerplate.
    """
    BatchMapper().map(
        process_video_batch,
        output_bucket=config.job.output_bucket,
        output_prefix=config.job.output_prefix,
        img_format=config.job.image_format,
        num_workers=config.job.num_workers,
        model_name=config.job.model_name,
    )


if __name__ == "__main__":
    main()
