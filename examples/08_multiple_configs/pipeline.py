"""
Multiple Configs Pipeline
==========================

Demonstrates using multiple configuration files with the same pipeline.
Users can specify which config to use via the --config CLI argument.
"""

from yt_framework.core.pipeline import DefaultPipeline

if __name__ == "__main__":
    DefaultPipeline.main()
