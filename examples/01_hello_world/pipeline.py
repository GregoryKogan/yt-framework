"""
Hello World Pipeline
=====================

The simplest example of a YT Framework pipeline.
Uses DefaultPipeline for automatic stage discovery.
"""

from yt_framework.core.pipeline import DefaultPipeline

if __name__ == "__main__":
    DefaultPipeline.main()
