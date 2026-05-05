"""Job-side helpers shipped with uploaded bundles (`ytjobs`).

Designed for mappers, reducers, vanilla scripts, and other code executed on YT
workers; the driver may import it for typing/tests, but production job logic
should treat it as the sandbox stdlib extension.
"""

# Re-export commonly used classes
from .s3 import S3Client
from .logging import get_logger, log_with_extra, redirect_stdout_to_stderr
from .config import get_config_path
from .mapper import read_input_rows, StreamMapper, BatchMapper

__all__ = [
    "S3Client",
    "get_logger",
    "log_with_extra",
    "redirect_stdout_to_stderr",
    "get_config_path",
    "read_input_rows",
    "StreamMapper",
    "BatchMapper",
]
