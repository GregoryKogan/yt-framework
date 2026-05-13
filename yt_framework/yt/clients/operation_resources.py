"""Operation resource limits for YT jobs."""

from dataclasses import dataclass


def _require_positive_resource(name: str, value: int) -> None:
    if value <= 0:
        msg = f"{name} must be set to a positive integer, got {value}"
        raise ValueError(msg)


def _require_non_negative_gpu(gpu_limit: int) -> None:
    if gpu_limit < 0:
        msg = f"gpu_limit must be set to a non-negative integer, got {gpu_limit}"
        raise ValueError(msg)


@dataclass
class OperationResources:
    """Resource configuration for YT operations.

    This dataclass defines the computational resources allocated to YT operations
    like map and vanilla jobs. Note that in configuration files, use `memory_limit_gb`
    (not `memory_gb`) - the framework automatically maps this field.

    Attributes:
        pool: YT pool name for resource allocation (default: "default").
        pool_tree: Optional pool tree name (default: None).
        docker_image: Optional Docker image name for containerized execution (default: None).
        memory_gb: Memory allocation in GB (default: 4). In config files, use `memory_limit_gb`.
        cpu_limit: CPU cores allocated (default: 2).
        gpu_limit: Number of GPUs allocated (default: 0).
        job_count: Number of parallel jobs (default: 1).
        user_slots: Optional user slots limit (default: None).

    Raises:
        ValueError: If memory_gb, cpu_limit, or job_count are not positive integers,
                   or if gpu_limit is negative.

    """

    pool: str = "default"
    pool_tree: str | None = None
    docker_image: str | None = None
    memory_gb: int = 4
    cpu_limit: int = 2
    gpu_limit: int = 0
    job_count: int = 1
    user_slots: int | None = None

    def __post_init__(self) -> None:
        """Validate resource fields after initialization."""
        _require_positive_resource("memory_gb", self.memory_gb)
        _require_positive_resource("cpu_limit", self.cpu_limit)
        _require_non_negative_gpu(self.gpu_limit)
        _require_positive_resource("job_count", self.job_count)
