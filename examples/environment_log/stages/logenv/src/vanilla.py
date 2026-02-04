"""
Comprehensive Environment Logger
==================================

Logs extensive system, hardware, environment, and software information
for debugging, reproducibility, and environment validation.
"""

import subprocess
import sys
import os
import platform
import socket
from datetime import datetime
from pathlib import Path
import logging
from typing import cast
from ytjobs.logging.logger import get_logger
from ytjobs.config import get_config_path


def run_command(cmd, logger, description="command", timeout=10):
    """Safely execute a command and return output."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=isinstance(cmd, str),
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            logger.warning(
                f"{description} failed (exit {result.returncode}): {result.stderr.strip()[:200]}"
            )
            return None
    except subprocess.TimeoutExpired:
        logger.warning(f"{description} timed out after {timeout}s")
        return None
    except Exception as e:
        logger.warning(f"{description} error: {str(e)[:200]}")
        return None


def log_section_header(logger, title):
    """Log a formatted section header."""
    logger.info("=" * 60)
    logger.info(title)
    logger.info("=" * 60)


def log_gpu_info(logger):
    """Log GPU and CUDA information."""
    log_section_header(logger, "1. GPU & CUDA INFORMATION")

    # nvidia-smi full output
    output = run_command(["nvidia-smi"], logger, "nvidia-smi")
    if output:
        logger.info("nvidia-smi output:")
        for line in output.split("\n"):
            logger.info(f"  {line}")
    else:
        logger.info("nvidia-smi: Not available")

    # nvcc version
    nvcc_output = run_command(["nvcc", "--version"], logger, "nvcc")
    if nvcc_output:
        logger.info(
            f"nvcc version: {nvcc_output.split('release')[-1].strip() if 'release' in nvcc_output else nvcc_output}"
        )

    # PyTorch CUDA info
    try:
        import torch

        logger.info(f"PyTorch version: {torch.__version__}")
        logger.info(f"PyTorch CUDA available: {torch.cuda.is_available()}")
        if torch.cuda.is_available():
            logger.info(f"PyTorch CUDA version: {torch.version.cuda}")
            logger.info(f"PyTorch cuDNN version: {torch.backends.cudnn.version()}")
            logger.info(f"Number of GPUs: {torch.cuda.device_count()}")
            for i in range(torch.cuda.device_count()):
                logger.info(f"GPU {i}: {torch.cuda.get_device_name(i)}")
                props = torch.cuda.get_device_properties(i)
                logger.info(f"  - Compute capability: {props.major}.{props.minor}")
                logger.info(f"  - Total memory: {props.total_memory / 1024**3:.2f} GB")
    except ImportError:
        logger.info("PyTorch: Not installed")
    except Exception as e:
        logger.warning(f"PyTorch CUDA check error: {e}")


def log_python_environment(logger):
    """Log Python-specific environment information."""
    log_section_header(logger, "2. PYTHON ENVIRONMENT")

    # Python version and implementation
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Python implementation: {platform.python_implementation()}")
    logger.info(f"Python compiler: {platform.python_compiler()}")
    logger.info(f"Python executable: {sys.executable}")

    # Virtual environment
    venv = os.environ.get("VIRTUAL_ENV")
    conda_env = os.environ.get("CONDA_DEFAULT_ENV")
    if venv:
        logger.info(f"Virtual environment: {venv}")
    if conda_env:
        logger.info(f"Conda environment: {conda_env}")

    # Site packages
    try:
        import site

        logger.info(f"Site packages: {', '.join(site.getsitepackages())}")
    except Exception as e:
        logger.warning(f"Error getting site packages: {e}")

    # Python path
    logger.info("Python path (sys.path):")
    for i, path in enumerate(sys.path):
        logger.info(f"  [{i}] {path}")

    # pip freeze
    logger.info("")
    logger.info("Installed packages (pip freeze):")
    pip_output = run_command(
        [sys.executable, "-m", "pip", "freeze"], logger, "pip freeze", timeout=30
    )
    if pip_output:
        for line in pip_output.split("\n"):
            if line.strip():
                logger.info(f"  {line}")
    else:
        logger.warning("Could not retrieve pip packages")


def log_system_info(logger):
    """Log system specifications."""
    log_section_header(logger, "3. SYSTEM INFORMATION")

    # OS information
    logger.info(f"OS: {platform.system()}")
    logger.info(f"OS Release: {platform.release()}")
    logger.info(f"OS Version: {platform.version()}")

    # Distribution info (Linux)
    try:
        import distro

        logger.info(f"Distribution: {distro.name()} {distro.version()}")
    except ImportError:
        dist_output = run_command(["cat", "/etc/os-release"], logger, "os-release")
        if dist_output:
            logger.info("Distribution info:")
            for line in dist_output.split("\n")[:5]:
                logger.info(f"  {line}")

    # Kernel and architecture
    logger.info(f"Kernel: {platform.release()}")
    logger.info(f"Architecture: {platform.machine()}")
    logger.info(f"Processor: {platform.processor()}")

    # Hostname
    logger.info(f"Hostname: {socket.gethostname()}")

    # CPU info
    cpu_info = run_command(["lscpu"], logger, "lscpu")
    if cpu_info:
        logger.info("CPU information:")
        for line in cpu_info.split("\n"):
            if any(
                key in line
                for key in ["Model name", "CPU(s)", "Thread", "Core", "Socket", "MHz"]
            ):
                logger.info(f"  {line.strip()}")

    # Memory info
    mem_info = run_command(["free", "-h"], logger, "free")
    if mem_info:
        logger.info("Memory information:")
        for line in mem_info.split("\n"):
            logger.info(f"  {line}")

    # Disk space
    df_output = run_command(["df", "-h"], logger, "df")
    if df_output:
        logger.info("Disk space:")
        for line in df_output.split("\n"):
            logger.info(f"  {line}")


def log_network_info(logger):
    """Log network and connectivity information."""
    log_section_header(logger, "4. NETWORK & CONNECTIVITY")

    # Network interfaces
    ip_output = run_command(["ip", "addr"], logger, "ip addr")
    if ip_output:
        logger.info("Network interfaces:")
        for line in ip_output.split("\n"):
            logger.info(f"  {line}")
    else:
        ifconfig_output = run_command(["ifconfig"], logger, "ifconfig")
        if ifconfig_output:
            logger.info("Network interfaces (ifconfig):")
            for line in ifconfig_output.split("\n")[:30]:
                logger.info(f"  {line}")

    # DNS configuration
    dns_output = run_command(["cat", "/etc/resolv.conf"], logger, "resolv.conf")
    if dns_output:
        logger.info("DNS configuration:")
        for line in dns_output.split("\n"):
            if line.strip() and not line.startswith("#"):
                logger.info(f"  {line}")

    # Proxy settings
    http_proxy = os.environ.get("http_proxy") or os.environ.get("HTTP_PROXY")
    https_proxy = os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY")
    if http_proxy:
        logger.info(f"HTTP Proxy: {http_proxy}")
    if https_proxy:
        logger.info(f"HTTPS Proxy: {https_proxy}")

    # Connectivity test
    ping_output = run_command(["ping", "-c", "1", "-W", "2", "8.8.8.8"], logger, "ping")
    logger.info(
        f"Internet connectivity (ping 8.8.8.8): {'OK' if ping_output else 'FAILED'}"
    )


def log_file_structure(logger):
    """Log formatted file structure of current directory."""
    log_section_header(logger, "5. FILE STRUCTURE")

    cwd = os.getcwd()
    logger.info(f"Working directory: {cwd}")
    logger.info("")

    # Fallback to custom tree implementation
    logger.info("Directory structure:")
    try:

        def format_tree(path, prefix="", max_depth=3, current_depth=0, max_files=200):
            """Generate tree structure recursively."""
            if current_depth >= max_depth:
                return [], 0

            items = []
            file_count = 0

            try:
                entries = sorted(
                    Path(path).iterdir(), key=lambda x: (not x.is_dir(), x.name)
                )
                # Filter out common ignored patterns
                entries = [
                    e
                    for e in entries
                    if not any(
                        pattern in e.name
                        for pattern in [
                            "__pycache__",
                            ".pyc",
                            ".git",
                            ".egg-info",
                            ".dev",
                        ]
                    )
                ]

                for i, entry in enumerate(entries[:100]):  # Limit entries per directory
                    if file_count >= max_files:
                        items.append(f"{prefix}... (truncated)")
                        break

                    is_last = i == len(entries) - 1
                    current = "└── " if is_last else "├── "
                    extension = "    " if is_last else "│   "

                    if entry.is_dir():
                        items.append(f"{prefix}{current}{entry.name}/")
                        file_count += 1
                        sub_items, sub_count = format_tree(
                            entry,
                            prefix + extension,
                            max_depth,
                            current_depth + 1,
                            max_files - file_count,
                        )
                        items.extend(sub_items)
                        file_count += sub_count
                    else:
                        try:
                            size = entry.stat().st_size
                            if size < 1024:
                                size_str = f"{size} B"
                            elif size < 1024**2:
                                size_str = f"{size/1024:.1f} KB"
                            else:
                                size_str = f"{size/1024**2:.1f} MB"
                            items.append(f"{prefix}{current}{entry.name} ({size_str})")
                            file_count += 1
                        except Exception as e:
                            logger.warning(f"Error getting file size: {e}")
                            items.append(f"{prefix}{current}{entry.name}")
                            file_count += 1
            except PermissionError:
                logger.warning(f"Permission denied: {path}")
                items.append(f"{prefix}[Permission Denied]")
            except Exception as e:
                logger.warning(f"Error getting file structure: {e}")
                items.append(f"{prefix}[Error: {str(e)[:50]}]")

            return items, file_count

        tree_items, total_files = format_tree(cwd, max_depth=3)
        logger.info(f"{Path(cwd).name}/")
        for item in tree_items[:500]:  # Limit total output
            logger.info(item)

        if total_files >= 200:
            logger.info("... (output truncated)")
        logger.info(f"\nTotal items shown: {min(total_files, 500)}")

    except Exception as e:
        logger.error(f"Error generating file structure: {e}")

    # Disk usage summary
    du_output = run_command(["du", "-sh", cwd], logger, "disk usage")
    if du_output:
        logger.info(f"Total directory size: {du_output.split()[0]}")


def log_software_versions(logger):
    """Log installed software versions."""
    log_section_header(logger, "6. INSTALLED SOFTWARE VERSIONS")

    software = {
        "git": ["git", "--version"],
        "docker": ["docker", "--version"],
        "cmake": ["cmake", "--version"],
        "gcc": ["gcc", "--version"],
        "g++": ["g++", "--version"],
        "make": ["make", "--version"],
        "curl": ["curl", "--version"],
        "wget": ["wget", "--version"],
    }

    for name, cmd in software.items():
        output = run_command(cmd, logger, name)
        if output:
            first_line = output.split("\n")[0]
            logger.info(f"{name}: {first_line}")
        else:
            logger.info(f"{name}: Not installed")


def log_process_info(logger):
    """Log process and resource information."""
    log_section_header(logger, "7. PROCESS & RESOURCE INFORMATION")

    # Current user and groups
    try:
        user = os.getlogin()
    except (OSError, AttributeError):
        user = os.environ.get("USER", "unknown")

    logger.info(f"Current user: {user}")
    logger.info(f"UID: {os.getuid()}, GID: {os.getgid()}")

    groups_output = run_command(["groups"], logger, "groups")
    if groups_output:
        logger.info(f"Groups: {groups_output}")

    # Current working directory
    logger.info(f"Working directory: {os.getcwd()}")

    # Ulimit settings
    ulimit_output = run_command(["bash", "-c", "ulimit -a"], logger, "ulimit")
    if ulimit_output:
        logger.info("Ulimit settings:")
        for line in ulimit_output.split("\n"):
            logger.info(f"  {line}")

    # Process info
    logger.info(f"Process ID: {os.getpid()}")
    logger.info(f"Parent process ID: {os.getppid()}")

    # Open file descriptors
    try:
        proc_fd = Path(f"/proc/{os.getpid()}/fd")
        if proc_fd.exists():
            fd_count = len(list(proc_fd.iterdir()))
            logger.info(f"Open file descriptors: {fd_count}")
    except Exception as e:
        logger.warning(f"Error getting open file descriptors: {e}")


def log_container_info(logger):
    """Log container/sandbox information."""
    log_section_header(logger, "8. CONTAINER/SANDBOX INFORMATION")

    # Check if in container
    in_container = Path("/.dockerenv").exists() or Path("/run/.containerenv").exists()
    logger.info(f"Running in container: {in_container}")

    # Check cgroup
    cgroup_output = run_command(["cat", "/proc/1/cgroup"], logger, "cgroup")
    if cgroup_output and ("docker" in cgroup_output or "lxc" in cgroup_output):
        logger.info("Container runtime detected in cgroups")

    # YT sandbox info
    yt_job_id = os.environ.get("YT_JOB_ID")
    yt_operation_id = os.environ.get("YT_OPERATION_ID")
    if yt_job_id:
        logger.info(f"YT Job ID: {yt_job_id}")
    if yt_operation_id:
        logger.info(f"YT Operation ID: {yt_operation_id}")

    # Mounted filesystems
    mount_output = run_command(["mount"], logger, "mount")
    if mount_output:
        logger.info("Mounted filesystems:")
        for line in mount_output.split("\n")[:20]:
            logger.info(f"  {line}")


def log_dl_frameworks(logger):
    """Log deep learning framework versions."""
    log_section_header(logger, "9. DEEP LEARNING FRAMEWORKS")

    # PyTorch
    try:
        import torch

        logger.info(f"PyTorch: {torch.__version__}")
        logger.info(f"  - Build: {torch.version.git_version}")
        logger.info(
            f"  - CUDA: {torch.version.cuda if torch.version.cuda else 'CPU-only'}"
        )
        logger.info(
            f"  - cuDNN: {torch.backends.cudnn.version() if torch.backends.cudnn.is_available() else 'N/A'}"
        )
    except ImportError:
        logger.info("PyTorch: Not installed")

    # TensorFlow
    try:
        import tensorflow as tf  # pyright: ignore[reportMissingImports]

        logger.info(f"TensorFlow: {tf.__version__}")
        logger.info(f"  - Built with CUDA: {tf.test.is_built_with_cuda()}")
        gpus = tf.config.list_physical_devices("GPU")
        logger.info(f"  - GPUs available: {len(gpus)}")
    except ImportError:
        logger.info("TensorFlow: Not installed")
    except Exception as e:
        logger.info(f"TensorFlow: Installed but error checking: {e}")

    # JAX
    try:
        import jax  # pyright: ignore[reportMissingImports]

        logger.info(f"JAX: {jax.__version__}")
        logger.info(f"  - Backend: {jax.default_backend()}")
    except ImportError:
        logger.info("JAX: Not installed")

    # ONNX Runtime
    try:
        import onnxruntime as ort  # pyright: ignore[reportMissingImports]

        logger.info(f"ONNX Runtime: {ort.__version__}")
        logger.info(
            f"  - Available providers: {', '.join(ort.get_available_providers())}"
        )
    except ImportError:
        logger.info("ONNX Runtime: Not installed")

    # Other common ML libraries
    libraries = [
        "numpy",
        "scipy",
        "scikit-learn",
        "pandas",
        "matplotlib",
        "opencv-python",
        "pillow",
        "transformers",
    ]

    logger.info("")
    logger.info("Other ML/Data libraries:")
    for lib_name in libraries:
        try:
            module = __import__(lib_name.replace("-", "_"))
            version = getattr(module, "__version__", "unknown")
            logger.info(f"  {lib_name}: {version}")
        except ImportError:
            pass


def log_config_info(logger):
    """Log configuration values that the job can see."""
    log_section_header(logger, "10. CONFIGURATION VALUES")

    def format_config_value(key, value, prefix=""):
        """Recursively format config values."""
        if isinstance(value, dict):
            for k, v in value.items():
                format_config_value(f"{key}.{k}" if key else k, v, prefix + "  ")
        elif isinstance(value, list):
            logger.info(f"{prefix}{key}:")
            for i, item in enumerate(value):
                logger.info(f"{prefix}  [{i}]: {item}")
        else:
            # Mask sensitive values
            if any(
                sensitive in key.lower()
                for sensitive in ["password", "secret", "token", "key"]
            ):
                logger.info(f"{prefix}{key}: {'*' * min(len(str(value)), 20)}")
            else:
                logger.info(f"{prefix}{key}: {value}")

    try:
        from omegaconf import OmegaConf
    except ImportError:
        logger.warning("omegaconf not available - cannot load config")
        return

    try:
        config_path = get_config_path()
        logger.info(f"Config file path: {config_path}")

        if config_path.exists():
            config = OmegaConf.load(config_path)
            logger.info("")
            logger.info("Configuration values:")

            config_dict = cast(dict, OmegaConf.to_container(config, resolve=True))
            for key, value in config_dict.items():
                format_config_value(key, value)
        else:
            logger.warning(f"Config file not found: {config_path}")
    except ValueError as e:
        logger.warning(f"Config path not available: {e}")
    except Exception as e:
        logger.warning(f"Error loading config: {e}")
        import traceback

        logger.debug(traceback.format_exc())


def log_metadata(logger, start_time):
    """Log execution metadata."""
    log_section_header(logger, "11. EXECUTION METADATA")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"Start time (UTC): {start_time.isoformat()}")
    logger.info(f"End time (UTC): {end_time.isoformat()}")
    logger.info(f"Duration: {duration:.2f} seconds")

    # YT job info
    yt_job_id = os.environ.get("YT_JOB_ID")
    yt_operation_id = os.environ.get("YT_OPERATION_ID")
    if yt_job_id:
        logger.info(f"YT Job ID: {yt_job_id}")
    if yt_operation_id:
        logger.info(f"YT Operation ID: {yt_operation_id}")

    # Script info
    logger.info(f"Script: {__file__}")
    logger.info(f"Script directory: {Path(__file__).parent}")


def main():
    """Main execution function."""
    start_time = datetime.now()

    # Initialize logger
    logger = get_logger("logenv", level=logging.INFO)

    logger.info("=" * 60)
    logger.info("COMPREHENSIVE ENVIRONMENT LOG")
    logger.info("=" * 60)
    logger.info(f"Started at: {start_time.isoformat()}")
    logger.info("")

    # Execute all logging functions
    try:
        log_gpu_info(logger)
        logger.info("")

        log_python_environment(logger)
        logger.info("")

        log_system_info(logger)
        logger.info("")

        log_network_info(logger)
        logger.info("")

        log_file_structure(logger)
        logger.info("")

        log_software_versions(logger)
        logger.info("")

        log_process_info(logger)
        logger.info("")

        log_container_info(logger)
        logger.info("")

        try:
            log_dl_frameworks(logger)
        except Exception as e:
            logger.error(f"Error in DL frameworks section: {e}")
        logger.info("")

        try:
            log_config_info(logger)
        except Exception as e:
            logger.error(f"Error in config section: {e}")
        logger.info("")

        log_metadata(logger, start_time)

    except Exception as e:
        logger.error(f"Critical error during logging: {e}")
        import traceback

        logger.error(traceback.format_exc())

    logger.info("")
    logger.info("=" * 60)
    logger.info("ENVIRONMENT LOG COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
