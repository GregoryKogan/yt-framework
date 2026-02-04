from functools import wraps
from contextlib import contextmanager
import sys
import os


def manage_output(mode="redirect"):
    """
    Decorator factory that can either suppress all output or redirect stdout to stderr.

    Args:
        mode: 'suppress' to suppress all output, 'redirect' to redirect stdout to stderr

    Usage:
        @manage_output(mode='redirect')
        def process_video(...):
            print("This goes to stderr")  # Won't corrupt stdout
            return result

        @manage_output(mode='suppress')
        def noisy_function(...):
            print("This is completely silenced")
            return result
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if mode == "suppress":
                with suppress_all_output():
                    return func(*args, **kwargs)
            elif mode == "redirect":
                with redirect_stdout_to_stderr():
                    return func(*args, **kwargs)
            else:
                raise ValueError(
                    f"Invalid mode: {mode}. Must be 'suppress' or 'redirect'"
                )

        return wrapper

    return decorator


@contextmanager
def redirect_stdout_to_stderr():
    """
    Context manager that redirects stdout to stderr.

    This is useful for YTsaurus mappers where you need clean JSON on stdout,
    but processing functions might print debug messages.

    Usage:
        with redirect_stdout_to_stderr():
            print("This goes to stderr")  # Won't corrupt stdout
            some_function_that_prints()
    """
    original_stdout = sys.stdout
    try:
        sys.stdout = sys.stderr
        yield
    finally:
        sys.stdout = original_stdout


@contextmanager
def suppress_all_output():
    """
    Context manager that suppresses ALL output: stdout, stderr, and warnings.

    This is useful when you need complete silence from noisy libraries
    like OpenCV, Ultralytics YOLO, TensorFlow, Ceres Solver, etc.

    Usage:
        with suppress_all_output():
            # All prints, warnings, and library output are suppressed
            model = YOLO('yolov8n.pt')  # No output
            results = model(image)       # No progress bars
            cv2.imread(path)             # No warnings
    """
    # Save original Python streams
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    # Save original OS-level file descriptors
    original_stdout_fd = os.dup(1)  # stdout FD
    original_stderr_fd = os.dup(2)  # stderr FD

    # Open devnull for writing
    devnull = open(os.devnull, "w")
    devnull_fd = devnull.fileno()

    try:
        # Redirect Python streams to devnull
        sys.stdout = devnull
        sys.stderr = devnull

        # Redirect OS-level file descriptors (this catches C library output like Ceres)
        os.dup2(devnull_fd, 1)  # Redirect stdout FD
        os.dup2(devnull_fd, 2)  # Redirect stderr FD

        yield

    finally:
        # Restore OS-level file descriptors first
        os.dup2(original_stdout_fd, 1)
        os.dup2(original_stderr_fd, 2)

        # Close duplicate FDs
        os.close(original_stdout_fd)
        os.close(original_stderr_fd)

        # Restore Python streams
        sys.stdout = original_stdout
        sys.stderr = original_stderr

        # Close devnull
        devnull.close()
