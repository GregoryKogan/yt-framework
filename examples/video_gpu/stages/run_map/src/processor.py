import os
import tempfile
from typing import List, Dict, Any
import torch.multiprocessing as mp
from functools import partial
import sys

sys.path.insert(0, ".")
from ytjobs.video.utils import (
    extract_frame,
    get_video_frame_count,
    encode_image,
)
from ytjobs.s3.client import S3Client

from ultralytics import YOLO  # pyright: ignore[reportPrivateImportUsage]
import torch

# Global model cache per process
_MODEL_CACHE = {}


def get_cached_model(model_name: str = "yolov8n-seg.pt"):
    """
    Get or create cached model for this process.

    Model is loaded from mounted checkpoint file (not from internet).
    Checkpoint file is mounted by YT and available in the current directory.
    Model is automatically moved to GPU if CUDA is available.

    Args:
        model_name: Model filename (mounted file name)

    Returns:
        Loaded YOLO model (on GPU if available)

    Raises:
        FileNotFoundError: If checkpoint file not found
        RuntimeError: If checkpoint cannot be loaded
    """
    if model_name not in _MODEL_CACHE:
        # Get checkpoint file path from environment or use model_name
        checkpoint_file = os.environ.get("CHECKPOINT_FILE", model_name)

        # Check if file exists (mounted files are in current directory)
        if not os.path.exists(checkpoint_file):
            raise FileNotFoundError(
                f"Checkpoint file not found: {checkpoint_file}\n"
                f"Current directory: {os.getcwd()}\n"
                f"Files in directory: {os.listdir('.')}\n"
                f"Please ensure checkpoint is mounted as a file in the job."
            )

        # Determine device (GPU if available, else CPU)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        device_id = torch.cuda.current_device() if torch.cuda.is_available() else None

        # Load model directly from mounted checkpoint file on specified device
        print(
            f"Loading YOLO model from mounted checkpoint file: {checkpoint_file}",
            file=sys.stderr,
        )
        print(
            f"Using device: {device}"
            + (f" (GPU {device_id})" if device_id is not None else ""),
            file=sys.stderr,
        )
        model = YOLO(checkpoint_file)

        # Move model to GPU if available
        if torch.cuda.is_available():
            model.to(device)
            print(
                f"✓ Model {model_name} loaded and moved to GPU {device_id}",
                file=sys.stderr,
            )
        else:
            print(
                f"✓ Model {model_name} loaded on CPU (CUDA not available)",
                file=sys.stderr,
            )

        _MODEL_CACHE[model_name] = model

    return _MODEL_CACHE[model_name]


def run_segmentation(frame_bytes: bytes, model_name: str = "yolov8n-seg.pt") -> bytes:
    """
    Run segmentation on a frame using cached model.

    Model inference runs on GPU if available, otherwise CPU.

    Args:
        frame_bytes: Frame image as bytes
        model_name: Model to use for segmentation

    Returns:
        Annotated image with segmentation masks as bytes
    """
    # Get cached model (loaded once per process, already on GPU if available)
    model = get_cached_model(model_name)

    # Determine device for inference
    device = "cuda" if torch.cuda.is_available() else "cpu"

    # Decode image
    with tempfile.NamedTemporaryFile(delete=True, suffix=".jpg") as tmp:
        tmp.write(frame_bytes)
        tmp.flush()

        # Run inference on GPU if available (model is already on GPU)
        # YOLO automatically uses the device the model is on, but we can be explicit
        results = model(tmp.name, device=device)

        # Get annotated image with masks
        annotated = results[0].plot()

        # Encode back to bytes
        return encode_image(annotated, "jpg")


def process_single_video(
    row_data: dict,
    output_bucket: str,
    output_prefix: str,
    img_format: str,
    model_name: str,
) -> List[Dict[str, Any]]:
    """
    Process a single video with segmentation.

    This function is called by worker processes in the pool.
    Model is loaded once per worker and reused for all videos.

    Args:
        row_data: Dictionary with 'bucket' and 'path' keys
        output_bucket: S3 bucket for outputs
        output_prefix: S3 prefix for outputs
        img_format: Image format
        model_name: Model to use

    Returns:
        List of result dictionaries
    """
    results = []

    # Initialize S3 clients
    # Read secrets from environment (passed from pipeline, not loaded into global env)
    import os
    secrets = {
        "S3_ENDPOINT": os.environ.get("S3_ENDPOINT", ""),
        "S3_DOWNLOAD_ACCESS_KEY": os.environ.get("S3_DOWNLOAD_ACCESS_KEY", ""),
        "S3_DOWNLOAD_SECRET_KEY": os.environ.get("S3_DOWNLOAD_SECRET_KEY", ""),
        "S3_UPLOAD_ACCESS_KEY": os.environ.get("S3_UPLOAD_ACCESS_KEY", ""),
        "S3_UPLOAD_SECRET_KEY": os.environ.get("S3_UPLOAD_SECRET_KEY", ""),
    }
    s3_download = S3Client.create(secrets=secrets, client_type="download")
    s3_upload = S3Client.create(secrets=secrets, client_type="upload")

    bucket = row_data["bucket"]
    path = row_data["path"]

    try:
        # Download video
        video_bytes = s3_download.download(bucket, path)

        # Get video metadata
        frame_count = get_video_frame_count(video_bytes)

        # Base name for output files
        base_name = os.path.join(
            output_prefix, os.path.splitext(os.path.basename(path))[0]
        )

        # Extract and process FIRST frame
        first_frame = extract_frame(video_bytes, 0, img_format)
        first_segmented = run_segmentation(first_frame, model_name)

        # Upload segmented first frame
        first_key = f"{base_name}_frame_0_segmented.{img_format}"
        s3_upload.upload(first_segmented, output_bucket, first_key)

        results.append(
            {
                "input_s3_path": path,
                "output_s3_path": first_key,
                "meta": "frame_0_segmented",
                "frame_count": frame_count,
                "output_bucket": output_bucket,
                "model": model_name,
            }
        )

        # Extract and process LAST frame
        last_frame = extract_frame(video_bytes, frame_count - 1, img_format)
        last_segmented = run_segmentation(last_frame, model_name)

        # Upload segmented last frame
        last_key = f"{base_name}_frame_{frame_count - 1}_segmented.{img_format}"
        s3_upload.upload(last_segmented, output_bucket, last_key)

        results.append(
            {
                "input_s3_path": path,
                "output_s3_path": last_key,
                "meta": f"frame_{frame_count - 1}_segmented",
                "frame_count": frame_count,
                "output_bucket": output_bucket,
                "model": model_name,
            }
        )

    except Exception as e:
        # Raise error with context about which video failed
        raise RuntimeError(f"Failed to process video s3://{bucket}/{path}: {e}") from e

    return results


def process_video_batch(
    rows: List[object],
    output_bucket: str,
    output_prefix: str,
    img_format: str,
    num_workers: int = 5,
    model_name: str = "yolov8n-seg.pt",
):
    """
    Process multiple videos using multiprocessing pool for GPU efficiency.

    Args:
        rows: List of dictionaries with bucket and path fields
        output_bucket: S3 bucket for outputs
        output_prefix: S3 prefix for outputs
        img_format: Image format
        num_workers: Number of parallel workers
        model_name: Model to use

    Yields:
        Dictionary with processing results for each frame
    """
    print("CUDA is available: ", torch.cuda.is_available(), file=sys.stderr)
    # Use parameters directly (already strings)
    out_bucket = output_bucket
    out_prefix = output_prefix
    img_ext = img_format

    # Rows are already dictionaries - convert to list for multiprocessing
    row_dicts: List[Dict[str, str]] = [dict(row) for row in rows]  # type: ignore[arg-type]

    # Create worker function with fixed parameters
    worker_func = partial(
        process_single_video,
        output_bucket=out_bucket,
        output_prefix=out_prefix,
        img_format=img_ext,
        model_name=model_name,
    )

    # Process videos in parallel using torch.multiprocessing pool
    # Use 'spawn' start method for CUDA compatibility
    # Errors will be raised and propagated (not silently caught)
    try:
        # Get spawn context for CUDA compatibility
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=num_workers) as pool:
            all_results = pool.map(worker_func, row_dicts)
    except Exception as e:
        # Re-raise with context about batch processing failure
        raise RuntimeError(
            f"Batch processing failed with {num_workers} workers: {e}"
        ) from e

    # Yield all results
    for video_results in all_results:
        for result in video_results:
            yield result
