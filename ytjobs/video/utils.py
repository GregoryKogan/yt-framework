"""
Common Video Processing Utilities
==================================

Reusable functions for video processing across all pipelines.
"""

import cv2
import tempfile
from typing import Tuple


def extract_frame(
    video_bytes: bytes, frame_index: int, img_format: str = "jpg"
) -> bytes:
    """
    Extract a single frame from video bytes.

    Args:
        video_bytes: Video data as bytes
        frame_index: Frame number to extract
        img_format: Output format (jpg, png, etc.)

    Returns:
        Frame image as bytes
    """
    with tempfile.NamedTemporaryFile(delete=True, suffix=".mp4") as tmp:
        tmp.write(video_bytes)
        tmp.flush()

        cap = cv2.VideoCapture(tmp.name)
        if not cap.isOpened():
            raise RuntimeError("Cannot open video")

        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
        ok, frame = cap.read()
        if not ok:
            raise RuntimeError(f"Failed to read frame {frame_index}")

        success, encoded = cv2.imencode(f".{img_format}", frame)
        if not success:
            raise RuntimeError("Failed to encode frame")

        cap.release()
        return encoded.tobytes()


def get_video_metadata(video_bytes: bytes) -> Tuple[int, float, int, int]:
    """
    Get video metadata.

    Args:
        video_bytes: Video data as bytes

    Returns:
        Tuple of (frame_count, fps, width, height)
    """
    with tempfile.NamedTemporaryFile(delete=True, suffix=".mp4") as tmp:
        tmp.write(video_bytes)
        tmp.flush()

        cap = cv2.VideoCapture(tmp.name)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()

        return frame_count, fps, width, height


def get_video_frame_count(video_bytes: bytes) -> int:
    """
    Get total frame count from video.

    Args:
        video_bytes: Video data as bytes

    Returns:
        Total number of frames
    """
    frame_count, _, _, _ = get_video_metadata(video_bytes)
    return frame_count


def encode_image(image_array, img_format: str = "jpg") -> bytes:
    """
    Encode image array to bytes.

    Args:
        image_array: OpenCV image array (numpy)
        img_format: Output format (jpg, png, etc.)

    Returns:
        Encoded image as bytes
    """
    success, encoded = cv2.imencode(f".{img_format}", image_array)
    if not success:
        raise RuntimeError("Failed to encode image")
    return encoded.tobytes()


def decode_image(image_bytes: bytes):
    """
    Decode image bytes to array.

    Args:
        image_bytes: Image data as bytes

    Returns:
        OpenCV image array (numpy)
    """
    import numpy as np

    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        raise RuntimeError("Failed to decode image")
    return img
