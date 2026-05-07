"""Video Processing Utilities.
===========================

Video processing utilities for GPU video processing pipeline.
"""

import tempfile

import cv2


def extract_frame(
    video_bytes: bytes, frame_index: int, img_format: str = "jpg"
) -> bytes:
    """Extract a single frame from video bytes.

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
            msg = "Cannot open video"
            raise RuntimeError(msg)

        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
        ok, frame = cap.read()
        if not ok:
            msg = f"Failed to read frame {frame_index}"
            raise RuntimeError(msg)

        success, encoded = cv2.imencode(f".{img_format}", frame)
        if not success:
            msg = "Failed to encode frame"
            raise RuntimeError(msg)

        cap.release()
        return encoded.tobytes()


def get_video_metadata(video_bytes: bytes) -> tuple[int, float, int, int]:
    """Get video metadata.

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
    """Get total frame count from video.

    Args:
        video_bytes: Video data as bytes

    Returns:
        Total number of frames

    """
    frame_count, _, _, _ = get_video_metadata(video_bytes)
    return frame_count


def encode_image(image_array, img_format: str = "jpg") -> bytes:
    """Encode image array to bytes.

    Args:
        image_array: OpenCV image array (numpy)
        img_format: Output format (jpg, png, etc.)

    Returns:
        Encoded image as bytes

    """
    success, encoded = cv2.imencode(f".{img_format}", image_array)
    if not success:
        msg = "Failed to encode image"
        raise RuntimeError(msg)
    return encoded.tobytes()


def decode_image(image_bytes: bytes):
    """Decode image bytes to array.

    Args:
        image_bytes: Image data as bytes

    Returns:
        OpenCV image array (numpy)

    """
    import numpy as np

    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        msg = "Failed to decode image"
        raise RuntimeError(msg)
    return img
