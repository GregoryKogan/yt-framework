"""
S3 Operations
=============

Reusable S3 client for any S3 operations.
"""

import boto3
import logging
from typing import Dict, List, Literal, Optional
from botocore.client import Config as BotoConfig


def _decode_http_chunked_if_present(data: bytes, logger: logging.Logger) -> bytes:
    """
    If `data` is HTTP chunked-transfer encoded (stored by buggy S3-compatible
    backends when uploading from dev/local), decode it
    and return only the payload. Otherwise return `data` unchanged.
    """
    if not data or data[0:1] not in b"0123456789abcdefABCDEF":
        return data
    out = bytearray()
    i = 0
    while i < len(data):
        line_end = data.find(b"\n", i)
        if line_end < 0:
            break
        line = data[i:line_end].strip(b"\r")
        if b";" in line:
            size_part = line.split(b";")[0].strip()
        else:
            size_part = line.strip()
        try:
            size = int(size_part, 16)
        except ValueError:
            break
        i = line_end + 1
        if size == 0:
            break
        if i + size > len(data):
            break
        out.extend(data[i : i + size])
        i += size
        while i < len(data) and data[i : i + 1] in (b"\r", b"\n"):
            i += 1
    if out:
        logger.debug(f"Decoded HTTP chunked body ({len(out)} bytes payload)")
        return bytes(out)
    return data


class S3Client:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        max_retries: int = 30,
        timeout: int = 360,
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger(__name__)

        self.client = boto3.client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint,
            config=BotoConfig(
                s3={"addressing_style": "virtual"},
                retries={"max_attempts": max_retries, "mode": "standard"},
                read_timeout=timeout,
                max_pool_connections=1,
            ),
        )

        self.logger.debug(f"S3 client initialized: {endpoint}")

    @staticmethod
    def create(secrets: Dict[str, str], client_type: Literal["download", "upload"] = "download") -> "S3Client":
        """
        Create S3 client from secrets dictionary.

        Args:
            secrets: Dictionary containing S3 credentials. Expected keys:
                    - S3_ENDPOINT
                    - S3_DOWNLOAD_ACCESS_KEY
                    - S3_DOWNLOAD_SECRET_KEY
                    - S3_UPLOAD_ACCESS_KEY
                    - S3_UPLOAD_SECRET_KEY
            client_type: 'download' or 'upload' (default: 'download')

        Returns:
            Configured S3Client instance
        """

        if client_type == "upload":
            access_key = secrets.get("S3_UPLOAD_ACCESS_KEY")
            secret_key = secrets.get("S3_UPLOAD_SECRET_KEY")
        elif client_type == "download":
            access_key = secrets.get("S3_DOWNLOAD_ACCESS_KEY")
            secret_key = secrets.get("S3_DOWNLOAD_SECRET_KEY")
        else:
            raise ValueError(f"Unknown client type: {client_type}")

        endpoint = secrets.get("S3_ENDPOINT")

        if not endpoint:
            raise ValueError("S3_ENDPOINT is not set")
        if not access_key:
            raise ValueError(f"S3_{client_type.upper()}_ACCESS_KEY is not set")
        if not secret_key:
            raise ValueError(f"S3_{client_type.upper()}_SECRET_KEY is not set")

        return S3Client(endpoint=endpoint, access_key=access_key, secret_key=secret_key)

    def list_files(
        self,
        bucket: str,
        prefix: str = "",
        extension: Optional[str] = None,
        max_files: Optional[int] = None,
    ) -> List[str]:
        self.logger.info(f"Listing files: s3://{bucket}/{prefix}")

        result = []
        truncated = True
        token = None

        while truncated:
            params = {"Bucket": bucket, "Prefix": prefix}
            if token:
                params["ContinuationToken"] = token

            try:
                response = self.client.list_objects_v2(**params)
            except Exception as e:
                self.logger.error(f"Failed to list objects: {e}")
                raise

            for obj in response.get("Contents", []):
                key = obj["Key"]

                # Apply extension filter
                if extension and not key.endswith(f".{extension}"):
                    continue

                result.append(key)

                # Check max files limit
                if max_files and len(result) >= max_files:
                    self.logger.info(f"Reached max_files limit ({max_files})")
                    return result

            truncated = response.get("IsTruncated", False)
            token = response.get("NextContinuationToken")

        self.logger.info(f"Found {len(result)} files")
        return result

    def download(self, bucket: str, key: str) -> bytes:
        self.logger.debug(f"Downloading: s3://{bucket}/{key}")

        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            data = response["Body"].read()
            data = _decode_http_chunked_if_present(data, self.logger)
            self.logger.debug(f"Downloaded {len(data)} bytes")
            return data
        except Exception as e:
            self.logger.error(f"Failed to download: {e}")
            raise

    def upload(
        self, data: bytes, bucket: str, key: str, content_type: Optional[str] = None
    ) -> None:
        self.logger.debug(f"Uploading {len(data)} bytes to s3://{bucket}/{key}")

        try:
            params = {"Bucket": bucket, "Key": key, "Body": data}
            if content_type:
                params["ContentType"] = content_type

            self.client.put_object(**params)
            self.logger.debug("Upload completed")
        except Exception as e:
            self.logger.error(f"Failed to upload: {e}")
            raise

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False
