"""Minimal boto3 wrapper for job-side list/get/put helpers."""

import logging
from typing import Any, Literal
from urllib.parse import urlparse

import boto3
from botocore.client import Config as BotoConfig


def _decode_http_chunked_if_present(data: bytes, logger: logging.Logger) -> bytes:
    """If `data` is HTTP chunked-transfer encoded (stored by buggy S3-compatible
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
        size_part = line.split(b";")[0].strip() if b";" in line else line.strip()
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
        logger.debug("Decoded HTTP chunked body (%s bytes payload)", len(out))
        return bytes(out)
    return data


class S3Client:
    """Thin boto3 S3 wrapper for job code (list, download, upload, head)."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        max_retries: int = 30,
        timeout: int = 360,
        logger: logging.Logger | None = None,
        *,
        region_name: str | None = None,
        boto_config: BotoConfig | None = None,
    ) -> None:
        """Build a boto3 S3 client for the given endpoint and credentials.

        Args:
            endpoint: S3 API endpoint URL (e.g. from ``S3_ENDPOINT``).
            access_key: Access key id for this client.
            secret_key: Secret access key for this client.
            max_retries: Boto3 retry ``max_attempts`` when ``boto_config`` is omitted.
            timeout: Read timeout in seconds when ``boto_config`` is omitted.
            logger: Optional logger; defaults to the module logger.
            region_name: Optional AWS region passed to ``boto3.client``.
            boto_config: If set, used as-is instead of the default ``BotoConfig``.

        """
        self.logger = logger or logging.getLogger(__name__)

        if boto_config is None:
            config = BotoConfig(
                s3={"addressing_style": "virtual"},
                retries={"max_attempts": max_retries, "mode": "standard"},
                read_timeout=timeout,
                max_pool_connections=1,
            )
        else:
            config = boto_config

        client_kwargs: dict[str, Any] = {
            "service_name": "s3",
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint,
            "config": config,
        }
        if region_name is not None:
            client_kwargs["region_name"] = region_name
        self.client = boto3.client(**client_kwargs)

        self.logger.debug("S3 client initialized: %s", endpoint)

    @staticmethod
    def parse_s3_uri(uri: str) -> tuple[str, str]:
        """Split ``s3://bucket/key/path`` into ``(bucket, key)``.

        Args:
            uri: S3 URI with non-empty bucket and key path.

        Returns:
            ``(bucket, key)`` where ``key`` has no leading slash.

        Raises:
            ValueError: If the URI is not a valid S3 URI.

        """
        u = urlparse(uri)
        if u.scheme != "s3" or not u.netloc or not u.path:
            msg = f"Bad s3 uri: {uri}"
            raise ValueError(msg)
        return u.netloc, u.path.lstrip("/")

    @staticmethod
    def create(
        secrets: dict[str, str], client_type: Literal["download", "upload"] = "download"
    ) -> "S3Client":
        """Create S3 client from secrets dictionary.

        Args:
            secrets: Dictionary containing S3 credentials. Expected keys:
                    - S3_ENDPOINT
                    - S3_DOWNLOAD_ACCESS_KEY
                    - S3_DOWNLOAD_SECRET_KEY
                    - S3_UPLOAD_ACCESS_KEY
                    - S3_UPLOAD_SECRET_KEY
            client_type: ``download`` or ``upload`` (default: ``download``).

        Returns:
            Configured ``S3Client`` instance.

        Raises:
            ValueError: If ``client_type`` is unknown or required secrets are missing.

        """
        if client_type == "upload":
            access_key = secrets.get("S3_UPLOAD_ACCESS_KEY")
            secret_key = secrets.get("S3_UPLOAD_SECRET_KEY")
        elif client_type == "download":
            access_key = secrets.get("S3_DOWNLOAD_ACCESS_KEY")
            secret_key = secrets.get("S3_DOWNLOAD_SECRET_KEY")
        else:
            msg = f"Unknown client type: {client_type}"
            raise ValueError(msg)

        endpoint = secrets.get("S3_ENDPOINT")

        if not endpoint:
            msg = "S3_ENDPOINT is not set"
            raise ValueError(msg)
        if not access_key:
            msg = f"S3_{client_type.upper()}_ACCESS_KEY is not set"
            raise ValueError(msg)
        if not secret_key:
            msg = f"S3_{client_type.upper()}_SECRET_KEY is not set"
            raise ValueError(msg)

        return S3Client(endpoint=endpoint, access_key=access_key, secret_key=secret_key)

    def list_files(
        self,
        bucket: str,
        prefix: str = "",
        extension: str | None = None,
        max_files: int | None = None,
    ) -> list[str]:
        """List object keys under ``prefix`` in ``bucket``.

        Args:
            bucket: Bucket name.
            prefix: Key prefix filter; use ``""`` to list from the bucket root.
            extension: If set, keep only keys ending with ``.<extension>``.
            max_files: If set, stop after this many keys (best-effort).

        Returns:
            List of S3 object keys (not full ``s3://`` URIs).

        Raises:
            Exception: Propagates boto3/client errors after logging.

        """
        self.logger.info("Listing files: s3://%s/%s", bucket, prefix)

        result = []
        truncated = True
        token = None

        while truncated:
            params = {"Bucket": bucket, "Prefix": prefix}
            if token:
                params["ContinuationToken"] = token

            try:
                response = self.client.list_objects_v2(**params)
            except Exception:
                self.logger.exception("Failed to list objects")
                raise

            for obj in response.get("Contents", []):
                key = obj["Key"]

                # Apply extension filter
                if extension and not key.endswith(f".{extension}"):
                    continue

                result.append(key)

                # Check max files limit
                if max_files and len(result) >= max_files:
                    self.logger.info("Reached max_files limit (%s)", max_files)
                    return result

            truncated = response.get("IsTruncated", False)
            token = response.get("NextContinuationToken")

        self.logger.info("Found %s files", len(result))
        return result

    def download(self, bucket: str, key: str) -> bytes:
        """Download one object body as bytes.

        Args:
            bucket: Bucket name.
            key: Object key.

        Returns:
            Raw object bytes (HTTP-chunked payloads may be normalized).

        Raises:
            Exception: Propagates boto3/client errors after logging.

        """
        self.logger.debug("Downloading: s3://%s/%s", bucket, key)

        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            data = response["Body"].read()
            data = _decode_http_chunked_if_present(data, self.logger)
            self.logger.debug("Downloaded %s bytes", len(data))
            return data
        except Exception:
            self.logger.exception("Failed to download")
            raise

    def download_by_uri(self, s3_uri: str) -> bytes:
        """Download object bytes from ``s3://bucket/key``.

        Args:
            s3_uri: Valid S3 URI.

        Returns:
            Same as ``download``.

        Raises:
            ValueError: If ``s3_uri`` is invalid (via ``parse_s3_uri``).
            Exception: Propagates boto3/client errors from ``download``.

        """
        bucket, key = S3Client.parse_s3_uri(s3_uri)
        return self.download(bucket, key)

    def upload(
        self, data: bytes, bucket: str, key: str, content_type: str | None = None
    ) -> None:
        """Upload bytes to ``s3://bucket/key`` via ``put_object``.

        Args:
            data: Object body.
            bucket: Bucket name.
            key: Object key.
            content_type: Optional ``ContentType`` header.

        Raises:
            Exception: Propagates boto3/client errors after logging.

        """
        self.logger.debug("Uploading %s bytes to s3://%s/%s", len(data), bucket, key)

        try:
            params = {"Bucket": bucket, "Key": key, "Body": data}
            if content_type:
                params["ContentType"] = content_type

            self.client.put_object(**params)
            self.logger.debug("Upload completed")
        except Exception:
            self.logger.exception("Failed to upload")
            raise

    def exists(self, bucket: str, key: str) -> bool:
        """Return whether an object exists (``head_object`` succeeds).

        Args:
            bucket: Bucket name.
            key: Object key.

        Returns:
            ``True`` if the object exists; ``False`` on any ``head_object`` error.

        """
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False
