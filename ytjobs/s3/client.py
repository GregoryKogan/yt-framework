"""Minimal boto3 wrapper for job-side list/get/put helpers."""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Literal
from urllib.parse import urlparse

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError


def _parse_chunk_size(line: bytes) -> int | None:
    size_part = line.split(b";")[0].strip() if b";" in line else line.strip()
    try:
        return int(size_part, 16)
    except ValueError:
        return None


def _skip_crlf_after_chunk(data: bytes, i: int) -> int:
    while i < len(data) and data[i : i + 1] in (b"\r", b"\n"):
        i += 1
    return i


def _try_decode_next_chunk(data: bytes, i: int) -> tuple[int, bytes] | None:
    line_end = data.find(b"\n", i)
    if line_end < 0:
        return None
    line = data[i:line_end].strip(b"\r")
    size = _parse_chunk_size(line)
    if size is None:
        return None
    i = line_end + 1
    if size == 0:
        return None
    if i + size > len(data):
        return None
    chunk = bytes(data[i : i + size])
    i += size
    i = _skip_crlf_after_chunk(data, i)
    return i, chunk


def _decode_all_http_chunks(data: bytes) -> bytes:
    out = bytearray()
    i = 0
    while i < len(data):
        step = _try_decode_next_chunk(data, i)
        if step is None:
            break
        i, chunk = step
        out.extend(chunk)
    return bytes(out)


def _decode_http_chunked_if_present(data: bytes, logger: logging.Logger) -> bytes:
    """Decode HTTP chunked-transfer payloads when present, else return ``data``.

    Some S3-compatible backends store chunked-encoded bodies from dev/local uploads;
    strip framing and return the raw payload. If ``data`` is not chunked, return it
    unchanged.
    """
    if not data or data[0:1] not in b"0123456789abcdefABCDEF":
        return data
    decoded = _decode_all_http_chunks(data)
    if not decoded:
        return data
    logger.debug("Decoded HTTP chunked body (%s bytes payload)", len(decoded))
    return decoded


@dataclass(frozen=True)
class S3ClientOptions:
    """Optional runtime knobs for `S3Client` construction."""

    max_retries: int = 30
    timeout: int = 360
    logger: logging.Logger | None = None
    region_name: str | None = None
    boto_config: BotoConfig | None = None


def _coerce_int_option(value: object, option_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{option_name} must be an integer, got {value!r}"
        raise TypeError(msg)
    return value


_LEGACY_S3_INIT_KEYS = frozenset(
    {
        "max_retries",
        "timeout",
        "logger",
        "region_name",
        "boto_config",
    },
)


def _reject_unknown_s3_legacy_kwargs(legacy_kwargs: dict[str, object]) -> None:
    unknown = set(legacy_kwargs) - _LEGACY_S3_INIT_KEYS
    if unknown:
        msg = f"Unknown S3Client init option(s): {sorted(unknown)}"
        raise TypeError(msg)


def _replace_options_with_legacy_kwargs(
    merged_options: S3ClientOptions,
    legacy_kwargs: dict[str, object],
) -> S3ClientOptions:
    return replace(
        merged_options,
        max_retries=_coerce_int_option(
            legacy_kwargs.get("max_retries", merged_options.max_retries),
            "max_retries",
        ),
        timeout=_coerce_int_option(
            legacy_kwargs.get("timeout", merged_options.timeout),
            "timeout",
        ),
        logger=legacy_kwargs.get("logger", merged_options.logger),
        region_name=(
            str(legacy_kwargs["region_name"])
            if "region_name" in legacy_kwargs
            and legacy_kwargs["region_name"] is not None
            else merged_options.region_name
        ),
        boto_config=legacy_kwargs.get("boto_config", merged_options.boto_config),
    )


def _options_from_legacy_kwargs(
    options: S3ClientOptions | None,
    legacy_kwargs: dict[str, object],
) -> S3ClientOptions:
    """Build options from explicit object plus backward-compatible kwargs."""
    merged_options = options or S3ClientOptions()
    if not legacy_kwargs:
        return merged_options

    _reject_unknown_s3_legacy_kwargs(legacy_kwargs)
    return _replace_options_with_legacy_kwargs(merged_options, legacy_kwargs)


def _append_single_listed_key(
    result: list[str],
    key: str,
    *,
    extension: str | None,
    max_files: int | None,
) -> bool:
    """Append key if it passes extension filter; return True if max_files reached."""
    if extension and not key.endswith(f".{extension}"):
        return False
    result.append(key)
    return max_files is not None and len(result) >= max_files


def _append_keys_until_limit(
    result: list[str],
    contents: Sequence[Mapping[str, Any]],
    *,
    extension: str | None,
    max_files: int | None,
    logger: logging.Logger,
) -> bool:
    """Append keys from one list_objects_v2 page; return True if max_files reached."""
    for obj in contents:
        if _append_single_listed_key(
            result,
            obj["Key"],
            extension=extension,
            max_files=max_files,
        ):
            logger.info("Reached max_files limit (%s)", max_files)
            return True
    return False


class S3Client:
    """Thin boto3 S3 wrapper for job code (list, download, upload, head)."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        *,
        options: S3ClientOptions | None = None,
        **legacy_kwargs: object,
    ) -> None:
        """Build a boto3 S3 client for the given endpoint and credentials.

        Args:
            endpoint: S3 API endpoint URL (e.g. from ``S3_ENDPOINT``).
            access_key: Access key id for this client.
            secret_key: Secret access key for this client.
            options: Optional strongly-typed client options.
            **legacy_kwargs: Backward-compatible aliases for old init kwargs
                (``max_retries``, ``timeout``, ``logger``, ``region_name``, ``boto_config``).

        """
        resolved_options = _options_from_legacy_kwargs(options, legacy_kwargs)
        self.logger = resolved_options.logger or logging.getLogger(__name__)

        if resolved_options.boto_config is None:
            config = BotoConfig(
                s3={"addressing_style": "virtual"},
                retries={
                    "max_attempts": resolved_options.max_retries,
                    "mode": "standard",
                },
                read_timeout=resolved_options.timeout,
                max_pool_connections=1,
            )
        else:
            config = resolved_options.boto_config

        client_kwargs: dict[str, Any] = {
            "service_name": "s3",
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint,
            "config": config,
        }
        if resolved_options.region_name is not None:
            client_kwargs["region_name"] = resolved_options.region_name
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
    def _access_keys_for_client_type(
        secrets: dict[str, str],
        client_type: Literal["download", "upload"],
    ) -> tuple[str | None, str | None]:
        if client_type == "upload":
            return secrets.get("S3_UPLOAD_ACCESS_KEY"), secrets.get(
                "S3_UPLOAD_SECRET_KEY"
            )
        if client_type == "download":
            return secrets.get("S3_DOWNLOAD_ACCESS_KEY"), secrets.get(
                "S3_DOWNLOAD_SECRET_KEY"
            )
        msg = f"Unknown client type: {client_type}"
        raise ValueError(msg)

    @staticmethod
    def _require_s3_connection_secrets(
        *,
        endpoint: object,
        access_key: object,
        secret_key: object,
        client_type: Literal["download", "upload"],
    ) -> tuple[str, str, str]:
        if not endpoint:
            msg = "S3_ENDPOINT is not set"
            raise ValueError(msg)
        if not access_key:
            msg = f"S3_{client_type.upper()}_ACCESS_KEY is not set"
            raise ValueError(msg)
        if not secret_key:
            msg = f"S3_{client_type.upper()}_SECRET_KEY is not set"
            raise ValueError(msg)
        return str(endpoint), str(access_key), str(secret_key)

    @staticmethod
    def create(
        secrets: dict[str, str],
        client_type: Literal["download", "upload"] = "download",
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
        access_key, secret_key = S3Client._access_keys_for_client_type(
            secrets, client_type
        )
        endpoint = secrets.get("S3_ENDPOINT")
        ep, ak, sk = S3Client._require_s3_connection_secrets(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            client_type=client_type,
        )
        return S3Client(endpoint=ep, access_key=ak, secret_key=sk)

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

            if _append_keys_until_limit(
                result,
                response.get("Contents", []),
                extension=extension,
                max_files=max_files,
                logger=self.logger,
            ):
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
        except Exception:
            self.logger.exception("Failed to download")
            raise
        else:
            return data

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
        self,
        data: bytes,
        bucket: str,
        key: str,
        content_type: str | None = None,
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
        except Exception:
            self.logger.exception("Failed to upload")
            raise
        else:
            self.logger.debug("Upload completed")

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
        except (ClientError, OSError):
            return False
        else:
            return True
