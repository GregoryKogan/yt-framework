"""Tests for ytjobs.s3.client (parse_s3_uri lives in test_ytjobs_s3_uri)."""

import logging
from unittest.mock import MagicMock

import pytest

import ytjobs.s3.client as s3_mod
from ytjobs.s3.client import S3Client, _decode_http_chunked_if_present


def _silent(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


def test_decode_http_chunked_if_present_returns_original_when_not_chunked_prefix() -> (
    None
):
    log = _silent("t.s3.dec1")
    raw = b"not-hex-started-body"
    assert _decode_http_chunked_if_present(raw, log) is raw


def test_decode_http_chunked_if_present_decodes_simple_chunked_body() -> None:
    log = _silent("t.s3.dec2")
    raw = b"4\r\nWiki\r\n0\r\n\r\n"
    assert _decode_http_chunked_if_present(raw, log) == b"Wiki"


def test_decode_http_chunked_if_present_returns_original_when_parsing_yields_no_payload() -> (
    None
):
    log = _silent("t.s3.dec3")
    raw = b"z\r\n"
    assert _decode_http_chunked_if_present(raw, log) == raw


def test_decode_http_chunked_if_present_returns_original_when_no_chunk_terminator() -> (
    None
):
    log = _silent("t.s3.dec4")
    raw = b"a"
    assert _decode_http_chunked_if_present(raw, log) is raw


def test_decode_http_chunked_if_present_parses_size_before_semicolon_extension() -> (
    None
):
    log = _silent("t.s3.dec5")
    raw = b"4;foo=bar\r\nWiki\r\n0\r\n\r\n"
    assert _decode_http_chunked_if_present(raw, log) == b"Wiki"


def test_decode_http_chunked_if_present_returns_original_when_hex_size_invalid() -> (
    None
):
    log = _silent("t.s3.dec6")
    # Starts with hex digit so the decoder enters; size line is not valid hex.
    raw = b"0g\r\n"
    assert _decode_http_chunked_if_present(raw, log) is raw


def test_decode_http_chunked_if_present_returns_original_when_chunk_truncated() -> None:
    log = _silent("t.s3.dec7")
    raw = b"10\r\na"
    assert _decode_http_chunked_if_present(raw, log) is raw


def test_decode_http_chunked_if_present_returns_original_for_zero_size_terminator_only() -> (
    None
):
    log = _silent("t.s3.dec8")
    raw = b"0\r\n"
    assert _decode_http_chunked_if_present(raw, log) is raw


def test_s3_client_create_from_download_secrets_builds_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    monkeypatch.setattr(s3_mod, "boto3", MagicMock(client=mock_boto))
    client = S3Client.create(
        {
            "S3_ENDPOINT": "https://s3.example",
            "S3_DOWNLOAD_ACCESS_KEY": "dk",
            "S3_DOWNLOAD_SECRET_KEY": "ds",
        },
        client_type="download",
    )
    assert isinstance(client, S3Client)
    mock_boto.assert_called_once()
    kwargs = mock_boto.call_args.kwargs
    assert kwargs["endpoint_url"] == "https://s3.example"
    assert kwargs["aws_access_key_id"] == "dk"


def test_s3_client_create_from_upload_secrets_uses_upload_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    monkeypatch.setattr(s3_mod, "boto3", MagicMock(client=mock_boto))
    S3Client.create(
        {
            "S3_ENDPOINT": "https://s3.example",
            "S3_UPLOAD_ACCESS_KEY": "uk",
            "S3_UPLOAD_SECRET_KEY": "us",
        },
        client_type="upload",
    )
    assert mock_boto.call_args.kwargs["aws_access_key_id"] == "uk"


def test_s3_client_create_raises_on_unknown_client_type() -> None:
    with pytest.raises(ValueError, match="Unknown client type"):
        S3Client.create(
            {
                "S3_ENDPOINT": "x",
                "S3_DOWNLOAD_ACCESS_KEY": "a",
                "S3_DOWNLOAD_SECRET_KEY": "b",
            },
            client_type="unknown",  # type: ignore[arg-type]
        )


def test_s3_client_create_raises_when_endpoint_missing() -> None:
    with pytest.raises(ValueError, match="S3_ENDPOINT is not set"):
        S3Client.create(
            {
                "S3_DOWNLOAD_ACCESS_KEY": "a",
                "S3_DOWNLOAD_SECRET_KEY": "b",
            },
        )


def test_s3_client_create_raises_when_access_key_missing() -> None:
    with pytest.raises(ValueError, match="S3_DOWNLOAD_ACCESS_KEY is not set"):
        S3Client.create(
            {
                "S3_ENDPOINT": "https://x",
                "S3_DOWNLOAD_SECRET_KEY": "b",
            },
        )


def test_s3_client_create_raises_when_upload_secret_key_missing() -> None:
    with pytest.raises(ValueError, match="S3_UPLOAD_SECRET_KEY is not set"):
        S3Client.create(
            {
                "S3_ENDPOINT": "https://x",
                "S3_UPLOAD_ACCESS_KEY": "uk",
            },
            client_type="upload",
        )


def test_s3_client_list_files_paginates_and_filters_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    inner.list_objects_v2.side_effect = [
        {
            "Contents": [{"Key": "a.txt"}, {"Key": "b.bin"}],
            "IsTruncated": True,
            "NextContinuationToken": "tok1",
        },
        {"Contents": [{"Key": "c.txt"}], "IsTruncated": False},
    ]
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.list"))
    keys = client.list_files("bucket", prefix="p/", extension="txt")
    assert keys == ["a.txt", "c.txt"]
    assert inner.list_objects_v2.call_count == 2


def test_s3_client_list_files_stops_at_max_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    inner.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "a.txt"},
            {"Key": "b.txt"},
            {"Key": "c.txt"},
        ],
        "IsTruncated": False,
    }
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.max"))
    keys = client.list_files("bucket", max_files=2)
    assert keys == ["a.txt", "b.txt"]
    inner.list_objects_v2.assert_called_once()


def test_s3_client_list_files_raises_after_error_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    inner.list_objects_v2.side_effect = OSError("list down")
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.list_e"))
    with pytest.raises(OSError, match="list down"):
        client.list_files("b")


def test_s3_client_download_returns_decoded_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    body = MagicMock()
    body.read.return_value = b"4\r\nWiki\r\n0\r\n\r\n"
    inner.get_object.return_value = {"Body": body}
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.dl"))
    assert client.download("b", "k") == b"Wiki"


def test_s3_client_download_raises_after_logging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    inner.get_object.side_effect = RuntimeError("get fail")
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.dl_e"))
    with pytest.raises(RuntimeError, match="get fail"):
        client.download("b", "k")


def test_s3_client_download_by_uri_delegates_to_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    body = MagicMock()
    body.read.return_value = b"ok"
    inner.get_object.return_value = {"Body": body}
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.uri"))
    assert client.download_by_uri("s3://mybucket/path/to/obj") == b"ok"
    inner.get_object.assert_called_once_with(Bucket="mybucket", Key="path/to/obj")


def test_s3_client_upload_passes_content_type_when_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.up"))
    client.upload(b"x", "b", "k", content_type="text/plain")
    inner.put_object.assert_called_once_with(
        Bucket="b", Key="k", Body=b"x", ContentType="text/plain"
    )


def test_s3_client_upload_raises_after_logging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    inner.put_object.side_effect = OSError("put fail")
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.up_e"))
    with pytest.raises(OSError, match="put fail"):
        client.upload(b"x", "b", "k")


def test_s3_client_exists_returns_false_on_head_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    inner.head_object.side_effect = OSError("404")
    client = S3Client("https://e", "k", "s", logger=_silent("t.s3.ex"))
    assert client.exists("b", "missing") is False


def test_s3_client_exists_returns_true_when_head_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    inner = MagicMock()
    mock_boto.client.return_value = inner
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    assert (
        S3Client("https://e", "k", "s", logger=_silent("t.s3.ex2")).exists("b", "k")
        is True
    )


def test_s3_client_init_uses_provided_boto_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from botocore.client import Config as BotoConfig

    mock_boto = MagicMock()
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    cfg = BotoConfig()
    S3Client(
        "https://e",
        "k",
        "s",
        boto_config=cfg,
        logger=_silent("t.s3.cfg"),
    )
    passed = mock_boto.client.call_args.kwargs["config"]
    assert passed is cfg


def test_s3_client_init_passes_region_name_to_boto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_boto = MagicMock()
    monkeypatch.setattr(s3_mod, "boto3", mock_boto)
    S3Client(
        "https://e",
        "k",
        "s",
        region_name="eu-west-1",
        logger=_silent("t.s3.reg"),
    )
    assert mock_boto.client.call_args.kwargs["region_name"] == "eu-west-1"
