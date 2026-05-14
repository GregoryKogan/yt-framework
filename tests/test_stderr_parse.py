"""Tests for yt_framework.yt.clients.stderr_parse."""

from typing import ClassVar

from yt_framework.yt.clients.stderr_parse import (
    stderr_from_job_dict,
    stderr_text_from_yt_attrs,
)


def test_stderr_from_job_dict_returns_none_when_not_dict() -> None:
    assert stderr_from_job_dict("x") is None


def test_stderr_from_job_dict_returns_none_when_error_not_dict() -> None:
    assert stderr_from_job_dict({"error": "not-a-dict"}) is None


def test_stderr_from_job_dict_returns_none_when_attributes_not_dict() -> None:
    assert stderr_from_job_dict({"error": {"attributes": "bad"}}) is None


def test_stderr_from_job_dict_returns_none_when_stderr_empty() -> None:
    assert stderr_from_job_dict({"error": {"attributes": {"stderr": ""}}}) is None


def test_stderr_from_job_dict_returns_text_when_present() -> None:
    assert stderr_from_job_dict({"error": {"attributes": {"stderr": "oops"}}}) == "oops"


def test_stderr_text_from_yt_attrs_returns_none_when_no_attributes() -> None:
    assert stderr_text_from_yt_attrs(ValueError("plain")) is None


def test_stderr_text_from_yt_attrs_returns_none_when_no_stderrs_key() -> None:
    class _FakeYtAttrsError(Exception):
        attributes: ClassVar[dict[str, object]] = {"other": 1}

    assert stderr_text_from_yt_attrs(_FakeYtAttrsError()) is None


def test_stderr_text_from_yt_attrs_returns_none_when_stderrs_empty() -> None:
    class _FakeYtAttrsError(Exception):
        attributes: ClassVar[dict[str, object]] = {"stderrs": []}

    assert stderr_text_from_yt_attrs(_FakeYtAttrsError()) is None


def test_stderr_text_from_yt_attrs_delegates_to_first_stderr_dict() -> None:
    class _FakeYtAttrsError(Exception):
        attributes: ClassVar[dict[str, object]] = {
            "stderrs": [{"error": {"attributes": {"stderr": "line1\\nline2"}}}],
        }

    assert stderr_text_from_yt_attrs(_FakeYtAttrsError()) == "line1\\nline2"


def test_stderr_text_from_yt_attrs_returns_none_when_first_stderr_invalid() -> None:
    class _FakeYtAttrsError(Exception):
        attributes: ClassVar[dict[str, object]] = {"stderrs": ["not-a-dict"]}

    assert stderr_text_from_yt_attrs(_FakeYtAttrsError()) is None


def test_stderr_from_job_dict_coerces_non_string_stderr_to_str() -> None:
    assert stderr_from_job_dict({"error": {"attributes": {"stderr": 42}}}) == "42"
