"""Parse stderr text from YT job error payloads."""


def stderr_from_job_dict(first: object) -> str | None:
    """Return stderr text from a YT ``stderrs[0]`` dict, or ``None``."""
    if not isinstance(first, dict):
        return None
    err = first.get("error")
    if not isinstance(err, dict):
        return None
    inner = err.get("attributes")
    if not isinstance(inner, dict):
        return None
    text = inner.get("stderr", "")
    return str(text) if text else None


def stderr_text_from_yt_attrs(exception: Exception) -> str | None:
    """Return stderr text from a YT Python exception ``attributes``, if any."""
    if not hasattr(exception, "attributes"):
        return None
    attrs = exception.attributes  # pyright: ignore[reportAttributeAccessIssue]
    if "stderrs" not in attrs:
        return None
    stderrs = attrs["stderrs"]
    if not stderrs or len(stderrs) == 0:
        return None
    return stderr_from_job_dict(stderrs[0])
