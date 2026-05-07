"""Tests for yt_framework.yt.factory.create_yt_client."""

from pathlib import Path

import pytest

from yt_framework.yt.client_dev import YTDevClient
from yt_framework.yt.client_prod import YTProdClient
from yt_framework.yt.factory import create_yt_client


def test_create_yt_client_prod_without_secrets_raises() -> None:
    with pytest.raises(ValueError, match="secrets are required for prod mode"):
        create_yt_client(mode="prod", secrets=None)


def test_create_yt_client_prod_with_proxy_and_token_returns_prod_client() -> None:
    client = create_yt_client(
        mode="prod",
        secrets={"YT_PROXY": "http://proxy", "YT_TOKEN": "tok"},
    )
    assert isinstance(client, YTProdClient), (
        "prod mode with secrets should yield YTProdClient"
    )


def test_create_yt_client_dev_returns_dev_client() -> None:
    client = create_yt_client(mode="dev")
    assert isinstance(client, YTDevClient), (
        "default / dev mode should yield YTDevClient"
    )


def test_create_yt_client_dev_accepts_str_pipeline_dir(tmp_path: Path) -> None:
    pipe = tmp_path / "pipeline"
    pipe.mkdir()
    client = create_yt_client(mode="dev", pipeline_dir=str(pipe))
    assert client.pipeline_dir == pipe.resolve(), (
        "str pipeline_dir should normalize to resolved Path"
    )
