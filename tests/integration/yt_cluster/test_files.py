"""upload_file and upload_directory against Cypress (real cluster)."""

from __future__ import annotations

import pytest


@pytest.mark.yt_cluster
def test_upload_file_and_upload_directory(
    yt_case_prefix: str, yt_client, local_case_dir
) -> None:
    f = local_case_dir / "hello.txt"
    f.write_text("cluster-upload", encoding="utf-8")
    yt_path = f"{yt_case_prefix}/files/hello.txt"
    yt_client.upload_file(f, yt_path, create_parent_dir=True)
    assert yt_client.exists(yt_path)

    sub = local_case_dir / "tree"
    (sub / "a").mkdir(parents=True)
    (sub / "a" / "one.bin").write_bytes(b"\x00\x01")
    (sub / "b.txt").write_text("leaf", encoding="utf-8")
    yt_dir = f"{yt_case_prefix}/files/dir_up"
    uploaded = yt_client.upload_directory(sub, yt_dir, pattern="*")
    assert len(uploaded) >= 2, "expected at least two uploaded files"
    assert all(isinstance(p, str) and p.startswith(yt_dir) for p in uploaded)
