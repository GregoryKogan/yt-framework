"""ytjobs.checkpoint helpers against the configured cell (global yt client)."""

from __future__ import annotations

import pytest

from ytjobs.checkpoint.utils import load_checkpoint, save_checkpoint


@pytest.mark.yt_cluster
def test_save_and_load_checkpoint_roundtrip(yt_case_prefix: str) -> None:
    base = f"{yt_case_prefix}/checkpoints"
    name = "smoke.bin"
    payload = b"\x01\x02cluster-checkpoint"
    meta = {"step": 1}
    path = save_checkpoint(
        data=payload,
        checkpoint_name=name,
        metadata=meta,
        base_path=base,
    )
    assert path.endswith(name)
    data, loaded_meta = load_checkpoint(name, base_path=base)
    assert data == payload
    assert loaded_meta == meta
