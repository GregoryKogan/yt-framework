"""Tests for ytjobs.checkpoint.utils (path logic and YT I/O via stubbed yt wrapper)."""

import json
import logging

import pytest
from unittest.mock import MagicMock

import ytjobs.checkpoint.utils as ckpt_utils
from ytjobs.checkpoint.utils import (
    delete_checkpoint,
    get_checkpoint_path,
    list_checkpoints,
    load_checkpoint,
    load_processing_state,
    save_checkpoint,
    save_processing_state,
)


def _silent_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.addHandler(logging.NullHandler())
    return log


def test_get_checkpoint_path_joins_explicit_base_and_name() -> None:
    assert (
        get_checkpoint_path("state.bin", "//yt/base/here") == "//yt/base/here/state.bin"
    )


def test_get_checkpoint_path_uses_yt_user_environment_when_base_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("YT_USER", "alice")
    assert get_checkpoint_path("ckpt.dat") == "//home/alice/checkpoints/ckpt.dat"


def test_get_checkpoint_path_falls_back_to_default_user_when_yt_user_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("YT_USER", raising=False)
    assert get_checkpoint_path("x") == "//home/default/checkpoints/x"


def test_save_checkpoint_writes_payload_at_joined_yt_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    path = save_checkpoint(
        b"payload",
        "f.bin",
        base_path="//root/cp",
        logger=_silent_logger("test_save_ckpt"),
    )
    assert path == "//root/cp/f.bin", "save_checkpoint must return full YT path"
    mock_yt.write_file.assert_called_once()
    args, _kwargs = mock_yt.write_file.call_args
    assert args[0] == "//root/cp/f.bin" and args[1] == b"payload"


def test_save_checkpoint_writes_metadata_sidecar_after_main_blob(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    save_checkpoint(
        b"x",
        "g.bin",
        base_path="//r/c",
        metadata={"v": 1},
        logger=_silent_logger("test_save_meta"),
    )
    paths = [c[0][0] for c in mock_yt.write_file.call_args_list]
    assert paths == ["//r/c/g.bin", "//r/c/g.bin.meta"]


def test_load_checkpoint_returns_none_tuple_when_primary_path_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = False
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    out = load_checkpoint(
        "nope", base_path="//b", logger=_silent_logger("test_ld_miss")
    )
    assert out == (None, None)


def test_load_checkpoint_returns_body_without_metadata_when_meta_sidecar_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.side_effect = [True, False]
    data_buf = MagicMock()
    data_buf.read.return_value = b"body"
    mock_yt.read_file.return_value = data_buf
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    out = load_checkpoint(
        "p.bin", base_path="//z", logger=_silent_logger("test_ld_body")
    )
    assert out == (b"body", None)


def test_load_checkpoint_returns_body_and_decoded_metadata_when_meta_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.side_effect = [True, True]
    data_buf = MagicMock()
    data_buf.read.return_value = b"body"
    meta_buf = MagicMock()
    meta_buf.read.return_value = json.dumps({"k": "v"}).encode("utf-8")

    def _read(path: str):
        if path.endswith(".meta"):
            return meta_buf
        return data_buf

    mock_yt.read_file.side_effect = _read
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    out = load_checkpoint(
        "p.bin", base_path="//z", logger=_silent_logger("test_ld_full")
    )
    assert out == (b"body", {"k": "v"})


def test_list_checkpoints_returns_empty_list_when_directory_node_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = False
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    assert (
        list_checkpoints(base_path="//q/dir", logger=_silent_logger("test_list_miss"))
        == []
    )


def test_list_checkpoints_drops_meta_files_and_applies_name_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = True
    mock_yt.list.return_value = ["a.bin", "a.bin.meta", "b.bin", "noise.txt"]
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    out = list_checkpoints(
        base_path="//q/dir", pattern=".bin", logger=_silent_logger("test_list_pat")
    )
    assert out == ["a.bin", "b.bin"]


def test_delete_checkpoint_returns_true_when_no_checkpoint_nodes_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = False
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    assert (
        delete_checkpoint(
            "ghost", base_path="//g", logger=_silent_logger("test_del_miss")
        )
        is True
    )


def test_delete_checkpoint_removes_data_file_and_metadata_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = True
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    assert (
        delete_checkpoint(
            "d.bin", base_path="//g", logger=_silent_logger("test_del_ok")
        )
        is True
        and mock_yt.remove.call_count == 2
    ), "checkpoint body and .meta should each be removed when they exist"


def test_save_processing_state_writes_json_named_checkpoint_under_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    path = save_processing_state(
        {"n": 1}, state_name="st", base_path="//p", logger=_silent_logger("test_sps")
    )
    assert path == "//p/st.json"


def test_load_processing_state_returns_none_when_checkpoint_blob_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = False
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    assert (
        load_processing_state(
            state_name="missing",
            base_path="//m",
            logger=_silent_logger("test_lps_miss"),
        )
        is None
    )


def test_load_processing_state_returns_decoded_dict_from_json_blob(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = True
    buf = MagicMock()
    buf.read.return_value = b'{"ok": true}'
    mock_yt.read_file.return_value = buf
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    assert load_processing_state(
        state_name="s", base_path="//m", logger=_silent_logger("test_lps_ok")
    ) == {"ok": True}
