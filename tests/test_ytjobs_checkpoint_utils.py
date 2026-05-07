"""Tests for ytjobs.checkpoint.utils (path logic and YT I/O via stubbed yt wrapper)."""

import json
import logging
from unittest.mock import MagicMock

import pytest

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
    assert args[0] == "//root/cp/f.bin"
    assert args[1] == b"payload"


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


def test_save_checkpoint_logs_warning_when_base_directory_create_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_yt = MagicMock()
    mock_yt.create.side_effect = OSError("mkdir denied")
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    caplog.set_level(logging.WARNING)
    save_checkpoint(
        b"a", "n.bin", base_path="//x", logger=_silent_logger("test_ckpt_mkdir")
    )
    assert (
        "Could not create checkpoint directory" in caplog.text
        and mock_yt.write_file.call_count == 1
    ), "create failure is non-fatal; main blob write should still run"


def test_save_checkpoint_raises_when_main_blob_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.write_file.side_effect = RuntimeError("write denied")
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    with pytest.raises(RuntimeError, match="write denied"):
        save_checkpoint(
            b"a", "n.bin", base_path="//y", logger=_silent_logger("test_ckpt_wfail")
        )


def test_save_checkpoint_warns_when_metadata_write_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_yt = MagicMock()

    def _write(path: str, *_a, **_k) -> None:
        if str(path).endswith(".meta"):
            msg = "meta write fail"
            raise OSError(msg)

    mock_yt.write_file.side_effect = _write
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    caplog.set_level(logging.WARNING)
    path = save_checkpoint(
        b"b",
        "m.bin",
        base_path="//z",
        metadata={"a": 1},
        logger=_silent_logger("test_ckpt_meta_w"),
    )
    assert path == "//z/m.bin"
    assert "Failed to save checkpoint metadata" in caplog.text


def test_load_checkpoint_returns_none_tuple_when_read_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = True
    mock_yt.read_file.side_effect = OSError("read broke")
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    caplog.set_level(logging.ERROR)
    out = load_checkpoint(
        "x", base_path="//b", logger=_silent_logger("test_ld_read_exc")
    )
    assert out == (None, None)
    assert "Failed to load checkpoint" in caplog.text


def test_load_checkpoint_returns_body_and_warns_when_metadata_json_invalid(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.side_effect = [True, True]
    body_buf = MagicMock()
    body_buf.read.return_value = b"body"
    meta_buf = MagicMock()
    meta_buf.read.return_value = b"not-json{"
    mock_yt.read_file.side_effect = [body_buf, meta_buf]
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    caplog.set_level(logging.WARNING)
    out = load_checkpoint(
        "p.bin", base_path="//z", logger=_silent_logger("test_ld_bad_meta")
    )
    assert out == (b"body", None)
    assert "Failed to load checkpoint metadata" in caplog.text


def test_list_checkpoints_returns_empty_when_list_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = True
    mock_yt.list.side_effect = RuntimeError("list failed")
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    caplog.set_level(logging.ERROR)
    assert (
        list_checkpoints(base_path="//d", logger=_silent_logger("test_list_exc")) == []
    )
    assert "Failed to list checkpoints" in caplog.text


def test_delete_checkpoint_returns_false_when_remove_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = True
    mock_yt.remove.side_effect = OSError("rm failed")
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    assert (
        delete_checkpoint("c", base_path="//d", logger=_silent_logger("test_del_exc"))
        is False
    )


def test_load_processing_state_returns_none_when_json_unmarshalling_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_yt = MagicMock()
    mock_yt.exists.return_value = True
    buf = MagicMock()
    buf.read.return_value = b"not-json"
    mock_yt.read_file.return_value = buf
    monkeypatch.setattr(ckpt_utils, "yt", mock_yt)
    caplog.set_level(logging.ERROR)
    out = load_processing_state(
        state_name="bad", base_path="//p", logger=_silent_logger("test_lps_badjson")
    )
    assert out is None
    assert "Failed to parse processing state" in caplog.text
