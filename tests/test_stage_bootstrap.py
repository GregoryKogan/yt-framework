"""Tests for yt_framework.typed_jobs.stage_bootstrap helpers and StageBootstrapTypedJob."""

import io
import os
import pickle
import sys
import tarfile
from pathlib import Path

import pytest

import yt_framework.typed_jobs.stage_bootstrap as sb
from yt_framework.typed_jobs import StageBootstrapTypedJob

_ENV_KEYS = (
    "JOB_CONFIG_PATH",
    "TOKENIZER_ARTIFACT_FILE",
    "TOKENIZER_ARTIFACT_DIR",
    "TOKENIZER_ARTIFACT_NAME",
    "YT_STAGE_NAME",
)


@pytest.fixture(autouse=True)
def _isolate_stage_bootstrap() -> None:
    path_before = sys.path[:]
    env_before = {k: os.environ.get(k) for k in _ENV_KEYS}
    sb._BOOTSTRAPPED_KEYS.clear()
    yield
    sys.path[:] = path_before
    sb._BOOTSTRAPPED_KEYS.clear()
    for key, val in env_before.items():
        if val is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = val


def test_find_source_tarball_root_returns_none_when_no_tarball(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert sb._find_source_tarball_root() is None, (
        "expected no tarball under search roots"
    )


def test_find_source_tarball_root_finds_tarball_in_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "source.tar.gz").touch()
    monkeypatch.chdir(tmp_path)
    assert sb._find_source_tarball_root() == str(tmp_path)


def test_find_source_tarball_root_skips_empty_path_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, "getcwd", lambda: "")
    assert sb._find_source_tarball_root() is None, (
        "empty cwd should yield only falsy candidates until /slot/sandbox check"
    )


@pytest.mark.skipif(os.name == "nt", reason="POSIX root parent walk")
def test_find_source_tarball_root_stops_parent_walk_at_filesystem_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir("/")
    assert sb._find_source_tarball_root() is None


@pytest.mark.skipif(os.name == "nt", reason="POSIX root parent walk")
def test_bootstrap_once_breaks_inner_parent_loop_at_root_and_uses_cwd_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir("/")
    sb._bootstrap_once("no_such_stage")
    assert "/" in sys.path, (
        "last-resort root should prepend filesystem root to sys.path"
    )


def test_find_source_tarball_root_walks_parents_for_tarball(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "jobroot"
    root.mkdir()
    deep = root / "nested" / "deep"
    deep.mkdir(parents=True)
    (root / "source.tar.gz").touch()
    monkeypatch.chdir(deep)
    assert sb._find_source_tarball_root() == str(root), (
        "tarball above cwd must be found"
    )


def test_bootstrap_once_inserts_workspace_and_stage_src_without_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "w"
    stage = "st_plain"
    src = workspace / "stages" / stage / "src"
    src.mkdir(parents=True)
    monkeypatch.chdir(src)
    sb._bootstrap_once(stage)
    assert str(workspace) in sys.path and str(src) in sys.path, (
        "root and stage src on path"
    )


def test_bootstrap_once_sets_job_config_path_when_config_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "w2"
    stage = "st_cfg"
    src = workspace / "stages" / stage / "src"
    src.mkdir(parents=True)
    cfg = workspace / "stages" / stage / "config.yaml"
    cfg.write_text("pipeline: {}\n", encoding="utf-8")
    monkeypatch.chdir(src)
    sb._bootstrap_once(stage)
    assert os.environ.get("JOB_CONFIG_PATH") == str(cfg), (
        "config path exported for worker"
    )


def test_bootstrap_once_extracts_source_tar_when_ytjobs_marker_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "w3"
    workspace.mkdir()
    tgz = workspace / "source.tar.gz"
    payload = b"# ytjobs\n"
    with tarfile.open(tgz, "w:gz") as tf:
        info = tarfile.TarInfo("ytjobs/__init__.py")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    monkeypatch.chdir(workspace)
    sb._bootstrap_once("any_stage")
    assert (workspace / "ytjobs" / "__init__.py").is_file(), (
        "archive should extract ytjobs"
    )


def test_bootstrap_once_extracts_tokenizer_artifact_and_sets_default_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "w4"
    stage = "tok_st"
    src = workspace / "stages" / stage / "src"
    src.mkdir(parents=True)
    art_rel = "tokenizer.tgz"
    art_abs = workspace / art_rel
    with tarfile.open(art_abs, "w:gz") as tf:
        info = tarfile.TarInfo("blob.bin")
        info.size = 0
        tf.addfile(info, io.BytesIO())
    monkeypatch.chdir(src)
    monkeypatch.setenv("TOKENIZER_ARTIFACT_FILE", art_rel)
    monkeypatch.delenv("TOKENIZER_ARTIFACT_DIR", raising=False)
    monkeypatch.delenv("TOKENIZER_ARTIFACT_NAME", raising=False)
    sb._bootstrap_once(stage)
    marker = workspace / "tokenizer_artifacts" / "default" / ".extracted"
    assert marker.is_file(), "tokenizer tarball should extract once"
    assert os.environ.get("TOKENIZER_ARTIFACT_DIR") == str(
        Path("tokenizer_artifacts") / "default",
    )


def test_bootstrap_once_does_not_reopen_tarball_on_second_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "w_idem"
    workspace.mkdir()
    tgz = workspace / "source.tar.gz"
    payload = b"# marker\n"
    with tarfile.open(tgz, "w:gz") as tf:
        info = tarfile.TarInfo("ytjobs/__init__.py")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    monkeypatch.chdir(workspace)
    stage = "idem_stage"
    opens: list[int] = []
    real_open = tarfile.open

    def _counting_open(*args: object, **kwargs: object) -> tarfile.TarFile:
        opens.append(1)
        return real_open(*args, **kwargs)

    monkeypatch.setattr(sb.tarfile, "open", _counting_open)
    sb._bootstrap_once(stage)
    n_after_first = len(opens)
    sb._bootstrap_once(stage)
    assert n_after_first >= 1 and len(opens) == n_after_first, (
        "first call may extract tarball; second call should short-circuit before open"
    )


def test_stage_bootstrap_typed_job_unpickle_runs_bootstrap_when_stage_name_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "w5"
    stage = "pickle_stage"
    (workspace / "stages" / stage / "src").mkdir(parents=True)
    monkeypatch.chdir(workspace)
    monkeypatch.setenv("YT_STAGE_NAME", stage)
    job = StageBootstrapTypedJob()
    pickle.loads(pickle.dumps(job))
    assert str(workspace) in sys.path, "unpickle should bootstrap PYTHONPATH root"


def test_safe_extractall_rejects_path_traversal_member(tmp_path: Path) -> None:
    dest = tmp_path / "dest"
    dest.mkdir()
    bad = tmp_path / "evil.tgz"
    with tarfile.open(bad, "w:gz") as tf:
        info = tarfile.TarInfo(name="../outside")
        info.size = 0
        tf.addfile(info, io.BytesIO())
    with tarfile.open(bad, "r:gz") as tf, pytest.raises(RuntimeError, match="outside"):
        sb._safe_extractall(tf, dest)


def test_scan_for_source_tarball_skips_empty_and_duplicate_bases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "source.tar.gz").touch()
    monkeypatch.chdir(tmp_path)
    base = str(tmp_path)
    assert sb._scan_for_source_tarball(["", base, base]) == base


def test_extract_tokenizer_returns_when_archive_file_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TOKENIZER_ARTIFACT_FILE", "missing.tgz")
    monkeypatch.setenv("TOKENIZER_ARTIFACT_DIR", "d")
    sb._extract_tokenizer_artifact_if_needed(str(tmp_path))


def test_extract_tokenizer_returns_when_marker_already_present(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path
    art = root / "tok.tgz"
    with tarfile.open(art, "w:gz") as tf:
        z = tarfile.TarInfo("x")
        z.size = 0
        tf.addfile(z, io.BytesIO())
    tdir = root / "tokenizer_artifacts" / "tok"
    tdir.mkdir(parents=True)
    (tdir / ".extracted").write_text("ok\n", encoding="utf-8")
    monkeypatch.setenv("TOKENIZER_ARTIFACT_FILE", "tok.tgz")
    monkeypatch.setenv("TOKENIZER_ARTIFACT_DIR", str(tdir.relative_to(root)))
    sb._extract_tokenizer_artifact_if_needed(str(root))
