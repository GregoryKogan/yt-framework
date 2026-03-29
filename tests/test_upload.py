"""Tests for yt_framework.operations.upload validation and path resolution."""

import logging
import sys
import tarfile
import types
from pathlib import Path
from typing import List, Tuple

import pytest

from yt_framework.operations.upload import (
    _copy_module_to_build_dir,
    _copy_path_to_build_dir,
    _copy_stage_to_build_dir,
    _create_map_reduce_command_wrappers,
    _create_wrappers_for_stage,
    _load_stage_job_section,
    _resolve_build_code_dir,
    _resolve_map_reduce_command_scripts,
    _resolve_reduce_command_script,
    _resolve_upload_target,
    _validate_upload_config,
    build_code_locally,
    create_code_archive,
    upload_all_code,
    upload_code_archive,
)
from yt_framework.yt.client_dev import YTDevClient


class _FakeUploadYtClient:
    """Records upload_file calls for upload_code_archive contract tests."""

    def __init__(self) -> None:
        self.upload_calls: List[Tuple[Path, str, bool]] = []

    def upload_file(
        self,
        local_path: Path,
        yt_path: str,
        create_parent_dir: bool = False,
    ) -> None:
        self.upload_calls.append((local_path, yt_path, create_parent_dir))


def test_resolve_upload_target_returns_stripped_explicit_target() -> None:
    out = _resolve_upload_target("/tmp/x", "  my_target  ", Path("/p"))
    assert out == "my_target"


def test_resolve_upload_target_uses_source_basename_when_target_empty() -> None:
    out = _resolve_upload_target("/data/extra", None, Path("/p"))
    assert out == "extra"


def test_validate_upload_config_rejects_reserved_target_from_upload_paths(
    tmp_path: Path,
) -> None:
    extra = tmp_path / "extra"
    extra.mkdir()
    with pytest.raises(ValueError, match="Reserved target name 'ytjobs'"):
        _validate_upload_config(
            upload_modules=None,
            upload_paths=[{"source": "extra", "target": "ytjobs"}],
            pipeline_dir=tmp_path,
        )


def test_validate_upload_config_rejects_source_outside_pipeline_dir(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="must be within pipeline directory"):
        _validate_upload_config(
            upload_modules=None,
            upload_paths=[{"source": "../outside"}],
            pipeline_dir=tmp_path,
        )


def test_validate_upload_config_rejects_upload_paths_entry_without_source_key(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="missing required 'source' key"):
        _validate_upload_config(
            upload_modules=None,
            upload_paths=[{"target": "only_target"}],
            pipeline_dir=tmp_path,
        )


def test_resolve_build_code_dir_removes_pre_existing_dot_build_tree(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.resolve_build")
    logger.addHandler(logging.NullHandler())
    stale = tmp_path / ".build" / "stale.txt"
    stale.parent.mkdir(parents=True)
    stale.write_text("old", encoding="utf-8")
    out = _resolve_build_code_dir(pipeline_dir=tmp_path, logger=logger)
    assert (
        out == tmp_path / ".build" and out.is_dir() and not stale.exists()
    ), "pre-existing .build must be replaced with a fresh directory"


def test_validate_upload_config_rejects_upload_modules_targeting_reserved_name() -> (
    None
):
    with pytest.raises(ValueError, match="Reserved target name 'stages'"):
        _validate_upload_config(
            upload_modules=["stages.something"],
            upload_paths=None,
            pipeline_dir=Path("/tmp/pipe"),
        )


def test_validate_upload_config_rejects_conflicting_upload_targets(
    tmp_path: Path,
) -> None:
    (tmp_path / "extra").mkdir()
    with pytest.raises(ValueError, match="Upload target conflict"):
        _validate_upload_config(
            upload_modules=["foo_pkg"],
            upload_paths=[{"source": "extra", "target": "foo_pkg"}],
            pipeline_dir=tmp_path,
        )


@pytest.mark.integration
def test_upload_all_code_creates_tarball_under_pipeline_dot_build(
    tmp_path: Path,
) -> None:
    """Dev client upload is a no-op; still exercises local build + archive + upload path."""
    logger = logging.getLogger("tests.upload_all_code")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "stages" / "map_only"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "mapper.py").write_text("def run():\n    pass\n", encoding="utf-8")
    (stage / "config.yaml").write_text("k: 1\n", encoding="utf-8")
    client = YTDevClient(logger=logger, pipeline_dir=tmp_path)
    upload_all_code(client, "//yt/test/build", tmp_path, logger)
    archive = tmp_path / ".build" / "source.tar.gz"
    assert (
        archive.is_file() and archive.stat().st_size > 0
    ), "expected non-empty archive after full upload pipeline"


def test_copy_module_to_build_dir_raises_value_error_on_import_error(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.copy_mod")
    logger.addHandler(logging.NullHandler())
    with pytest.raises(ValueError, match="Failed to import module"):
        _copy_module_to_build_dir(
            "definitely_missing_module_xyz",
            tmp_path / "out",
            logger,
        )


def test_copy_module_to_build_dir_rejects_single_file_module(tmp_path: Path) -> None:
    (tmp_path / "singlemod.py").write_text("x = 1\n", encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    try:
        logger = logging.getLogger("tests.upload.single")
        logger.addHandler(logging.NullHandler())
        with pytest.raises(ValueError, match="single-file module"):
            _copy_module_to_build_dir(
                "singlemod",
                tmp_path / "build_single",
                logger,
            )
    finally:
        sys.path.remove(str(tmp_path))


def test_copy_module_to_build_dir_copies_package_tree(tmp_path: Path) -> None:
    pkg = tmp_path / "upload_pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "mod.py").write_text("# m\n", encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    try:
        logger = logging.getLogger("tests.upload.pkg")
        logger.addHandler(logging.NullHandler())
        target = tmp_path / "build_pkg" / "upload_pkg"
        n = _copy_module_to_build_dir("upload_pkg", target, logger)
        assert n >= 2 and (target / "mod.py").is_file()
    finally:
        sys.path.remove(str(tmp_path))


def test_copy_module_to_build_dir_rejects_module_without_file_attribute(
    tmp_path: Path,
) -> None:
    name = "ns_like_pkg_xyz"
    mod = types.ModuleType(name)
    mod.__path__ = []  # type: ignore[attr-defined]
    mod.__file__ = None  # type: ignore[attr-defined]
    sys.modules[name] = mod
    try:
        logger = logging.getLogger("tests.upload.ns")
        logger.addHandler(logging.NullHandler())
        with pytest.raises(ValueError, match="no __file__"):
            _copy_module_to_build_dir(name, tmp_path / "out_ns", logger)
    finally:
        del sys.modules[name]


def test_copy_module_to_build_dir_raises_when_package_path_is_not_a_directory(
    tmp_path: Path,
) -> None:
    pkg = tmp_path / "bad_path_pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    bogus = tmp_path / "not_a_dir_but_file.txt"
    bogus.write_text("x", encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    try:
        import bad_path_pkg  # type: ignore[import-not-found]

        bad_path_pkg.__path__ = [str(bogus)]  # type: ignore[attr-defined]
        logger = logging.getLogger("tests.upload.badpath")
        logger.addHandler(logging.NullHandler())
        with pytest.raises(ValueError, match="invalid __path__"):
            _copy_module_to_build_dir("bad_path_pkg", tmp_path / "out_bad", logger)
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("bad_path_pkg", None)


def test_copy_module_to_build_dir_skips_ytignored_files_and_logs_debug(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    pkg = tmp_path / "ign_pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "keep.py").write_text("# k\n", encoding="utf-8")
    (pkg / "drop.py").write_text("# d\n", encoding="utf-8")
    (pkg / ".ytignore").write_text("drop.py\n", encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    try:
        caplog.set_level(logging.DEBUG)
        logger = logging.getLogger("tests.upload.ytignore_mod")
        logger.addHandler(logging.NullHandler())
        target = tmp_path / "build_ign" / "ign_pkg"
        n = _copy_module_to_build_dir("ign_pkg", target, logger)
        assert (
            (target / "keep.py").is_file()
            and not (target / "drop.py").exists()
            and n >= 1
            and "Ignoring file (matched .ytignore)" in caplog.text
        ), "ignored module files should not land in build dir"
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("ign_pkg", None)


def test_copy_path_to_build_dir_raises_file_not_found_when_source_missing(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.path_missing")
    logger.addHandler(logging.NullHandler())
    with pytest.raises(FileNotFoundError, match="does not exist"):
        _copy_path_to_build_dir(
            source_path="nope",
            target_name="dst",
            build_dir=tmp_path / "build",
            pipeline_dir=tmp_path,
            logger=logger,
        )


def test_copy_path_to_build_dir_raises_when_source_is_file_not_directory(
    tmp_path: Path,
) -> None:
    f = tmp_path / "file_only.txt"
    f.write_text("x", encoding="utf-8")
    logger = logging.getLogger("tests.upload.path_file")
    logger.addHandler(logging.NullHandler())
    with pytest.raises(ValueError, match="must be a directory"):
        _copy_path_to_build_dir(
            source_path="file_only.txt",
            target_name="dst",
            build_dir=tmp_path / "build",
            pipeline_dir=tmp_path,
            logger=logger,
        )


def test_copy_path_to_build_dir_raises_when_source_escapes_pipeline_dir(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.path_escape")
    logger.addHandler(logging.NullHandler())
    with pytest.raises(ValueError, match="within pipeline directory"):
        _copy_path_to_build_dir(
            source_path="../outside",
            target_name="dst",
            build_dir=tmp_path / "build",
            pipeline_dir=tmp_path,
            logger=logger,
        )


def test_copy_path_to_build_dir_copies_directory_tree(tmp_path: Path) -> None:
    src = tmp_path / "extra"
    src.mkdir()
    (src / "a.txt").write_text("1", encoding="utf-8")
    logger = logging.getLogger("tests.upload.path_ok")
    logger.addHandler(logging.NullHandler())
    build = tmp_path / "build"
    n = _copy_path_to_build_dir(
        source_path="extra",
        target_name="my_extra",
        build_dir=build,
        pipeline_dir=tmp_path,
        logger=logger,
    )
    assert n == 1 and (build / "my_extra" / "a.txt").read_text(encoding="utf-8") == "1"


def test_copy_path_to_build_dir_skips_ytignored_files(tmp_path: Path) -> None:
    src = tmp_path / "extra_ig"
    src.mkdir()
    (src / "keep.txt").write_text("k", encoding="utf-8")
    (src / "drop.txt").write_text("d", encoding="utf-8")
    (src / ".ytignore").write_text("drop.txt\n", encoding="utf-8")
    logger = logging.getLogger("tests.upload.path_ytignore")
    logger.addHandler(logging.NullHandler())
    build = tmp_path / "build_igpath"
    n = _copy_path_to_build_dir(
        source_path="extra_ig",
        target_name="out",
        build_dir=build,
        pipeline_dir=tmp_path,
        logger=logger,
    )
    out = build / "out"
    assert (
        n == 1
        and (out / "keep.txt").read_text(encoding="utf-8") == "k"
        and not (out / "drop.txt").exists()
    ), ".ytignore must exclude drop.txt; .ytignore file is never copied"


def test_copy_stage_to_build_dir_skips_src_file_when_matched_by_ytignore(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.stage_src_ig")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_igsrc"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "keep.py").write_text("#\n", encoding="utf-8")
    (stage / "src" / "skip.py").write_text("#\n", encoding="utf-8")
    (stage / ".ytignore").write_text("src/skip.py\n", encoding="utf-8")
    (stage / "config.yaml").write_text("job:\n  type: map\n", encoding="utf-8")
    build = tmp_path / "b_igsrc"
    n = _copy_stage_to_build_dir(build, stage, logger)
    tgt = build / "stages" / "st_igsrc" / "src"
    assert (
        n == 2 and (tgt / "keep.py").is_file() and not (tgt / "skip.py").exists()
    ), "patterned src file must be skipped; config and keep.py copy"


def test_load_stage_job_section_returns_empty_when_config_yaml_missing(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.lj_miss")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_nocfg"
    stage.mkdir()
    assert _load_stage_job_section(stage, logger) == {}


def test_load_stage_job_section_returns_empty_when_yaml_root_is_sequence(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.lj_seq")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_seqroot"
    stage.mkdir()
    (stage / "config.yaml").write_text("- item\n", encoding="utf-8")
    assert _load_stage_job_section(stage, logger) == {}


def test_resolve_map_reduce_command_scripts_treats_non_mapping_as_empty(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.mrc_nm")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_mrc_scalar"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "mapper.py").write_text("#\n", encoding="utf-8")
    (stage / "config.yaml").write_text(
        "job:\n  map_reduce_command: not_a_mapping\n",
        encoding="utf-8",
    )
    mapper, reducer = _resolve_map_reduce_command_scripts(stage, logger)
    assert (
        mapper == "mapper.py" and reducer is None
    ), "scalar map_reduce_command must fall back like empty dict"


def test_resolve_map_reduce_command_scripts_returns_none_when_mapper_missing(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    logger = logging.getLogger("tests.upload.mrc_nom")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_nomapper"
    (stage / "src").mkdir(parents=True)
    (stage / "config.yaml").write_text(
        "job:\n  map_reduce_command:\n    mapper_script: ghost.py\n",
        encoding="utf-8",
    )
    mapper, reducer = _resolve_map_reduce_command_scripts(stage, logger)
    assert (
        mapper is None
        and reducer is None
        and "ghost.py" in caplog.text
        and "skipping map_reduce_mapper" in caplog.text
    )


def test_resolve_map_reduce_command_scripts_writes_both_wrappers_when_reducer_resolved(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.mrc_both")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_mrboth"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "mapper.py").write_text("#\n", encoding="utf-8")
    (stage / "src" / "r_side.py").write_text("#\n", encoding="utf-8")
    (stage / "config.yaml").write_text(
        "job:\n"
        "  map_reduce_command:\n"
        "    mapper_script: mapper.py\n"
        "    reducer_script: r_side.py\n",
        encoding="utf-8",
    )
    build = tmp_path / "build_mrboth"
    build.mkdir()
    _create_map_reduce_command_wrappers("st_mrboth", stage, build, logger)
    assert (build / "operation_wrapper_st_mrboth_map_reduce_mapper.sh").is_file() and (
        build / "operation_wrapper_st_mrboth_map_reduce_reducer.sh"
    ).is_file(), "mapper and reducer shell wrappers must both exist"


def test_create_map_reduce_command_wrappers_writes_nothing_when_mapper_unresolvable(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.mrc_early_ret")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_mr_empty"
    (stage / "src").mkdir(parents=True)
    (stage / "config.yaml").write_text(
        "job:\n  map_reduce_command:\n    mapper_script: absent.py\n",
        encoding="utf-8",
    )
    build = tmp_path / "build_mr_empty"
    build.mkdir()
    _create_map_reduce_command_wrappers("st_mr_empty", stage, build, logger)
    assert (
        list(build.glob("operation_wrapper_*")) == []
    ), "no mapper script means no wrapper files"


def test_resolve_map_reduce_command_scripts_auto_discovers_reducer_mds_file(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.mrc_autored")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_autored"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "mapper.py").write_text("#\n", encoding="utf-8")
    (stage / "src" / "reducer_mds.py").write_text("#\n", encoding="utf-8")
    (stage / "config.yaml").write_text(
        "job:\n  map_reduce_command:\n    mapper_script: mapper.py\n",
        encoding="utf-8",
    )
    mapper, reducer = _resolve_map_reduce_command_scripts(stage, logger)
    assert (
        mapper == "mapper.py" and reducer == "reducer_mds.py"
    ), "candidate list should pick reducer_mds.py when reducer_script omitted"


def test_resolve_reduce_command_script_ignores_scalar_reduce_command_value(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.rc_scalar")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_rc_scalar"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "reducer.py").write_text("#\n", encoding="utf-8")
    (stage / "config.yaml").write_text(
        "job:\n  reduce_command: not_a_mapping\n",
        encoding="utf-8",
    )
    assert (
        _resolve_reduce_command_script(stage, logger) == "reducer.py"
    ), "non-dict reduce_command must fall back to default reducer filename"


def test_resolve_reduce_command_script_returns_explicit_name_when_file_exists(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.red_ok")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_red_explicit"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "my_red.py").write_text("#\n", encoding="utf-8")
    (stage / "config.yaml").write_text(
        "job:\n  reduce_command:\n    reducer_script: my_red.py\n",
        encoding="utf-8",
    )
    assert _resolve_reduce_command_script(stage, logger) == "my_red.py"


def test_create_wrappers_for_stage_is_no_op_when_stage_has_no_src_dir(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.wrap_nosrc")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_only_cfg"
    stage.mkdir()
    (stage / "config.yaml").write_text("job:\n  type: map\n", encoding="utf-8")
    build = tmp_path / "build_nosrc"
    build.mkdir()
    _create_wrappers_for_stage("only_cfg", stage, build, logger)
    assert list(build.glob("operation_wrapper_*")) == [], "no src means no wrappers"


def test_build_code_locally_creates_vanilla_wrapper_for_vanilla_only_stage(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.vanilla_only")
    logger.addHandler(logging.NullHandler())
    st = tmp_path / "stages" / "van_only"
    (st / "src").mkdir(parents=True)
    (st / "src" / "vanilla.py").write_text("#\n", encoding="utf-8")
    build_dir = tmp_path / "out_vanilla"
    build_code_locally(
        build_dir=build_dir,
        pipeline_dir=tmp_path,
        logger=logger,
        create_wrappers=True,
    )
    wrap = build_dir / "operation_wrapper_van_only_vanilla.sh"
    assert wrap.is_file() and "vanilla.py" in wrap.read_text(
        encoding="utf-8"
    ), "vanilla-only stage should get vanilla operation wrapper"


def test_build_code_locally_creates_reduce_wrapper_when_reduce_command_configured(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.reduce_wrap")
    logger.addHandler(logging.NullHandler())
    st = tmp_path / "stages" / "red_wrap_st"
    (st / "src").mkdir(parents=True)
    (st / "src" / "mapper.py").write_text("#\n", encoding="utf-8")
    (st / "src" / "red_only.py").write_text("#\n", encoding="utf-8")
    (st / "config.yaml").write_text(
        "job:\n  reduce_command:\n    reducer_script: red_only.py\n",
        encoding="utf-8",
    )
    build_dir = tmp_path / "out_reduce_wrap"
    build_code_locally(
        build_dir=build_dir,
        pipeline_dir=tmp_path,
        logger=logger,
        create_wrappers=True,
    )
    sh = build_dir / "operation_wrapper_red_wrap_st_reduce.sh"
    assert sh.is_file() and "red_only.py" in sh.read_text(
        encoding="utf-8"
    ), "reduce_command should produce reduce shell wrapper"


def test_build_code_locally_copies_upload_modules_package_into_build_dir(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.modules_build")
    logger.addHandler(logging.NullHandler())
    (tmp_path / "stages").mkdir()
    pkg = tmp_path / "upl_modpkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "marker.py").write_text("v = 1\n", encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    try:
        build_dir = tmp_path / "build_with_mod"
        build_code_locally(
            build_dir=build_dir,
            pipeline_dir=tmp_path,
            logger=logger,
            upload_modules=["upl_modpkg"],
        )
        assert (build_dir / "upl_modpkg" / "marker.py").read_text(
            encoding="utf-8"
        ) == "v = 1\n", "upload_modules tree must appear under build_dir"
    finally:
        sys.path.remove(str(tmp_path))


def test_build_code_locally_copies_upload_paths_into_build_dir(tmp_path: Path) -> None:
    logger = logging.getLogger("tests.upload.paths_build")
    logger.addHandler(logging.NullHandler())
    (tmp_path / "stages").mkdir()
    extra = tmp_path / "payload"
    extra.mkdir()
    (extra / "note.txt").write_text("ok", encoding="utf-8")
    build_dir = tmp_path / "out_build"
    build_code_locally(
        build_dir=build_dir,
        pipeline_dir=tmp_path,
        logger=logger,
        upload_paths=[{"source": "payload", "target": "custom_name"}],
    )
    assert (build_dir / "custom_name" / "note.txt").read_text(
        encoding="utf-8"
    ) == "ok", "upload_paths should land under build_dir"


def test_build_code_locally_creates_only_map_reduce_mapper_wrapper_when_no_reducer(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.mr_wrap")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "stages" / "mr_stage"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "mapper.py").write_text("def run():\n    pass\n", encoding="utf-8")
    (stage / "config.yaml").write_text(
        "job:\n  map_reduce_command:\n    mapper_script: mapper.py\n",
        encoding="utf-8",
    )
    build_dir = tmp_path / "build"
    build_code_locally(
        build_dir=build_dir,
        pipeline_dir=tmp_path,
        logger=logger,
        create_wrappers=True,
    )
    mapper_sh = build_dir / "operation_wrapper_mr_stage_map_reduce_mapper.sh"
    reducer_sh = build_dir / "operation_wrapper_mr_stage_map_reduce_reducer.sh"
    assert (
        mapper_sh.is_file() and not reducer_sh.exists()
    ), "expected mapper wrapper without reducer when no reducer script resolves"


def test_copy_stage_to_build_dir_copies_job_config_and_src_files(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.stage_ok")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "stage_ok"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "m.py").write_text("#\n", encoding="utf-8")
    (stage / "config.yaml").write_text("job:\n  type: map\n", encoding="utf-8")
    build = tmp_path / "build_stage"
    n = _copy_stage_to_build_dir(build, stage, logger)
    cfg = build / "stages" / "stage_ok" / "config.yaml"
    src_f = build / "stages" / "stage_ok" / "src" / "m.py"
    assert n == 2 and cfg.is_file() and src_f.is_file(), "job config plus src file"


def test_copy_stage_to_build_dir_skips_config_when_yaml_load_fails(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.stage_bad_yaml")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "stage_bad"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "only.txt").write_text("z", encoding="utf-8")
    (stage / "config.yaml").write_text("{ not valid yaml: [[", encoding="utf-8")
    build = tmp_path / "build_bad"
    n = _copy_stage_to_build_dir(build, stage, logger)
    assert (
        n == 1
        and not (build / "stages" / "stage_bad" / "config.yaml").exists()
        and (build / "stages" / "stage_bad" / "src" / "only.txt").read_text() == "z"
    ), "unparseable config must be skipped; src still copied"


def test_copy_stage_to_build_dir_skips_config_when_matched_by_ytignore(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.stage_ytignore")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "stage_ig"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "x.txt").write_text("1", encoding="utf-8")
    (stage / ".ytignore").write_text("config.yaml\n", encoding="utf-8")
    (stage / "config.yaml").write_text("job:\n  type: map\n", encoding="utf-8")
    build = tmp_path / "build_ig"
    n = _copy_stage_to_build_dir(build, stage, logger)
    assert (
        n == 1
        and not (build / "stages" / "stage_ig" / "config.yaml").exists()
        and (build / "stages" / "stage_ig" / "src" / "x.txt").read_text() == "1"
    ), "ignored config must not copy; src still copies"


def test_resolve_reduce_command_script_warns_when_explicit_script_missing_then_falls_back(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("tests.upload.red_fallback")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_rf"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "reducer.py").write_text("x=1\n", encoding="utf-8")
    (stage / "config.yaml").write_text(
        "job:\n  reduce_command:\n    reducer_script: nope.py\n",
        encoding="utf-8",
    )
    name = _resolve_reduce_command_script(stage, logger)
    assert (
        name == "reducer.py"
        and "reduce_command.reducer_script" in caplog.text
        and "nope.py" in caplog.text
    ), "missing explicit script should warn and fall back to default reducer name"


def test_resolve_reduce_command_script_returns_none_after_warning_when_no_reducer_file(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("tests.upload.red_none")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_rn"
    (stage / "src").mkdir(parents=True)
    (stage / "config.yaml").write_text(
        "job:\n  reduce_command:\n    reducer_script: ghost.py\n",
        encoding="utf-8",
    )
    assert (
        _resolve_reduce_command_script(stage, logger) is None
        and "ghost.py" in caplog.text
    )


def test_load_stage_job_section_returns_empty_dict_and_warns_when_config_unreadable(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("tests.upload.load_job")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_lj"
    stage.mkdir()
    (stage / "config.yaml").write_text("{ bad", encoding="utf-8")
    job = _load_stage_job_section(stage, logger)
    assert (
        job == {} and "Could not read" in caplog.text and "config.yaml" in caplog.text
    )


def test_create_map_reduce_command_wrappers_logs_warning_when_reducer_unresolvable(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("tests.upload.mr_cap")
    logger.addHandler(logging.NullHandler())
    stage = tmp_path / "st_mrw"
    (stage / "src").mkdir(parents=True)
    (stage / "src" / "mapper.py").write_text("x=1\n", encoding="utf-8")
    (stage / "config.yaml").write_text(
        "job:\n  map_reduce_command:\n    mapper_script: mapper.py\n",
        encoding="utf-8",
    )
    build = tmp_path / "build_mrw"
    build.mkdir()
    _create_map_reduce_command_wrappers("st_mrw", stage, build, logger)
    assert (
        "map_reduce_mapper wrapper created but no reducer script" in caplog.text
    ), "expected warning when mapper wrapper exists but reducer file cannot be resolved"


def test_create_code_archive_writes_gzip_tar_with_relative_arcnames(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.archive")
    logger.addHandler(logging.NullHandler())
    build = tmp_path / "src"
    (build / "sub").mkdir(parents=True)
    (build / "sub" / "a.txt").write_text("hi", encoding="utf-8")
    archive = tmp_path / "out" / "bundle.tar.gz"
    create_code_archive(build, archive, logger)
    assert archive.is_file() and archive.stat().st_size > 0, "archive must exist"
    with tarfile.open(archive, "r:gz") as tf:
        names = sorted(tf.getnames())
    assert names == ["sub/a.txt"]


def test_upload_code_archive_calls_client_upload_file_with_build_folder_and_name(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("tests.upload.up_archive")
    logger.addHandler(logging.NullHandler())
    arch = tmp_path / "source.tar.gz"
    arch.write_bytes(b"x")
    fake = _FakeUploadYtClient()
    upload_code_archive(fake, arch, "//yt/myproject/build", logger)
    assert fake.upload_calls == [
        (arch, "//yt/myproject/build/source.tar.gz", True),
    ]
