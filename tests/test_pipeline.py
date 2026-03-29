"""Tests for yt_framework.core.pipeline (normalization, run, upload_code)."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from omegaconf import OmegaConf

from yt_framework.core.pipeline import (
    BasePipeline,
    DefaultPipeline,
    _normalize_upload_modules,
    _normalize_upload_paths,
)
from yt_framework.core.registry import StageRegistry


def test_normalize_upload_modules_none_returns_empty_list() -> None:
    assert _normalize_upload_modules(None) == []


def test_normalize_upload_modules_blank_string_returns_empty_list() -> None:
    assert _normalize_upload_modules("   ") == []


def test_normalize_upload_modules_non_blank_string_returns_single_element() -> None:
    assert _normalize_upload_modules("  mod_a  ") == ["mod_a"]


def test_normalize_upload_modules_list_strips_entries() -> None:
    raw = OmegaConf.create([" a ", "b", ""])
    assert _normalize_upload_modules(raw) == ["a", "b"]


def test_normalize_upload_modules_rejects_non_sequence_non_string() -> None:
    with pytest.raises(ValueError, match="upload_modules"):
        _normalize_upload_modules(42)


def test_normalize_upload_paths_none_returns_empty_list() -> None:
    assert _normalize_upload_paths(None) == []


def test_normalize_upload_paths_rejects_non_list_container() -> None:
    with pytest.raises(ValueError, match="upload_paths must be a list"):
        _normalize_upload_paths({"source": "x"})


def test_normalize_upload_paths_requires_source_key() -> None:
    with pytest.raises(ValueError, match="missing required 'source'"):
        _normalize_upload_paths([{"target": "only"}])


def test_normalize_upload_paths_rejects_non_mapping_element() -> None:
    with pytest.raises(ValueError, match=r"upload_paths\[0\] must be a mapping"):
        _normalize_upload_paths(["not-a-dict"])


def test_normalize_upload_paths_converts_dictconfig_element_to_str_mapping() -> None:
    raw = OmegaConf.create([{"source": "/src", "target": "//yt/t"}])
    out = _normalize_upload_paths(raw)
    assert out == [{"source": "/src", "target": "//yt/t"}]


def _touch_configs_secrets(pipeline_root: Path) -> None:
    cfg_dir = pipeline_root / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "secrets.env").write_text(
        "# test secrets placeholder\n", encoding="utf-8"
    )


def _write_stage_module(pipeline_root: Path, stage_name: str) -> type:
    stage_dir = pipeline_root / "stages" / stage_name
    stage_dir.mkdir(parents=True)
    (stage_dir / "config.yaml").write_text("k: 1\n", encoding="utf-8")
    (stage_dir / "stage.py").write_text(
        "from yt_framework.core.stage import BaseStage\n"
        "class MinimalStage(BaseStage):\n"
        "    def run(self, debug):\n"
        "        root = self.stage_dir.parent.parent\n"
        "        (root / 'stage_ran.txt').write_text(self.name, encoding='utf-8')\n"
        "        return {**debug, 'ran': self.name}\n",
        encoding="utf-8",
    )
    stage_py = stage_dir / "stage.py"
    mod_name = f"_dyn_stage_{stage_name}"
    spec = importlib.util.spec_from_file_location(mod_name, stage_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod.MinimalStage


def test_base_pipeline_init_raises_when_pipeline_dir_does_not_exist() -> None:
    cfg = OmegaConf.create({"pipeline": {"mode": "dev"}})
    missing = Path("/nonexistent/pipeline/root/yt_framework_test")
    with pytest.raises(ValueError, match="Pipeline directory not found"):
        BasePipeline(cfg, missing)


def test_base_pipeline_default_setup_leaves_registry_unset(tmp_path: Path) -> None:
    _touch_configs_secrets(tmp_path)
    cfg = OmegaConf.create({"pipeline": {"mode": "dev"}})

    class _UsesBaseSetup(BasePipeline):
        pass

    pipe = _UsesBaseSetup(cfg, tmp_path)
    assert pipe._stage_registry is None, "BasePipeline.setup is a no-op by default"


def test_stages_need_code_execution_is_false_when_no_enabled_stages(
    tmp_path: Path,
) -> None:
    _touch_configs_secrets(tmp_path)
    cfg = OmegaConf.create(
        {"pipeline": {"mode": "dev"}, "stages": {"enabled_stages": []}}
    )

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry())

    p = _P(cfg, tmp_path)
    assert p._stages_need_code_execution() is False


def test_stages_need_code_execution_is_false_when_enabled_stage_has_no_src(
    tmp_path: Path,
) -> None:
    _touch_configs_secrets(tmp_path)
    stage_dir = tmp_path / "stages" / "plain"
    stage_dir.mkdir(parents=True)
    (stage_dir / "config.yaml").write_text("x: 1\n", encoding="utf-8")
    cfg = OmegaConf.create(
        {
            "pipeline": {"mode": "dev"},
            "stages": {"enabled_stages": ["plain"]},
        }
    )

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry())

    p = _P(cfg, tmp_path)
    assert p._stages_need_code_execution() is False


def test_stages_need_code_execution_is_true_when_enabled_stage_has_src_dir(
    tmp_path: Path,
) -> None:
    _touch_configs_secrets(tmp_path)
    stage_dir = tmp_path / "stages" / "coded"
    (stage_dir / "src").mkdir(parents=True)
    (stage_dir / "src" / "mapper.py").write_text("#\n", encoding="utf-8")
    cfg = OmegaConf.create(
        {
            "pipeline": {"mode": "dev"},
            "stages": {"enabled_stages": ["coded"]},
        }
    )

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry())

    p = _P(cfg, tmp_path)
    assert p._stages_need_code_execution() is True


@patch("yt_framework.core.pipeline.upload_all_code")
def test_upload_code_does_not_call_upload_when_no_stage_needs_execution(
    mock_upload: object,
    tmp_path: Path,
) -> None:
    _touch_configs_secrets(tmp_path)
    cfg = OmegaConf.create(
        {
            "pipeline": {"mode": "dev"},
            "stages": {"enabled_stages": ["plain"]},
        }
    )
    plain = tmp_path / "stages" / "plain"
    plain.mkdir(parents=True)

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry())

    _P(cfg, tmp_path).upload_code()
    mock_upload.assert_not_called()


@patch("yt_framework.core.pipeline.upload_all_code")
def test_upload_code_raises_when_src_present_but_build_folder_missing(
    mock_upload: object,
    tmp_path: Path,
) -> None:
    _touch_configs_secrets(tmp_path)
    cfg = OmegaConf.create(
        {
            "pipeline": {"mode": "dev"},
            "stages": {"enabled_stages": ["coded"]},
        }
    )
    coded = tmp_path / "stages" / "coded"
    (coded / "src").mkdir(parents=True)
    (coded / "src" / "mapper.py").write_text("#\n", encoding="utf-8")

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry())

    p = _P(cfg, tmp_path)
    with pytest.raises(ValueError, match="build_folder"):
        p.upload_code()
    mock_upload.assert_not_called()


def test_run_raises_when_no_enabled_stages(tmp_path: Path) -> None:
    _touch_configs_secrets(tmp_path)
    cfg = OmegaConf.create(
        {"pipeline": {"mode": "dev"}, "stages": {"enabled_stages": []}}
    )

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry())

    p = _P(cfg, tmp_path)
    with pytest.raises(ValueError, match="No enabled_stages"):
        p.run()


def test_run_raises_when_stage_registry_not_set(tmp_path: Path) -> None:
    _touch_configs_secrets(tmp_path)
    cfg = OmegaConf.create(
        {
            "pipeline": {"mode": "dev"},
            "stages": {"enabled_stages": ["any"]},
        }
    )

    class _P(BasePipeline):
        def setup(self) -> None:
            pass

    p = _P(cfg, tmp_path)
    with pytest.raises(AttributeError, match="stage registry"):
        p.run()


def test_run_raises_for_unknown_enabled_stage_name(tmp_path: Path) -> None:
    _touch_configs_secrets(tmp_path)
    StageCls = _write_stage_module(tmp_path, "only_one")
    cfg = OmegaConf.create(
        {
            "pipeline": {"mode": "dev"},
            "stages": {"enabled_stages": ["missing"]},
        }
    )

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry().add_stage(StageCls))

    p = _P(cfg, tmp_path)
    with pytest.raises(ValueError, match="Unknown stage"):
        p.run()


@patch("yt_framework.core.pipeline.upload_all_code")
def test_run_executes_registered_stage_in_order(
    _mock_upload: object,
    tmp_path: Path,
) -> None:
    _touch_configs_secrets(tmp_path)
    StageCls = _write_stage_module(tmp_path, "step_a")
    cfg = OmegaConf.create(
        {
            "pipeline": {"mode": "dev"},
            "stages": {"enabled_stages": ["step_a"]},
        }
    )

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry().add_stage(StageCls))

    p = _P(cfg, tmp_path)
    p.run()
    assert (tmp_path / "stage_ran.txt").read_text(encoding="utf-8") == "step_a"


def _write_packaged_stage(pipeline_root: Path, folder_name: str) -> None:
    pkg = pipeline_root / "stages"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    stage_dir = pkg / folder_name
    stage_dir.mkdir()
    (stage_dir / "__init__.py").write_text("", encoding="utf-8")
    (stage_dir / "stage.py").write_text(
        "from yt_framework.core.stage import BaseStage\n"
        "class PackagedStage(BaseStage):\n"
        "    def run(self, debug):\n"
        "        return debug\n",
        encoding="utf-8",
    )


def test_default_pipeline_warns_when_discovery_finds_no_stages(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _touch_configs_secrets(tmp_path)
    cfg = OmegaConf.create(
        {"pipeline": {"mode": "dev"}, "stages": {"enabled_stages": []}}
    )
    with patch("yt_framework.core.discovery.discover_stages", return_value=[]):
        DefaultPipeline(cfg, tmp_path)
    assert "No stages discovered" in capsys.readouterr().out


def test_default_pipeline_setup_registers_discovered_stages(tmp_path: Path) -> None:
    for key in list(sys.modules):
        if key == "stages" or key.startswith("stages."):
            del sys.modules[key]
    _touch_configs_secrets(tmp_path)
    _write_packaged_stage(tmp_path, "pack_st")
    cfg = OmegaConf.create(
        {"pipeline": {"mode": "dev"}, "stages": {"enabled_stages": []}}
    )
    pipe = DefaultPipeline(cfg, tmp_path)
    assert pipe._stage_registry is not None
    assert pipe._stage_registry.has_stage("pack_st"), "discovered stage should register"


def _write_yaml_config(pipeline_root: Path, relative: str, data: object) -> None:
    cfg_path = pipeline_root / relative
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(yaml.safe_dump(data), encoding="utf-8")


def test_main_exits_with_code_1_when_config_file_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "pipeline_entry.py"
    script.write_text("# entry\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(script)])
    _touch_configs_secrets(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        BasePipeline.main(["--config", "configs/absent.yaml"])
    assert exc_info.value.code == 1


def test_main_exits_with_code_1_when_config_root_is_not_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "pipeline_entry.py"
    script.write_text("# entry\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(script)])
    _touch_configs_secrets(tmp_path)
    _write_yaml_config(tmp_path, "configs/config.yaml", [1, 2])
    with pytest.raises(SystemExit) as exc_info:
        BasePipeline.main(["--config", "configs/config.yaml"])
    assert exc_info.value.code == 1


def test_main_exits_with_code_1_when_config_file_is_unparseable_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OmegaConf.load failures hit the except branch (logger + sys.exit(1))."""
    script = tmp_path / "pipeline_entry.py"
    script.write_text("# entry\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(script)])
    _touch_configs_secrets(tmp_path)
    bad = tmp_path / "configs" / "config.yaml"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("{ not: valid: yaml: [[", encoding="utf-8")
    with pytest.raises(SystemExit) as exc_info:
        BasePipeline.main(["--config", "configs/config.yaml"])
    assert exc_info.value.code == 1, "parse error must surface as exit code 1"


def test_main_uses_dev_mode_when_config_probe_load_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "pipeline_entry.py"
    script.write_text("# entry\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(script)])
    _touch_configs_secrets(tmp_path)
    stage_name = "probe_ok"
    stage_cls = _write_stage_module(tmp_path, stage_name)
    _write_yaml_config(
        tmp_path,
        "configs/config.yaml",
        {"pipeline": {"mode": "dev"}, "stages": {"enabled_stages": [stage_name]}},
    )
    loaded = OmegaConf.create(
        {"pipeline": {"mode": "dev"}, "stages": {"enabled_stages": [stage_name]}}
    )

    class _ProbeOkPipeline(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry().add_stage(stage_cls))

    stage_cfg = OmegaConf.create({"k": 1})
    with patch("yt_framework.core.pipeline.OmegaConf.load", MagicMock()) as mock_load:
        mock_load.side_effect = [RuntimeError("probe"), loaded, stage_cfg]
        with pytest.raises(SystemExit) as exc_info:
            _ProbeOkPipeline.main(["--config", "configs/config.yaml"])
    assert exc_info.value.code == 0


def test_main_exits_with_code_0_when_pipeline_run_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "pipeline_entry.py"
    script.write_text("# entry\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(script)])
    _touch_configs_secrets(tmp_path)
    stage_name = "cli_ok"
    stage_cls = _write_stage_module(tmp_path, stage_name)
    _write_yaml_config(
        tmp_path,
        "configs/config.yaml",
        {"pipeline": {"mode": "dev"}, "stages": {"enabled_stages": [stage_name]}},
    )

    class _CliOkPipeline(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry().add_stage(stage_cls))

    with pytest.raises(SystemExit) as exc_info:
        _CliOkPipeline.main(["--config", "configs/config.yaml"])
    assert exc_info.value.code == 0
    assert (tmp_path / "stage_ran.txt").read_text(encoding="utf-8") == stage_name


def test_main_exits_with_code_1_when_pipeline_run_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "pipeline_entry.py"
    script.write_text("# entry\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(script)])
    _touch_configs_secrets(tmp_path)
    _write_yaml_config(
        tmp_path,
        "configs/config.yaml",
        {
            "pipeline": {"mode": "dev"},
            "stages": {"enabled_stages": ["any"]},
        },
    )

    class _NoRegistryPipeline(BasePipeline):
        def setup(self) -> None:
            pass

    with pytest.raises(SystemExit) as exc_info:
        _NoRegistryPipeline.main(["--config", "configs/config.yaml"])
    assert exc_info.value.code == 1


@patch("yt_framework.core.pipeline.upload_all_code")
def test_upload_code_calls_upload_all_code_with_pipeline_build_folder(
    mock_upload: object,
    tmp_path: Path,
) -> None:
    _touch_configs_secrets(tmp_path)
    coded = tmp_path / "stages" / "coded"
    (coded / "src").mkdir(parents=True)
    (coded / "src" / "mapper.py").write_text("#\n", encoding="utf-8")
    cfg = OmegaConf.create(
        {
            "pipeline": {"mode": "dev", "build_folder": "//yt/test/build"},
            "stages": {"enabled_stages": ["coded"]},
        }
    )

    class _P(BasePipeline):
        def setup(self) -> None:
            self.set_stage_registry(StageRegistry())

    pipeline = _P(cfg, tmp_path)
    pipeline.upload_code()
    kwargs = mock_upload.call_args.kwargs
    assert (
        mock_upload.call_count == 1
        and kwargs["build_folder"] == "//yt/test/build"
        and kwargs["yt_client"] is pipeline.yt
        and kwargs["pipeline_dir"] == pipeline.pipeline_dir
        and kwargs["upload_modules"] == []
        and kwargs["upload_paths"] == []
    ), "upload_code should delegate once with normalized modules/paths and build_folder"
