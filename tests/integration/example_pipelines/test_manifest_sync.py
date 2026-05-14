"""Manifest must list every ``examples/*/pipeline.py`` (and only real trees)."""

from __future__ import annotations

from integration.example_pipelines._manifest import (
    discover_pipeline_dirs,
    examples_dir,
    load_manifest_specs,
    manifest_path,
)


def test_manifest_lists_every_example_pipeline_on_disk() -> None:
    on_disk = discover_pipeline_dirs()
    in_manifest = {s.slug for s in load_manifest_specs()}
    missing = sorted(on_disk - in_manifest)
    assert not missing, f"Add these slugs to {manifest_path()}: {missing}"


def test_manifest_slugs_exist_and_have_pipeline_py() -> None:
    root = examples_dir()
    for spec in load_manifest_specs():
        tree = root / spec.slug
        assert tree.is_dir(), f"Missing example dir for slug {spec.slug!r}"
        assert (tree / "pipeline.py").is_file(), (
            f"Missing pipeline.py for {spec.slug!r}"
        )


def test_manifest_slug_list_unique() -> None:
    slugs = [s.slug for s in load_manifest_specs()]
    assert len(slugs) == len(set(slugs)), f"Duplicate slug in manifest: {slugs}"


def test_manifest_cluster_optional_only_known_slugs() -> None:
    allowed = {"06_s3_integration", "07_custom_docker"}
    for spec in load_manifest_specs():
        if spec.ci_tier != "cluster_optional":
            continue
        assert spec.slug in allowed, (
            f"Add an explicit opt-in test for new cluster_optional slug: {spec.slug}"
        )
