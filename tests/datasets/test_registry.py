from dataclasses import FrozenInstanceError, fields
from pathlib import Path
from types import MappingProxyType

import pytest

from datasets import registry as reg

ROOT = Path(__file__).resolve().parents[2]
REAL = ROOT / "datasets" / "registry.yaml"
V2_FIXTURE = ROOT / "tests" / "datasets" / "fixtures" / "registry-v2-minimal.yaml"


def test_load_real_registry_has_core_datasets():
    ds = reg.load_registry(REAL)
    assert {"nyc_taxi", "gh_archive", "movielens", "tpch", "online_retail"} <= set(ds)
    assert ds["nyc_taxi"].kind == "http"
    assert ds["tpch"].kind == "tpch"
    assert ds["movielens"].unzip is True
    assert ds["online_retail"].kind == "http"
    assert ds["online_retail"].unzip is True
    assert all(dataset.provenance is not None for dataset in ds.values())
    assert sum(len(dataset.schemas) for dataset in ds.values()) == 24
    assert sum(len(dataset.artifacts) for dataset in ds.values()) == 15


def test_resolve_http_scale_returns_urls():
    ds = reg.load_registry(REAL)["nyc_taxi"]
    plan = reg.resolve_scale(ds, "tiny")
    assert plan.urls and plan.sf is None
    assert all(u.startswith("http") for u in plan.urls)
    assert [artifact.id for artifact in plan.artifacts] == ["yellow_2023_01"]


def test_resolve_tpch_scale_returns_sf():
    ds = reg.load_registry(REAL)["tpch"]
    plan = reg.resolve_scale(ds, "small")
    assert plan.sf == 1 and plan.urls == ()
    assert plan.generator_scale is ds.generator.scales["small"]
    assert len(plan.generator_scale.outputs) == 8


def test_unknown_scale_raises():
    ds = reg.load_registry(REAL)["nyc_taxi"]
    with pytest.raises(KeyError):
        reg.resolve_scale(ds, "gigantic")


def test_invalid_registry_raises(tmp_path: Path):
    bad = tmp_path / "r.yaml"
    bad.write_text("version: 1\ndatasets: {}\n")
    with pytest.raises(ValueError):
        reg.load_registry(bad)


def test_load_registry_rejects_version_1_even_when_otherwise_complete(tmp_path: Path):
    candidate = tmp_path / "registry.yaml"
    candidate.write_text(
        "version: 1\ndatasets:\n  sample:\n"
        "    description: old\n    format: csv\n    license: old\n"
        "    landing_prefix: sample\n    fetch: {kind: http}\n"
        "    scales: {tiny: {urls: [https://example.com/old.csv]}}\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="registry.version: must be integer 2"):
        reg.load_registry(candidate)


def test_real_registry_preserves_all_downloader_facing_scale_selections():
    datasets = reg.load_registry(REAL)
    taxi = tuple(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month}.parquet"
        for month in ("01", "02", "03", "04", "05", "06")
    )
    gh = tuple(f"https://data.gharchive.org/2023-01-01-{hour}.json.gz" for hour in range(6))
    expected_urls = {
        "nyc_taxi": {"tiny": taxi[:1], "small": taxi[:3], "medium": taxi},
        "gh_archive": {"tiny": gh[:1], "small": gh[:3], "medium": gh},
        "movielens": {
            "tiny": ("https://files.grouplens.org/datasets/movielens/ml-latest-small.zip",),
            "small": ("https://files.grouplens.org/datasets/movielens/ml-latest-small.zip",),
            "medium": ("https://files.grouplens.org/datasets/movielens/ml-25m.zip",),
        },
        "online_retail": {
            tier: ("https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip",)
            for tier in ("tiny", "small", "medium")
        },
    }
    for dataset_name, scales in expected_urls.items():
        for scale, expected in scales.items():
            plan = reg.resolve_scale(datasets[dataset_name], scale)
            assert plan.urls == expected
            assert tuple(artifact.url for artifact in plan.artifacts) == expected
    assert tuple((tier, reg.resolve_scale(datasets["tpch"], tier).sf) for tier in ("tiny", "small", "medium")) == (
        ("tiny", 0.01),
        ("small", 1),
        ("medium", 10),
    )


def test_load_v2_resolves_shared_http_artifacts_without_duplication():
    datasets = reg.load_registry_v2(V2_FIXTURE)
    direct = datasets["direct"]
    tiny = reg.resolve_scale(direct, "tiny")
    assert [artifact.id for artifact in tiny.artifacts] == ["sample"]
    assert tiny.urls == ("https://example.com/direct.parquet",)
    assert tiny.sf is None
    assert tiny.artifacts[0] is direct.artifacts["sample"]


def test_load_v2_resolves_archive_member_and_schema():
    archive = reg.load_registry_v2(V2_FIXTURE)["archive"]
    output = reg.resolve_scale(archive, "tiny").artifacts[0].outputs[0]
    assert output.member_path == "archive/ratings.csv"
    assert output.schema_id == "ratings"


def test_load_v2_resolves_generator_outputs_and_legacy_sf_view():
    generated = reg.load_registry_v2(V2_FIXTURE)["generated"]
    plan = reg.resolve_scale(generated, "tiny")
    assert plan.sf == 0.01
    assert plan.urls == ()
    assert plan.generator_scale is generated.generator.scales["tiny"]
    assert len(plan.generator_scale.outputs) == 8


@pytest.mark.parametrize("content", ["null\n", "[]\n", "registry\n", "2\n"])
def test_load_v2_reports_non_mapping_roots(tmp_path: Path, content: str):
    candidate = tmp_path / "registry.yaml"
    candidate.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match="registry: must be a mapping"):
        reg.load_registry_v2(candidate)


def test_v2_models_have_exact_contract_fields():
    expected = {
        reg.Provenance: (
            "publisher",
            "homepage",
            "license_name",
            "license_url",
            "attribution",
            "source_stability",
            "update_policy",
        ),
        reg.SchemaField: ("name", "logical_type", "nullable"),
        reg.SchemaContract: ("id", "format", "mode", "fields", "options", "fingerprint"),
        reg.SourceVersion: ("kind", "value"),
        reg.RawArtifact: ("name", "size_bytes", "sha256"),
        reg.LandingObject: (
            "object_name",
            "size_bytes",
            "sha256",
            "schema_id",
            "member_path",
            "raw_identity",
        ),
        reg.HttpArtifact: (
            "id",
            "url",
            "version",
            "stability",
            "evidence",
            "raw",
            "outputs",
            "provenance",
        ),
        reg.GeneratorEnvironment: (
            "image",
            "image_digest",
            "platform",
            "uv_lock_sha256",
            "locale",
            "timezone",
            "threads",
            "preserve_insertion_order",
        ),
        reg.GeneratorOutput: ("table", "object_name", "size_bytes", "sha256", "schema_id"),
        reg.GeneratorScale: ("name", "scale_factor", "outputs"),
        reg.GeneratorContract: (
            "engine_name",
            "engine_version",
            "engine_wheel_sha256",
            "extension_name",
            "extension_version_relation",
            "extension_repository_url",
            "extension_sha256",
            "environment",
            "procedure",
            "scale_parameter",
            "export_format",
            "compression",
            "row_group_size",
            "order_by",
            "scales",
        ),
        reg.ScalePlan: ("dataset", "scale", "urls", "sf", "artifacts", "generator_scale"),
    }
    assert {model: tuple(field.name for field in fields(model)) for model in expected} == expected


def test_v2_contract_is_deeply_read_only():
    direct = reg.load_registry_v2(V2_FIXTURE)["direct"]
    schema = direct.schemas["sample"]
    artifact = direct.artifacts["sample"]

    assert isinstance(direct.schemas, MappingProxyType)
    assert isinstance(direct.artifacts, MappingProxyType)
    assert isinstance(direct.scales, MappingProxyType)
    assert isinstance(schema.options, MappingProxyType)
    assert isinstance(artifact.evidence, MappingProxyType)
    assert isinstance(schema.fields, tuple)
    assert isinstance(artifact.outputs, tuple)
    assert isinstance(direct.scales["tiny"], tuple)
    with pytest.raises(TypeError):
        direct.artifacts["other"] = artifact
    with pytest.raises(FrozenInstanceError):
        artifact.url = "https://example.com/changed.parquet"

    archive_artifact = reg.load_registry_v2(V2_FIXTURE)["archive"].artifacts["release"]
    assert archive_artifact.provenance == reg.Provenance(
        publisher="Archive Release Publisher",
        homepage="https://example.com/archive-release",
        license_name="Archive Release License",
        license_url="https://example.com/archive-release-license",
        attribution="Archive Release Publisher",
        source_stability="mutable",
        update_policy="reviewed-lock-update",
    )
    assert direct.artifacts["sample"].provenance is None

    generator = reg.load_registry_v2(V2_FIXTURE)["generated"].generator
    assert isinstance(generator.order_by, MappingProxyType)
    assert isinstance(generator.scales, MappingProxyType)
    assert all(isinstance(columns, tuple) for columns in generator.order_by.values())
    assert all(isinstance(scale.outputs, tuple) for scale in generator.scales.values())
