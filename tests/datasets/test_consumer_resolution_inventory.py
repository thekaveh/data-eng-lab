"""Exact executable-surface inventory for immutable dataset resolution."""

from __future__ import annotations

import json
import re
import urllib.request
from pathlib import Path

import nbformat
import pytest

ROOT = Path(__file__).resolve().parents[2]
LEGACY_LANDING_PATTERN = re.compile(r"s3a?://landing/(?:\{dataset\}|[a-z0-9_.-]+)(?:/(?!_generations/)|(?=[\"' )]))")
RESOLVER_BYPASS_PATTERN = re.compile(r"landingPrefix|pathsForScale|DefaultScale")
SCALES = ("tiny", "small", "medium")
RESOLVED_NOTEBOOKS = {
    "batch_ingest-nyc_taxi-spark-iceberg": "nyc_taxi",
    "feature_engineering-movielens-spark-iceberg": "movielens",
    "join_optimization-tpch-spark-iceberg": "tpch",
    "json_flatten-gh_archive-spark-iceberg": "gh_archive",
    "sessionization-gh_archive-spark-iceberg": "gh_archive",
    "star_schema-tpch-spark-iceberg": "tpch",
    "streaming_ingest-gh_archive-spark-iceberg": "gh_archive",
}


def _runtime_sources() -> tuple[Path, ...]:
    paths = [
        ROOT / "scripts" / "bronze_smoke.py",
        ROOT / "scripts" / "new_scenario.py",
        *sorted((ROOT / "scripts").rglob("*.sh")),
        *sorted((ROOT / "scripts").rglob("*.sql")),
        *sorted((ROOT / "spark-apps").rglob("dag.py")),
        *sorted((ROOT / "spark-apps").rglob("src/main/scala/**/*.scala")),
        *sorted((ROOT / "scenarios").rglob("*.py")),
        *sorted((ROOT / "scenarios").rglob("*.sh")),
        *sorted((ROOT / "scenarios").rglob("*.sql")),
        *sorted((ROOT / "scenarios").rglob("jupyter/notebook.ipynb")),
        *sorted((ROOT / "scenarios").rglob("zeppelin/notebook.zpln")),
        ROOT / "tests" / "scenarios" / "live_exec.py",
        ROOT / "tests" / "scenarios" / "test_notebook_reproducibility_live.py",
    ]
    return tuple(path for path in paths if path.is_file())


def scan_runtime_sources(pattern: re.Pattern[str]) -> list[dict[str, object]]:
    """Return a stable, machine-readable inventory grouped by runtime path."""
    offenders: list[dict[str, object]] = []
    for path in _runtime_sources():
        matches = tuple(dict.fromkeys(pattern.findall(path.read_text(encoding="utf-8"))))
        if matches:
            offenders.append(
                {
                    "path": path.relative_to(ROOT).as_posix(),
                    "runtime": path.suffix.removeprefix("."),
                    "legacy_uris": matches,
                }
            )
    return offenders


def test_runtime_consumers_do_not_construct_flat_landing_paths():
    assert scan_runtime_sources(LEGACY_LANDING_PATTERN) == []


def test_runtime_inventory_includes_all_executable_surfaces_and_no_resolver_bypass():
    inventory = {path.relative_to(ROOT).as_posix() for path in _runtime_sources()}
    assert "spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/TaxiLanding.scala" in inventory
    assert "tests/scenarios/live_exec.py" in inventory
    assert any(path.endswith(".sh") for path in inventory)
    assert scan_runtime_sources(RESOLVER_BYPASS_PATTERN) == []


@pytest.mark.parametrize("scenario,dataset", sorted(RESOLVED_NOTEBOOKS.items()))
def test_each_migrated_notebook_bootstraps_one_frozen_expected_scale_result(scenario, dataset):
    root = ROOT / "scenarios" / scenario
    jupyter = nbformat.read(root / "jupyter" / "notebook.ipynb", as_version=4)
    jupyter_code = "\n".join(cell.source for cell in jupyter.cells if cell.cell_type == "code")
    zeppelin = json.loads((root / "zeppelin" / "notebook.zpln").read_text(encoding="utf-8"))
    zeppelin_code = "\n".join(paragraph["text"] for paragraph in zeppelin["paragraphs"])
    for code in (jupyter_code, zeppelin_code):
        assert code.count("/v1/resolve") == 1
        assert "DATASET_RESOLVER_URI" in code
        assert "DATASET_SCALE" in code
        assert dataset in code
        assert "expected_scale" in code
        assert "_generations/" in code
        assert "size_bytes" in code and "sha256" in code and "schema_id" in code
        assert "dataset_spark_uris" in code or "datasetSparkUris" in code
        assert 'replace("s3://", "s3a://"' in code or '"s3a://" + uri.stripPrefix("s3://")' in code
        assert "readNBytes" in code or "read(_MAX_RESOLUTION_BYTES + 1)" in code
        assert "STRICT_DUPLICATE_DETECTION" in code or "object_pairs_hook" in code


def test_migrated_notebooks_preserve_resolver_object_order_without_globs():
    for scenario in RESOLVED_NOTEBOOKS:
        root = ROOT / "scenarios" / scenario
        text = (root / "jupyter" / "notebook.ipynb").read_text(encoding="utf-8")
        text += (root / "zeppelin" / "notebook.zpln").read_text(encoding="utf-8")
        assert "objects" in text and "object_name" in text and "uri" in text
        assert "sorted(" not in text and ".sorted" not in text
        assert "/*" not in text and "*.parquet" not in text and "*.json" not in text


def test_dags_and_smoke_defer_exact_resolution_to_run_time():
    for path in (
        ROOT / "spark-apps" / "nyc-taxi-etl" / "dag.py",
        ROOT / "spark-apps" / "nyc-taxi-medallion" / "dag.py",
        ROOT / "scripts" / "bronze_smoke.py",
    ):
        text = path.read_text(encoding="utf-8")
        assert text.count("/v1/resolve") == 1
        assert "DATASET_RESOLVER_URI" in text
        assert "DATASET_SCALE" in text
        assert "expected_scale" in text
        assert "_generations/" in text
        assert "size_bytes" in text and "sha256" in text and "schema_id" in text


def test_scala_entrypoints_require_explicit_verified_immutable_uri_arguments():
    for path in (
        ROOT / "spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/NycTaxiEtl.scala",
        ROOT / "spark-apps/nyc-taxi-medallion/src/main/scala/com/thekaveh/dataeng/medallion/NycTaxiMedallion.scala",
    ):
        text = path.read_text(encoding="utf-8")
        assert "_generations/" in text
        assert "verified immutable NYC Taxi URI arguments are required" in text
        assert "args.nonEmpty" in text
        assert '"s3a://" + uri.stripPrefix("s3://")' in text


def test_medallion_processing_is_driven_by_frozen_resolver_inputs():
    text = (
        ROOT / "spark-apps/nyc-taxi-medallion/src/main/scala/com/thekaveh/dataeng/medallion/NycTaxiMedallion.scala"
    ).read_text(encoding="utf-8")
    assert "readResolved" in text
    assert "arguments.sparkUris" in text
    assert "spark.table(arguments.bronzeTable)" not in text


def test_streaming_checkpoint_is_keyed_by_frozen_publication_identity():
    scenario = ROOT / "scenarios/streaming_ingest-gh_archive-spark-iceberg"
    for path in (scenario / "jupyter/notebook.ipynb", scenario / "zeppelin/notebook.zpln"):
        text = path.read_text(encoding="utf-8")
        assert "gh_events_file/" in text
        assert "manifest_sha256" in text
        assert "dataset_scale" in text


def _taxi_resolution_body(*, objects=None, extra=None) -> bytes:
    plan = "1" * 64
    publication = "a" * 32
    names = tuple(f"yellow_tripdata_2023-{month:02d}.parquet" for month in range(1, 4))
    document = {
        "dataset": "nyc_taxi",
        "scale": "small",
        "plan_id": plan,
        "manifest_sha256": "2" * 64,
        "publication_id": publication,
        "objects": objects
        if objects is not None
        else [
            {
                "object_name": name,
                "uri": f"s3://landing/nyc_taxi/_generations/{plan}/{publication}/{name}",
                "size_bytes": 1,
                "sha256": "3" * 64,
                "schema_id": "taxi-v1",
            }
            for name in names
        ],
    }
    if extra:
        document.update(extra)
    return json.dumps(document, separators=(",", ":")).encode()


def _run_taxi_jupyter_bootstrap(monkeypatch, body: bytes, *, override="small"):
    notebook = nbformat.read(
        ROOT / "scenarios/batch_ingest-nyc_taxi-spark-iceberg/jupyter/notebook.ipynb", as_version=4
    )
    source = next(cell.source for cell in notebook.cells if cell.cell_type == "code" and "/v1/resolve" in cell.source)
    source = source[source.index("import json") :]

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, size):
            return body[:size]

    monkeypatch.setenv("DATASET_RESOLVER_URI", "http://resolver")
    monkeypatch.setenv("DATASET_SCALE", "small")
    monkeypatch.setattr(urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    namespace = {} if override is None else {"dataset_scale_override": override}
    exec(compile(source, "notebook.ipynb", "exec"), namespace)
    return namespace


def test_jupyter_bootstrap_executes_strict_success_and_spark_boundary(monkeypatch):
    namespace = _run_taxi_jupyter_bootstrap(monkeypatch, _taxi_resolution_body())
    assert all(uri.startswith("s3://") for uri in namespace["dataset_uris"])
    assert all(uri.startswith("s3a://") for uri in namespace["dataset_spark_uris"])
    assert namespace["dataset_object_by_name"]["yellow_tripdata_2023-01.parquet"].startswith("s3a://")


@pytest.mark.parametrize(
    "body",
    [
        b'{"dataset":"nyc_taxi","dataset":"nyc_taxi"}',
        b"{" + b'"nested":' * 18 + b"0" + b"}" * 18,
        _taxi_resolution_body(objects=[]),
        _taxi_resolution_body(extra={"unexpected": True}),
        _taxi_resolution_body(objects=[{"object_name": 1}]),
        b"x" * ((1 << 20) + 1),
    ],
    ids=("duplicate", "depth", "empty", "extra", "wrong-types", "oversized"),
)
def test_jupyter_bootstrap_rejects_malformed_responses(monkeypatch, body):
    with pytest.raises((ValueError, json.JSONDecodeError)):
        _run_taxi_jupyter_bootstrap(monkeypatch, body)


def test_jupyter_explicit_empty_scale_override_does_not_fall_through(monkeypatch):
    with pytest.raises(ValueError, match="invalid DATASET_SCALE"):
        _run_taxi_jupyter_bootstrap(monkeypatch, _taxi_resolution_body(), override="")


def test_streaming_checkpoint_changes_across_scale_and_generation_with_runtime_parity():
    def checkpoint(scale, publication, manifest):
        return f"s3a://checkpoints/gh_events_file/{scale}/{publication}/{manifest}"

    assert checkpoint("tiny", "a" * 32, "1" * 64) != checkpoint("small", "a" * 32, "1" * 64)
    assert checkpoint("small", "a" * 32, "1" * 64) != checkpoint("small", "b" * 32, "1" * 64)
    assert checkpoint("small", "a" * 32, "1" * 64) != checkpoint("small", "a" * 32, "2" * 64)
    scenario = ROOT / "scenarios/streaming_ingest-gh_archive-spark-iceberg"
    jupyter = (scenario / "jupyter/notebook.ipynb").read_text(encoding="utf-8")
    zeppelin = (scenario / "zeppelin/notebook.zpln").read_text(encoding="utf-8")
    assert "gh_events_file/{dataset_scale}/{publication_id}/{manifest_sha256}" in jupyter
    assert "gh_events_file/$datasetScale/$publicationId/$manifestSha256" in zeppelin
