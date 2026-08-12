import importlib.util
import io
import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("bronze_smoke", ROOT / "scripts" / "bronze_smoke.py")
bs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bs)


def test_bronze_table_name_default():
    # pure helper: derives the target table for a landing prefix
    assert bs.bronze_table("nyc_taxi") == "lakehouse.bronze.nyc_taxi"


@pytest.mark.parametrize("scale", ["tiny", "small", "medium"])
def test_effective_scale_prefers_explicit_then_environment_then_small(scale):
    assert bs.effective_scale(scale, {"DATASET_SCALE": "medium"}) == scale
    assert bs.effective_scale(None, {"DATASET_SCALE": scale}) == scale
    assert bs.effective_scale(None, {}) == "small"


@pytest.mark.parametrize("scale", ["", "large", "SMALL"])
def test_effective_scale_rejects_invalid_values(scale):
    with pytest.raises(ValueError, match="tiny, small, medium"):
        bs.effective_scale(scale, {})


class _Response(io.BytesIO):
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def _resolution(*, dataset="nyc_taxi", scale="small", objects=None):
    if objects is None:
        objects = [
            {
                "object_name": "yellow_tripdata_2023-01.parquet",
                "uri": "s3://landing/nyc_taxi/_generations/"
                + "1" * 64
                + "/"
                + "a" * 32
                + "/yellow_tripdata_2023-01.parquet",
                "size_bytes": 3,
                "sha256": "2" * 64,
                "schema_id": "taxi",
            }
        ]
    return {
        "dataset": dataset,
        "scale": scale,
        "plan_id": "1" * 64,
        "manifest_sha256": "2" * 64,
        "publication_id": "a" * 32,
        "objects": objects,
    }


def test_resolve_dataset_posts_exact_request_once_and_preserves_object_order():
    calls = []
    document = _resolution(
        objects=[
            _resolution()["objects"][0],
            {
                **_resolution()["objects"][0],
                "object_name": "yellow_tripdata_2023-02.parquet",
                "uri": _resolution()["objects"][0]["uri"].replace("01", "02"),
            },
        ]
    )

    def opener(request, timeout):
        calls.append((request, timeout))
        return _Response(json.dumps(document, separators=(",", ":"), sort_keys=True).encode())

    result = bs.resolve_dataset("nyc_taxi", "small", "http://resolver:8080", opener=opener)
    assert result == tuple(item["uri"] for item in document["objects"])
    assert len(calls) == 1
    request, timeout = calls[0]
    assert request.full_url == "http://resolver:8080/v1/resolve"
    assert request.method == "POST" and timeout == 120
    assert json.loads(request.data) == {"dataset": "nyc_taxi", "expected_scale": "small"}


def test_build_bronze_converts_only_validated_canonical_uris_to_spark_s3a():
    seen = []

    class Reader:
        def parquet(self, *uris):
            seen.extend(uris)
            return Frame()

    class Frame:
        def writeTo(self, _table):
            return self

        def using(self, _format):
            return self

        def createOrReplace(self):
            return None

        def count(self):
            return 1

    class Spark:
        read = Reader()

        def table(self, _table):
            return Frame()

    canonical = _resolution()["objects"][0]["uri"]
    assert bs.build_bronze(Spark(), (canonical,), "lakehouse.bronze.taxi") == 1
    assert seen == [canonical.replace("s3://", "s3a://", 1)]
    with pytest.raises(ValueError, match="verified immutable URI"):
        bs.build_bronze(Spark(), ("s3://landing/nyc_taxi/file.parquet",), "lakehouse.bronze.taxi")


@pytest.mark.parametrize(
    "document",
    [
        _resolution(dataset="other"),
        _resolution(scale="medium"),
        _resolution(objects=[]),
        _resolution(objects=[_resolution()["objects"][0], _resolution()["objects"][0]]),
        _resolution(objects=[{**_resolution()["objects"][0], "uri": "s3://landing/nyc_taxi/x"}]),
        {**_resolution(), "manifest_sha256": "bad"},
        _resolution(objects=[{**_resolution()["objects"][0], "size_bytes": -1}]),
        _resolution(objects=[{**_resolution()["objects"][0], "sha256": "bad"}]),
        _resolution(objects=[{**_resolution()["objects"][0], "schema_id": ""}]),
        _resolution(
            objects=[
                {
                    **_resolution()["objects"][0],
                    "object_name": "../escape.parquet",
                    "uri": _resolution()["objects"][0]["uri"].rsplit("/", 1)[0] + "/../escape.parquet",
                }
            ]
        ),
    ],
)
def test_resolve_dataset_rejects_missing_mismatched_duplicate_or_mutable_results(document):
    def opener(*_args, **_kwargs):
        return _Response(json.dumps(document).encode())

    with pytest.raises(ValueError, match="dataset resolution failed"):
        bs.resolve_dataset("nyc_taxi", "small", "http://resolver:8080", opener=opener)


@pytest.mark.infra
@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs a live enhanced-Atlas stack")
def test_build_bronze_end_to_end():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
    paths = bs.resolve_dataset(
        "nyc_taxi",
        bs.effective_scale(None, os.environ),
        os.environ["DATASET_RESOLVER_URI"],
    )
    n = bs.build_bronze(spark, paths, bs.bronze_table("nyc_taxi_trips"))
    assert n > 0
