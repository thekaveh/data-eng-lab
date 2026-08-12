#!/usr/bin/env python3
"""Bronze smoke: load a landing dataset into an Iceberg bronze table via Spark Connect."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request
from collections.abc import Mapping, Sequence

SCALES = ("tiny", "small", "medium")
_RESULT_FIELDS = {
    "dataset",
    "scale",
    "plan_id",
    "manifest_sha256",
    "publication_id",
    "objects",
}
_OBJECT_FIELDS = {"object_name", "uri", "size_bytes", "sha256", "schema_id"}


def effective_scale(explicit: str | None, environment: Mapping[str, str]) -> str:
    scale = explicit if explicit is not None else environment.get("DATASET_SCALE", "small")
    if scale not in SCALES:
        raise ValueError("dataset scale must be one of: tiny, small, medium")
    return scale


def _unique_mapping(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("dataset resolution failed")
        result[key] = value
    return result


def resolve_dataset(dataset: str, scale: str, resolver_uri: str, *, opener=urllib.request.urlopen) -> tuple[str, ...]:
    """Resolve and freeze one ordered immutable URI tuple for this process run."""
    request = urllib.request.Request(
        resolver_uri.rstrip("/") + "/v1/resolve",
        data=json.dumps(
            {"dataset": dataset, "expected_scale": scale},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with opener(request, timeout=120) as response:
            body = response.read((1 << 20) + 1)
        if len(body) > 1 << 20:
            raise ValueError
        document = json.loads(body, object_pairs_hook=_unique_mapping)
        if not isinstance(document, dict) or set(document) != _RESULT_FIELDS:
            raise ValueError
        if document["dataset"] != dataset or document["scale"] != scale:
            raise ValueError
        plan = document["plan_id"]
        publication = document["publication_id"]
        manifest_sha256 = document["manifest_sha256"]
        if not isinstance(plan, str) or re.fullmatch(r"[0-9a-f]{64}", plan) is None:
            raise ValueError
        if not isinstance(publication, str) or re.fullmatch(r"[0-9a-f]{32}", publication) is None:
            raise ValueError
        if not isinstance(manifest_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None:
            raise ValueError
        objects = document["objects"]
        if not isinstance(objects, list) or not objects:
            raise ValueError
        names: set[str] = set()
        uris: list[str] = []
        prefix = f"s3://landing/{dataset}/_generations/{plan}/{publication}/"
        for item in objects:
            if not isinstance(item, dict) or set(item) != _OBJECT_FIELDS:
                raise ValueError
            name, uri = item["object_name"], item["uri"]
            if (
                not isinstance(name, str)
                or re.fullmatch(r"[A-Za-z0-9._-]+", name) is None
                or name in {".", ".."}
                or name in names
                or not isinstance(uri, str)
                or isinstance(item["size_bytes"], bool)
                or not isinstance(item["size_bytes"], int)
                or item["size_bytes"] < 0
                or not isinstance(item["sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
                or not isinstance(item["schema_id"], str)
                or not item["schema_id"]
            ):
                raise ValueError
            if uri != prefix + name:
                raise ValueError
            names.add(name)
            uris.append(uri)
        return tuple(uris)
    except Exception as error:
        raise ValueError("dataset resolution failed") from error


def bronze_table(name: str) -> str:
    return f"lakehouse.bronze.{name}"


def build_bronze(spark, immutable_uris: Sequence[str], table: str) -> int:
    df = spark.read.parquet(*immutable_uris)
    (df.writeTo(table).using("iceberg").createOrReplace())
    return spark.table(table).count()


def main(argv=None) -> int:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description="Publish verified NYC Taxi data to bronze.")
    parser.add_argument("--scale", choices=SCALES)
    parser.add_argument("--resolver-uri", default=os.environ.get("DATASET_RESOLVER_URI"))
    args = parser.parse_args(argv)
    if not args.resolver_uri:
        parser.error("DATASET_RESOLVER_URI or --resolver-uri is required")
    scale = effective_scale(args.scale, os.environ)
    immutable_uris = resolve_dataset("nyc_taxi", scale, args.resolver_uri)
    spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
    n = build_bronze(spark, immutable_uris, bronze_table("nyc_taxi_trips"))
    print(f"bronze lakehouse.bronze.nyc_taxi_trips: {n} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
