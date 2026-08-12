"""Live publication, acquisition, generator, resolver, and recovery acceptance."""

from __future__ import annotations

import hashlib
import os
import subprocess
import uuid
from dataclasses import replace

import pytest
from botocore.exceptions import EndpointConnectionError

from datasets.locking import canonical_json
from datasets.publication import (
    ActivePointer,
    PublishMode,
    active_pointer_key,
    immutable_manifest_key,
    publication_prefix,
    publish_dataset,
    resolve_active_dataset,
)
from datasets.registry import Dataset, load_registry, resolve_scale
from datasets.s3 import read_control_object, s3_client_from_env, stream_verify_object
from datasets.sources.http import fetch_http
from datasets.sources.tpch import DockerContainerRunner, generate_tpch
from datasets.verification import ExpectedObject, LockMismatch, VerificationContext
from tests.infra.test_dataset_lock_enforcement_live import (
    BUCKET,
    ROOT,
    _delete_exact_keys,
    _owned_keys,
)

pytestmark = [
    pytest.mark.infra,
    pytest.mark.skipif(
        os.environ.get("RUN_INFRA") != "1",
        reason="set RUN_INFRA=1 with the pinned Atlas stack running",
    ),
]

REGISTRY_PATH = ROOT / "datasets" / "registry.yaml"


def _registry_sha256() -> str:
    return hashlib.sha256(REGISTRY_PATH.read_bytes()).hexdigest()


def _test_dataset(source: Dataset, run_id: str) -> Dataset:
    return replace(
        source,
        name=f"issue81_live_{run_id}",
        landing_prefix=f"issue81-live/{run_id}",
    )


def _cleanup_dataset(client, dataset: Dataset) -> None:
    pointer_key = active_pointer_key(dataset.name)
    client.delete_object(Bucket=BUCKET, Key=pointer_key)
    prefixes = (
        f"{dataset.landing_prefix}/",
        f"_data-eng-locks/manifests/{dataset.name}/",
    )
    _delete_exact_keys(client, _owned_keys(client, prefixes))
    client.delete_object(Bucket=BUCKET, Key=f"_data-eng-locks/leases/{dataset.name}.json")
    assert _owned_keys(client, prefixes) == ()


class _MutationCounter:
    def __init__(self, client) -> None:
        self._client = client
        self.puts = 0
        self.deletes = 0

    def put_object(self, **request):
        self.puts += 1
        return self._client.put_object(**request)

    def delete_object(self, **request):
        self.deletes += 1
        return self._client.delete_object(**request)

    def delete_objects(self, **request):
        self.deletes += 1
        return self._client.delete_objects(**request)

    def __getattr__(self, name: str):
        return getattr(self._client, name)


class _LoseExactPointerPutResponse:
    """Commit one exact conditional pointer PUT, then lose its response."""

    def __init__(self, client, pointer_key: str) -> None:
        self._client = client
        self._pointer_key = pointer_key
        self.pointer_put_attempts = 0
        self.lost_responses = 0

    def put_object(self, **request):
        if request.get("Key") != self._pointer_key:
            return self._client.put_object(**request)
        assert request.get("IfNoneMatch") == "*" or "IfMatch" in request
        self.pointer_put_attempts += 1
        response = self._client.put_object(**request)
        if self.lost_responses == 0:
            self.lost_responses += 1
            raise EndpointConnectionError(endpoint_url="https://redacted.invalid")
        return response

    def __getattr__(self, name: str):
        return getattr(self._client, name)


def _unexpected_fetch(_plan, _destination):
    raise AssertionError("an exact rerun must not acquire or generate source bytes")


def test_tiny_http_publication_is_idempotent_and_recovers_without_mixed_generations() -> None:
    registry_before = REGISTRY_PATH.read_bytes()
    registry = load_registry(REGISTRY_PATH)
    run_id = uuid.uuid4().hex
    dataset = _test_dataset(registry["movielens"], run_id)
    plan = resolve_scale(dataset, "tiny")
    namespaced_registry = {dataset.name: dataset}
    client = s3_client_from_env(ROOT / "infra")
    digest = _registry_sha256()
    try:
        first = publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=client,
            fetcher=fetch_http,
            raw_registry_sha256=digest,
        )
        first_resolution = resolve_active_dataset(client, namespaced_registry, dataset.name, "tiny")
        assert first.object_count == 5
        assert first.manifest_sha256 == first_resolution.manifest_sha256

        counter = _MutationCounter(client)
        second = publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=counter,
            fetcher=_unexpected_fetch,
            raw_registry_sha256=digest,
        )
        assert second.status == "verified-existing"
        assert second.manifest_sha256 == first.manifest_sha256
        assert counter.puts == counter.deletes == 0

        refreshed = publish_dataset(
            plan,
            mode=PublishMode.REFRESH,
            client=client,
            fetcher=fetch_http,
            raw_registry_sha256=digest,
        )
        current = resolve_active_dataset(client, namespaced_registry, dataset.name, "tiny")
        assert refreshed.manifest_sha256 == current.manifest_sha256
        assert refreshed.manifest_sha256 != first.manifest_sha256
        assert {item.uri.rsplit("/", 1)[0] for item in first_resolution.objects} != {
            item.uri.rsplit("/", 1)[0] for item in current.objects
        }
        assert all("/_generations/" in item.uri for item in (*first_resolution.objects, *current.objects))

        # A corrupt object in a never-referenced UUID candidate cannot affect readers.
        inactive_prefix = publication_prefix(plan, uuid.uuid4().hex)
        client.put_object(Bucket=BUCKET, Key=f"{inactive_prefix}/corrupt.bin", Body=b"corrupt")
        assert resolve_active_dataset(client, namespaced_registry, dataset.name, "tiny") == current

        # Corrupt one active object out of band, prove fail-closed/no pointer mutation,
        # then restore its exact test-owned bytes so recovery can proceed safely.
        active_key = current.objects[0].uri.removeprefix(f"s3://{BUCKET}/")
        saved = client.get_object(Bucket=BUCKET, Key=active_key)
        saved_body = saved["Body"].read()
        saved["Body"].close()
        saved_metadata = dict(saved.get("Metadata", {}))
        pointer_before = read_control_object(client, BUCKET, active_pointer_key(dataset.name))
        client.put_object(Bucket=BUCKET, Key=active_key, Body=b"corrupt", Metadata=saved_metadata)
        with pytest.raises(LockMismatch):
            publish_dataset(
                plan,
                mode=PublishMode.DEFAULT,
                client=client,
                fetcher=_unexpected_fetch,
                raw_registry_sha256=digest,
            )
        pointer_after_failure = read_control_object(client, BUCKET, active_pointer_key(dataset.name))
        assert pointer_after_failure.body == pointer_before.body
        assert pointer_after_failure.etag == pointer_before.etag
        client.put_object(Bucket=BUCKET, Key=active_key, Body=saved_body, Metadata=saved_metadata)

        recovered = publish_dataset(
            plan,
            mode=PublishMode.REFRESH,
            client=client,
            fetcher=fetch_http,
            raw_registry_sha256=digest,
        )
        assert recovered.manifest_sha256 not in {first.manifest_sha256, refreshed.manifest_sha256}
        rolled_back = publish_dataset(
            plan,
            mode=PublishMode.ROLLBACK,
            client=client,
            fetcher=_unexpected_fetch,
            rollback_sha256=first.manifest_sha256,
            raw_registry_sha256=digest,
        )
        assert rolled_back.manifest_sha256 == first.manifest_sha256
        assert resolve_active_dataset(client, namespaced_registry, dataset.name, "tiny") == first_resolution

        # Move the active pointer to a different selected scale while retaining
        # an already-resolved reader's immutable generation.  Create two
        # generations under the new plan, then roll back within that still-
        # current plan.
        small_plan = resolve_scale(dataset, "small")
        small_first = publish_dataset(
            small_plan,
            mode=PublishMode.REFRESH,
            client=client,
            fetcher=fetch_http,
            raw_registry_sha256=digest,
        )
        assert resolve_active_dataset(client, namespaced_registry, dataset.name, "small").manifest_sha256 == (
            small_first.manifest_sha256
        )
        for item in first_resolution.objects:
            key = item.uri.removeprefix(f"s3://{BUCKET}/")
            stream_verify_object(
                client,
                BUCKET,
                key,
                ExpectedObject(item.object_name, item.size_bytes, item.sha256, item.schema_id),
                VerificationContext(dataset.name, "tiny", "stale reader", object_name=item.object_name),
            )
        small_second = publish_dataset(
            small_plan,
            mode=PublishMode.REFRESH,
            client=client,
            fetcher=fetch_http,
            raw_registry_sha256=digest,
        )
        assert small_second.manifest_sha256 != small_first.manifest_sha256
        small_rollback = publish_dataset(
            small_plan,
            mode=PublishMode.ROLLBACK,
            client=client,
            fetcher=_unexpected_fetch,
            rollback_sha256=small_first.manifest_sha256,
            raw_registry_sha256=digest,
        )
        assert small_rollback.manifest_sha256 == small_first.manifest_sha256
        assert resolve_active_dataset(client, namespaced_registry, dataset.name, "small").manifest_sha256 == (
            small_first.manifest_sha256
        )
        assert REGISTRY_PATH.read_bytes() == registry_before
        print(
            canonical_json(
                {
                    "dataset": dataset.name,
                    "first_manifest_sha256": first.manifest_sha256,
                    "first_publication_id": first.publication_id,
                    "recovered_manifest_sha256": recovered.manifest_sha256,
                    "recovered_publication_id": recovered.publication_id,
                    "refreshed_manifest_sha256": refreshed.manifest_sha256,
                    "refreshed_publication_id": refreshed.publication_id,
                    "rollback_manifest_sha256": rolled_back.manifest_sha256,
                    "scale_switch_manifest_sha256": small_first.manifest_sha256,
                    "scale_switch_publication_id": small_first.publication_id,
                    "scale_switch_rollback_manifest_sha256": small_rollback.manifest_sha256,
                }
            ).decode("utf-8")
        )
    finally:
        _cleanup_dataset(client, dataset)
        close = getattr(client, "close", None)
        if callable(close):
            close()


def test_publish_reconciles_lost_real_minio_active_pointer_response_without_retry() -> None:
    registry = load_registry(REGISTRY_PATH)
    run_id = uuid.uuid4().hex
    dataset = _test_dataset(registry["movielens"], run_id)
    plan = resolve_scale(dataset, "tiny")
    registry_view = {dataset.name: dataset}
    client = s3_client_from_env(ROOT / "infra")
    pointer_key = active_pointer_key(dataset.name)
    losing_client = _LoseExactPointerPutResponse(client, pointer_key)
    try:
        result = publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=losing_client,
            fetcher=fetch_http,
            raw_registry_sha256=_registry_sha256(),
        )

        assert losing_client.pointer_put_attempts == 1
        assert losing_client.lost_responses == 1
        assert result.status == "published-reconciled"
        # `committed` is the canonical pointer outcome; reconciliation is
        # carried by the status because the exact intended bytes were observed.
        assert result.pointer_outcome == "committed"
        assert result.manifest_sha256 is not None
        assert result.manifest_key == immutable_manifest_key(dataset.name, result.manifest_sha256)

        pointer_snapshot = read_control_object(client, BUCKET, pointer_key)
        pointer = ActivePointer.from_bytes(pointer_snapshot.body)
        resolved = resolve_active_dataset(client, registry_view, dataset.name, "tiny")
        assert pointer_snapshot.etag.startswith('"') and pointer_snapshot.etag.endswith('"')
        assert pointer.dataset == dataset.name
        assert pointer.manifest_key == result.manifest_key
        assert pointer.manifest_sha256 == result.manifest_sha256
        assert resolved.manifest_sha256 == result.manifest_sha256
        assert resolved.publication_id == result.publication_id
        assert all(f"/{result.publication_id}/" in item.uri for item in resolved.objects)
        print(
            canonical_json(
                {
                    "dataset": dataset.name,
                    "manifest_sha256": result.manifest_sha256,
                    "pointer_etag": pointer_snapshot.etag,
                    "pointer_put_attempts": losing_client.pointer_put_attempts,
                    "publication_id": result.publication_id,
                    "status": result.status,
                }
            ).decode("utf-8")
        )
    finally:
        _cleanup_dataset(client, dataset)
        close = getattr(client, "close", None)
        if callable(close):
            close()


def test_canonical_tiny_tpch_builds_linux_amd64_and_second_run_generates_nothing() -> None:
    registry = load_registry(REGISTRY_PATH)
    run_id = uuid.uuid4().hex
    dataset = _test_dataset(registry["tpch"], run_id)
    plan = resolve_scale(dataset, "tiny")
    client = s3_client_from_env(ROOT / "infra")
    digest = _registry_sha256()
    runner = DockerContainerRunner()
    try:
        evidence = runner.ensure_image(dataset.generator)
        assert evidence.platform == "linux/amd64"
        first = publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=client,
            fetcher=lambda selected, destination: generate_tpch(selected, destination, runner=runner),
            raw_registry_sha256=digest,
        )
        resolved = resolve_active_dataset(client, {dataset.name: dataset}, dataset.name, "tiny")
        assert first.object_count == len(resolved.objects) == 8
        assert tuple(item.object_name for item in resolved.objects) == (
            "customer.parquet",
            "lineitem.parquet",
            "nation.parquet",
            "orders.parquet",
            "part.parquet",
            "partsupp.parquet",
            "region.parquet",
            "supplier.parquet",
        )
        counter = _MutationCounter(client)
        repeated = publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=counter,
            fetcher=_unexpected_fetch,
            raw_registry_sha256=digest,
        )
        assert repeated.manifest_sha256 == first.manifest_sha256
        assert counter.puts == counter.deletes == 0
        print(
            canonical_json(
                {
                    "dataset": dataset.name,
                    "manifest_sha256": first.manifest_sha256,
                    "object_count": first.object_count,
                    "publication_id": first.publication_id,
                }
            ).decode("utf-8")
        )
    finally:
        _cleanup_dataset(client, dataset)
        close = getattr(client, "close", None)
        if callable(close):
            close()


def _container_id(service: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            "label=com.docker.compose.project=data-eng-lab",
            "--filter",
            f"label=com.docker.compose.service={service}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    container_id = result.stdout.strip()
    assert container_id, f"{service} container is not running"
    return container_id


@pytest.mark.parametrize("service", ["airflow-scheduler", "jupyterhub", "zeppelin"])
def test_consumer_container_resolves_one_verified_generation_without_flat_path(service: str) -> None:
    script = """
import json
import os
import urllib.request

base = os.environ['DATASET_RESOLVER_URI'].rstrip('/')
assert base == 'http://dataset-resolver:8080'
with urllib.request.urlopen(base + '/healthz', timeout=10) as response:
    assert json.load(response) == {'status': 'ok'}
assert '/landing/' not in base
request = urllib.request.Request(
    base + '/v1/resolve',
    data=json.dumps({'dataset': 'movielens', 'expected_scale': 'tiny'}).encode(),
    headers={'Content-Type': 'application/json'},
    method='POST',
)
with urllib.request.urlopen(request, timeout=120) as response:
    resolved = json.load(response)
assert resolved['dataset'] == 'movielens'
assert resolved['scale'] == 'tiny'
assert len(resolved['objects']) == 5
assert len({item['uri'].rsplit('/', 1)[0] for item in resolved['objects']}) == 1
assert all('/_generations/' in item['uri'] for item in resolved['objects'])
"""
    subprocess.run(
        ["docker", "exec", _container_id(service), "python3", "-c", script],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_jupyter_spark_reads_one_row_from_resolved_immutable_uri() -> None:
    script = """
import json
import os
import urllib.request

from pyspark.sql import SparkSession

base = os.environ['DATASET_RESOLVER_URI'].rstrip('/')
request = urllib.request.Request(
    base + '/v1/resolve',
    data=json.dumps({'dataset': 'movielens', 'expected_scale': 'tiny'}).encode(),
    headers={'Content-Type': 'application/json'},
    method='POST',
)
with urllib.request.urlopen(request, timeout=120) as response:
    resolved = json.load(response)
ratings = next(item for item in resolved['objects'] if item['object_name'] == 'ratings.csv')
assert '/_generations/' in ratings['uri']
spark = SparkSession.builder.remote('sc://spark-connect:15002').getOrCreate()
try:
    rows = spark.read.option('header', 'true').csv(ratings['uri'].replace('s3://', 's3a://', 1)).limit(1).count()
    assert rows == 1
finally:
    spark.stop()
"""
    subprocess.run(
        ["docker", "exec", _container_id("jupyterhub"), "python3", "-c", script],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_manifest_key_helper_retains_full_content_address() -> None:
    """Keep the live evidence code from ever truncating rollback identifiers."""
    digest = "a" * 64
    assert immutable_manifest_key("issue81_live_evidence", digest).endswith(f"/{digest}.json")
