"""Live acceptance for the dataset lock protocol against the pinned MinIO.

Every object created here is rooted in a UUID-owned namespace.  The finalizer
first removes the test pointer, making the associated generation inactive,
then removes only keys bearing that test identifier.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from datasets.locking import canonical_json
from datasets.s3 import (
    ConditionalConflict,
    acquire_lease,
    put_control_object,
    read_control_object,
    release_lease,
    renew_lease,
    s3_client_from_env,
)

pytestmark = [
    pytest.mark.infra,
    pytest.mark.skipif(
        os.environ.get("RUN_INFRA") != "1",
        reason="set RUN_INFRA=1 with the pinned Atlas stack running",
    ),
]

ROOT = Path(__file__).resolve().parents[2]
BUCKET = "landing"


def _status(error: ClientError) -> int | None:
    value = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return value if isinstance(value, int) else None


def _owned_keys(client, prefixes: tuple[str, ...]) -> tuple[str, ...]:
    keys: list[str] = []
    for prefix in prefixes:
        token: str | None = None
        while True:
            request: dict[str, object] = {"Bucket": BUCKET, "Prefix": prefix}
            if token is not None:
                request["ContinuationToken"] = token
            page = client.list_objects_v2(**request)
            keys.extend(item["Key"] for item in page.get("Contents", ()))
            if not page.get("IsTruncated", False):
                break
            token = page["NextContinuationToken"]
    return tuple(dict.fromkeys(keys))


def _delete_exact_keys(client, keys: tuple[str, ...]) -> None:
    for offset in range(0, len(keys), 1000):
        batch = keys[offset : offset + 1000]
        if batch:
            client.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
            )


@dataclass(frozen=True)
class LiveNamespace:
    client: object
    run_id: str
    dataset: str
    data_prefix: str
    pointer_key: str
    lease_key: str
    manifest_prefix: str
    protocol_prefix: str


@pytest.fixture
def live_namespace() -> LiveNamespace:
    client = s3_client_from_env(ROOT / "infra")
    run_id = uuid.uuid4().hex
    dataset = f"issue81_live_{run_id}"
    namespace = LiveNamespace(
        client=client,
        run_id=run_id,
        dataset=dataset,
        data_prefix=f"issue81-live/{run_id}/",
        pointer_key=f"_data-eng-locks/current/{dataset}.json",
        lease_key=f"_data-eng-locks/leases/{dataset}.json",
        manifest_prefix=f"_data-eng-locks/manifests/{dataset}/",
        protocol_prefix=f"_data-eng-live/{run_id}/",
    )
    try:
        yield namespace
    finally:
        # Removing this test-owned pointer first makes every test generation inactive.
        client.delete_object(Bucket=BUCKET, Key=namespace.pointer_key)
        prefixes = (
            namespace.data_prefix,
            namespace.manifest_prefix,
            namespace.protocol_prefix,
        )
        keys = _owned_keys(client, prefixes)
        _delete_exact_keys(client, keys)
        client.delete_object(Bucket=BUCKET, Key=namespace.lease_key)
        remaining = _owned_keys(client, prefixes)
        assert remaining == (), f"test-owned MinIO cleanup incomplete: {remaining}"
        close = getattr(client, "close", None)
        if callable(close):
            close()


class _LoseFirstPutResponse:
    """Commit one real MinIO PUT, then emulate a lost client response."""

    def __init__(self, client) -> None:
        self._client = client
        self.puts = 0

    def put_object(self, **request):
        response = self._client.put_object(**request)
        self.puts += 1
        if self.puts == 1:
            raise EndpointConnectionError(endpoint_url="https://redacted.invalid")
        return response

    def __getattr__(self, name: str):
        return getattr(self._client, name)


def test_minio_conditional_writes_preserve_opaque_etags_and_reconcile_lost_response(
    live_namespace: LiveNamespace,
) -> None:
    client = live_namespace.client
    key = f"{live_namespace.protocol_prefix}pointer.json"
    first = canonical_json({"generation": "first"})
    second = canonical_json({"generation": "second"})

    created = client.put_object(Bucket=BUCKET, Key=key, Body=first, IfNoneMatch="*")
    first_etag = created["ETag"]
    assert first_etag.startswith('"') and first_etag.endswith('"')

    with pytest.raises(ClientError) as duplicate:
        client.put_object(Bucket=BUCKET, Key=key, Body=second, IfNoneMatch="*")
    assert _status(duplicate.value) in {409, 412}

    replaced = client.put_object(Bucket=BUCKET, Key=key, Body=second, IfMatch=first_etag)
    second_etag = replaced["ETag"]
    assert second_etag != first_etag
    with pytest.raises(ClientError) as stale:
        client.put_object(Bucket=BUCKET, Key=key, Body=first, IfMatch=first_etag)
    assert _status(stale.value) in {409, 412}

    lost_key = f"{live_namespace.protocol_prefix}lost-response.json"
    lost_client = _LoseFirstPutResponse(client)
    reconciled = put_control_object(
        lost_client,
        BUCKET,
        lost_key,
        canonical_json({"outcome": "exact-self"}),
        if_none_match=True,
    )
    assert lost_client.puts == 1
    assert json.loads(reconciled.body) == {"outcome": "exact-self"}
    assert read_control_object(client, BUCKET, lost_key).etag == reconciled.etag
    print(
        canonical_json(
            {
                "first_etag": first_etag,
                "lost_response_etag": reconciled.etag,
                "replacement_etag": second_etag,
                "run_id": live_namespace.run_id,
            }
        ).decode("utf-8")
    )


def test_minio_lease_create_renew_release_takeover_and_stale_owner_aba(
    live_namespace: LiveNamespace,
) -> None:
    client = live_namespace.client
    publication_a = uuid.uuid4().hex
    owner_a = uuid.uuid4().hex
    first = acquire_lease(
        client,
        live_namespace.dataset,
        publication_a,
        owner_a,
        lease_seconds=5,
    )
    assert first.etag.startswith('"') and first.etag.endswith('"')

    # MinIO Date has one-second precision; wait for a distinct canonical renewal.
    time.sleep(1.1)
    renewed = renew_lease(client, first, lease_seconds=5)
    assert renewed.etag != first.etag
    with pytest.raises(ConditionalConflict):
        release_lease(client, first)
    released = release_lease(client, renewed)
    assert released.state == "released"

    publication_b = uuid.uuid4().hex
    owner_b = uuid.uuid4().hex
    successor = acquire_lease(
        client,
        live_namespace.dataset,
        publication_b,
        owner_b,
        lease_seconds=1,
    )
    time.sleep(1.1)
    publication_c = uuid.uuid4().hex
    owner_c = uuid.uuid4().hex
    takeover = acquire_lease(
        client,
        live_namespace.dataset,
        publication_c,
        owner_c,
        lease_seconds=5,
    )
    assert takeover.etag != successor.etag
    with pytest.raises(ConditionalConflict):
        renew_lease(client, successor, lease_seconds=5)
    with pytest.raises(ConditionalConflict):
        release_lease(client, successor)
    release_lease(client, takeover)
    print(
        canonical_json(
            {
                "dataset": live_namespace.dataset,
                "initial_etag": first.etag,
                "renewed_etag": renewed.etag,
                "run_id": live_namespace.run_id,
                "takeover_etag": takeover.etag,
            }
        ).decode("utf-8")
    )
