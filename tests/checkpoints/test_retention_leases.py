from __future__ import annotations

import json
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.checkpoints.leases import (
    AcquireRequest,
    HeartbeatRequest,
    LeaseFailure,
    LeaseManager,
    TerminalRequest,
)
from scripts.checkpoints.policy import load_policy
from scripts.checkpoints.s3_gateway import GatewayFailure

ROOT = Path(__file__).resolve().parents[2]
POLICY = load_policy(ROOT / "checkpoints" / "retention-policy.yaml")
NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)
RUN_UUID = "550e8400-e29b-41d4-a716-446655440000"
PREFIX = f"streaming_test/{RUN_UUID}/"
CHECKPOINT_ID = "go-live-streaming-test-v1"
LEASE_KEY = f"_retention/leases/{CHECKPOINT_ID}.json"
TERMINAL_KEY = f"_retention/terminals/{CHECKPOINT_ID}.json"
EPOCH = "11111111-1111-4111-8111-111111111111"


class FakeGateway:
    def __init__(self):
        self.controls: dict[str, tuple[bytes, str]] = {}
        self.calls: list[tuple] = []
        self.read_entered: threading.Event | None = None
        self.release_read: threading.Event | None = None

    def read_control(self, key, *, max_bytes):
        self.calls.append(("read", key, max_bytes))
        if self.read_entered is not None:
            self.read_entered.set()
        if self.release_read is not None:
            assert self.release_read.wait(timeout=2)
        if key not in self.controls:
            raise GatewayFailure("control_missing")
        return self.controls[key]

    def create_control(self, key, body):
        self.calls.append(("create", key, body))
        if key in self.controls:
            raise GatewayFailure("control_write_failed")
        etag = __import__("hashlib").md5(body, usedforsecurity=False).hexdigest()
        self.controls[key] = (body, etag)
        return etag

    def replace_lease(self, key, etag, body):
        self.calls.append(("replace", key, etag, body))
        if key not in self.controls or self.controls[key][1] != etag:
            raise GatewayFailure("control_write_failed")
        next_etag = __import__("hashlib").md5(body, usedforsecurity=False).hexdigest()
        self.controls[key] = (body, next_etag)
        return next_etag


def _acquire() -> AcquireRequest:
    return AcquireRequest(
        checkpoint_id=CHECKPOINT_ID,
        prefix=PREFIX,
        workload="go-live-streaming-test",
        owner_id="acceptance-engineering",
        session_id="issue86-live-001",
    )


def _manager(gateway: FakeGateway, now: datetime = NOW) -> LeaseManager:
    return LeaseManager(
        gateway,
        POLICY,
        now=lambda: now,
        uuid_factory=lambda: EPOCH,
    )


def _active_body(*, heartbeat_at: datetime = NOW, expires_at: datetime | None = None, **changes) -> bytes:
    value = {
        "acquired_at": min(NOW, heartbeat_at).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checkpoint_id": CHECKPOINT_ID,
        "epoch": EPOCH,
        "expires_at": (expires_at or (heartbeat_at + timedelta(seconds=600))).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "heartbeat_at": heartbeat_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "owner_id": "acceptance-engineering",
        "prefix": PREFIX,
        "schema_version": 1,
        "session_id": "issue86-live-001",
        "state": "active",
        "terminal_evidence": None,
        "workload": "go-live-streaming-test",
    }
    value.update(changes)
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def test_acquire_creates_exact_canonical_active_lease_and_readback_etag():
    gateway = FakeGateway()

    result = _manager(gateway).acquire(_acquire())

    assert result.epoch == EPOCH
    assert result.etag == gateway.controls[LEASE_KEY][1]
    assert json.loads(result.body) == {
        "acquired_at": "2026-08-13T12:00:00Z",
        "checkpoint_id": CHECKPOINT_ID,
        "epoch": EPOCH,
        "expires_at": "2026-08-13T12:10:00Z",
        "heartbeat_at": "2026-08-13T12:00:00Z",
        "owner_id": "acceptance-engineering",
        "prefix": PREFIX,
        "schema_version": 1,
        "session_id": "issue86-live-001",
        "state": "active",
        "terminal_evidence": None,
        "workload": "go-live-streaming-test",
    }
    assert gateway.calls[0] == ("read", LEASE_KEY, 65_536)
    assert gateway.calls[1][0:2] == ("create", LEASE_KEY)


@pytest.mark.parametrize(
    ("heartbeat", "expires", "code"),
    [
        (NOW, NOW + timedelta(minutes=10), "lease_active"),
        (NOW - timedelta(minutes=11), NOW - timedelta(minutes=1), "lease_expired_active_uncertain"),
        (NOW + timedelta(minutes=6), NOW + timedelta(minutes=16), "lease_future_clock"),
    ],
)
def test_acquire_refuses_active_expired_uncertain_and_future_leases(heartbeat, expires, code):
    gateway = FakeGateway()
    gateway.controls[LEASE_KEY] = (_active_body(heartbeat_at=heartbeat, expires_at=expires), "a" * 32)

    with pytest.raises(LeaseFailure, match=code):
        _manager(gateway).acquire(_acquire())

    assert not any(call[0] in {"create", "replace"} for call in gateway.calls)


@pytest.mark.parametrize(
    "changes",
    [
        {"checkpoint_id": "foreign-v1"},
        {"prefix": "streaming_test/00000000-0000-0000-0000-000000000000/"},
        {"epoch": "not-a-uuid"},
        {"state": "unknown"},
        {"heartbeat_at_value": "not-a-clock"},
        {"extra": "unknown"},
    ],
)
def test_acquire_refuses_malformed_or_foreign_existing_lease(changes):
    gateway = FakeGateway()
    changes = dict(changes)
    heartbeat_at_value = changes.pop("heartbeat_at_value", None)
    body = _active_body(**changes)
    if heartbeat_at_value is not None:
        value = json.loads(body)
        value["heartbeat_at"] = heartbeat_at_value
        body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    gateway.controls[LEASE_KEY] = (body, "a" * 32)

    with pytest.raises(LeaseFailure, match="lease_malformed|lease_identity_mismatch"):
        _manager(gateway).acquire(_acquire())


def test_heartbeat_requires_exact_epoch_identity_and_sets_exact_sixty_six_hundred_clocks():
    gateway = FakeGateway()
    initial = _active_body()
    gateway.controls[LEASE_KEY] = (initial, "a" * 32)
    manager = _manager(gateway, NOW + timedelta(seconds=60))

    result = manager.heartbeat(HeartbeatRequest(CHECKPOINT_ID, PREFIX, EPOCH))

    value = json.loads(result.body)
    assert value["acquired_at"] == "2026-08-13T12:00:00Z"
    assert value["heartbeat_at"] == "2026-08-13T12:01:00Z"
    assert value["expires_at"] == "2026-08-13T12:11:00Z"
    assert gateway.calls[-1][0:3] == ("replace", LEASE_KEY, "a" * 32)

    for request in (
        HeartbeatRequest(CHECKPOINT_ID, PREFIX, "22222222-2222-4222-8222-222222222222"),
        HeartbeatRequest(CHECKPOINT_ID, "streaming_test/00000000-0000-0000-0000-000000000000/", EPOCH),
    ):
        with pytest.raises(LeaseFailure, match="lease_identity_mismatch"):
            manager.heartbeat(request)


def test_terminal_transition_binds_exact_generation_and_class_state():
    gateway = FakeGateway()
    gateway.controls[LEASE_KEY] = (_active_body(), "a" * 32)
    request = TerminalRequest(
        CHECKPOINT_ID,
        PREFIX,
        EPOCH,
        "stopped",
        {
            "generation": {"run_uuid": RUN_UUID},
            "successful": True,
            "exclusive_run": True,
            "recovery_approved": True,
            "source_available": True,
            "sink_disposition_approved": True,
        },
    )

    result = _manager(gateway, NOW + timedelta(seconds=60)).terminal(request)

    value = json.loads(result.body)
    assert value["state"] == "stopped"
    assert value["terminal_evidence"] == request.evidence
    assert value["heartbeat_at"] == "2026-08-13T12:01:00Z"
    assert value["expires_at"] == "2026-08-13T12:11:00Z"
    terminal = json.loads(gateway.controls[TERMINAL_KEY][0])
    assert terminal == {
        "checkpoint_id": CHECKPOINT_ID,
        "exclusive_run": True,
        "generation": {"run_uuid": RUN_UUID},
        "occurred_at": "2026-08-13T12:01:00Z",
        "prefix": PREFIX,
        "recovery_approved": True,
        "schema_version": 1,
        "sink_disposition_approved": True,
        "source_available": True,
        "state": "stopped",
        "successful": True,
    }

    with pytest.raises(LeaseFailure, match="terminal_state_invalid"):
        _manager(gateway).terminal(replace(request, state="completed"))
    with pytest.raises(LeaseFailure, match="generation_identity_mismatch"):
        _manager(gateway).terminal(
            replace(request, evidence={**request.evidence, "generation": {"run_uuid": "0" * 36}})
        )


def test_terminal_defaults_missing_recovery_facts_to_false_and_rotates_completed_generation_conditionally():
    gateway = FakeGateway()
    gateway.controls[LEASE_KEY] = (_active_body(), "a" * 32)
    manager = _manager(gateway, NOW + timedelta(seconds=60))
    request = TerminalRequest(
        CHECKPOINT_ID,
        PREFIX,
        EPOCH,
        "stopped",
        {"generation": {"run_uuid": RUN_UUID}, "successful": True, "exclusive_run": True},
    )
    manager.terminal(request)
    assert json.loads(gateway.controls[TERMINAL_KEY][0])["recovery_approved"] is False

    next_uuid = "22222222-2222-4222-8222-222222222222"
    next_prefix = f"streaming_test/{next_uuid}/"
    rotated = manager.acquire(
        replace(_acquire(), prefix=next_prefix, session_id="issue86-live-002")
    )

    assert rotated.epoch == EPOCH
    assert json.loads(rotated.body)["prefix"] == next_prefix
    assert gateway.calls[-1][0] == "replace"


def test_terminal_control_failure_is_fail_closed_and_retry_can_finish_missing_evidence():
    class TerminalFailGateway(FakeGateway):
        fail_terminal = True

        def create_control(self, key, body):
            if key == TERMINAL_KEY and self.fail_terminal:
                raise GatewayFailure("control_write_failed")
            return super().create_control(key, body)

    gateway = TerminalFailGateway()
    gateway.controls[LEASE_KEY] = (_active_body(), "a" * 32)
    manager = _manager(gateway, NOW + timedelta(seconds=60))
    request = TerminalRequest(
        CHECKPOINT_ID,
        PREFIX,
        EPOCH,
        "stopped",
        {
            "generation": {"run_uuid": RUN_UUID},
            "successful": True,
            "exclusive_run": True,
            "recovery_approved": True,
            "source_available": True,
            "sink_disposition_approved": True,
        },
    )

    with pytest.raises(LeaseFailure, match="terminal_record_failed"):
        manager.terminal(request)
    assert json.loads(gateway.controls[LEASE_KEY][0])["state"] == "stopped"
    gateway.fail_terminal = False

    result = manager.terminal(request)

    assert result.body == gateway.controls[LEASE_KEY][0]
    assert TERMINAL_KEY in gateway.controls


def test_per_checkpoint_lock_serializes_competing_acquires():
    gateway = FakeGateway()
    gateway.read_entered = threading.Event()
    gateway.release_read = threading.Event()
    manager = _manager(gateway)
    outcomes: list[str] = []

    def acquire():
        try:
            manager.acquire(_acquire())
            outcomes.append("created")
        except LeaseFailure as error:
            outcomes.append(error.code)

    first = threading.Thread(target=acquire)
    second = threading.Thread(target=acquire)
    first.start()
    assert gateway.read_entered.wait(timeout=2)
    second.start()
    gateway.release_read.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert sorted(outcomes) == ["created", "lease_active"]
    assert sum(call[0] == "create" for call in gateway.calls) == 1
