from __future__ import annotations

import traceback

import pytest

from scripts.checkpoints.lease_client import LeaseSession
from scripts.checkpoints.leases import AcquireRequest, LeaseFailure

RUN_UUID = "550e8400-e29b-41d4-a716-446655440000"
PREFIX = f"streaming_test/{RUN_UUID}/"
REQUEST = AcquireRequest(
    checkpoint_id="go-live-streaming-test-v1",
    prefix=PREFIX,
    workload="go-live-streaming-test",
    owner_id="acceptance-engineering",
    session_id="issue86-live-001",
)
EVIDENCE = {"generation": {"run_uuid": RUN_UUID}, "successful": True, "exclusive_run": True}


class FakeApi:
    def __init__(self):
        self.calls = []
        self.heartbeat_failure: BaseException | None = None
        self.terminal_failure: BaseException | None = None

    def acquire(self, request):
        self.calls.append(("acquire", request))
        return type("Result", (), {"epoch": "11111111-1111-4111-8111-111111111111"})()

    def heartbeat(self, request):
        self.calls.append(("heartbeat", request))
        if self.heartbeat_failure is not None:
            raise self.heartbeat_failure

    def terminal(self, request):
        self.calls.append(("terminal", request))
        if self.terminal_failure is not None:
            raise self.terminal_failure


class FakeQuery:
    def __init__(self):
        self.stop_count = 0

    def stop(self):
        self.stop_count += 1


def test_session_orders_acquire_user_heartbeat_final_heartbeat_and_terminal():
    api = FakeApi()
    query = FakeQuery()

    with LeaseSession(api, REQUEST, terminal_state="stopped", terminal_evidence=EVIDENCE) as session:
        session.bind_query(query)
        session.heartbeat()

    assert [call[0] for call in api.calls] == ["acquire", "heartbeat", "heartbeat", "terminal"]
    assert query.stop_count == 0
    assert api.calls[-1][1].evidence == EVIDENCE


def test_start_failure_terminalizes_and_cleanup_cannot_mask_primary():
    api = FakeApi()
    api.terminal_failure = RuntimeError("credential=cleanup-secret")
    primary = RuntimeError("query-start-failed")

    with pytest.raises(RuntimeError, match="query-start-failed") as failure:
        with LeaseSession(api, REQUEST, terminal_state="stopped", terminal_evidence=EVIDENCE):
            raise primary

    assert failure.value is primary
    assert [call[0] for call in api.calls] == ["acquire", "heartbeat", "terminal"]
    rendered = "".join(traceback.format_exception(failure.value))
    assert "cleanup-secret" not in rendered


def test_heartbeat_loss_stops_query_and_preserves_bounded_primary():
    api = FakeApi()
    api.heartbeat_failure = LeaseFailure("lease_ownership_lost")
    query = FakeQuery()

    with pytest.raises(LeaseFailure, match="lease_ownership_lost"):
        with LeaseSession(api, REQUEST, terminal_state="stopped", terminal_evidence=EVIDENCE) as session:
            session.bind_query(query)
            session.heartbeat()

    assert query.stop_count == 1
    assert [call[0] for call in api.calls] == ["acquire", "heartbeat", "heartbeat", "terminal"]


@pytest.mark.parametrize("control", [KeyboardInterrupt(), SystemExit(7)])
def test_control_flow_primary_is_preserved_through_terminal_cleanup(control):
    api = FakeApi()
    api.terminal_failure = RuntimeError("cleanup must not replace control flow")

    with pytest.raises(type(control)) as failure:
        with LeaseSession(api, REQUEST, terminal_state="stopped", terminal_evidence=EVIDENCE):
            raise control

    assert failure.value is control
