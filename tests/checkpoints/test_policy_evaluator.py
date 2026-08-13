from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.checkpoints import policy as api

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "checkpoints" / "retention-policy.yaml"
NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)
GENERATION_PREFIX = "gh_events_file/tiny/" + "a" * 32 + "/" + "b" * 64 + "/"


@pytest.fixture
def policy():
    return api.load_policy(POLICY_PATH)


def _retired(policy, checkpoint_id: str = "streaming-events-v1"):
    entries = dict(policy.entries)
    entries[checkpoint_id] = replace(entries[checkpoint_id], lifecycle="retired")
    return replace(policy, entries=entries)


def _lease(
    *,
    checkpoint_id: str = "streaming-events-v1",
    prefix: str = "events/",
    state: str = "retired",
    age: timedelta = timedelta(days=31),
    conflicting: bool = False,
    malformed: bool = False,
):
    heartbeat = NOW - age
    return api.LeaseFacts(
        checkpoint_id=checkpoint_id,
        prefix=prefix,
        state=state,
        acquired_at=heartbeat - timedelta(hours=1),
        heartbeat_at=heartbeat,
        expires_at=heartbeat + timedelta(minutes=10),
        etag="0123456789abcdef",
        conflicting=conflicting,
        malformed=malformed,
    )


def _terminal(
    *,
    state: str = "retired",
    age: timedelta = timedelta(days=31),
    recovery_approved: bool = True,
    source_available: bool = True,
    sink_disposition_approved: bool = True,
    retirement_review: str | None = "issue-85-reviewed-transition",
    generation: dict[str, str] | None = None,
    exclusive_run: bool = False,
    successful: bool = False,
):
    return api.TerminalFacts(
        state=state,
        occurred_at=NOW - age,
        recovery_approved=recovery_approved,
        source_available=source_available,
        sink_disposition_approved=sink_disposition_approved,
        retirement_review=retirement_review,
        generation=generation or {},
        exclusive_run=exclusive_run,
        successful=successful,
    )


def _inventory(
    *,
    age: timedelta = timedelta(days=31),
    changed: bool = False,
    partial_retry_confined: bool = True,
):
    return api.InventorySummary(
        object_count=17,
        total_bytes=4096,
        newest_last_modified=NOW - age,
        inventory_sha256="c" * 64,
        changed_since_plan=changed,
        partial_retry_confined=partial_retry_confined,
    )


def _facts(
    *,
    prefix: str = "events/",
    lease=None,
    terminal=None,
    inventory=None,
    evaluated_at: datetime = NOW,
):
    return api.EvaluationInput(
        prefix=prefix,
        evaluated_at=evaluated_at,
        lease=_lease(prefix=prefix) if lease is None else lease,
        terminal=_terminal() if terminal is None else terminal,
        inventory=_inventory() if inventory is None else inventory,
    )


def test_active_durable_registry_never_becomes_age_eligible(policy):
    decision = api.evaluate_retention(policy, _facts())

    assert decision.eligible is False
    assert decision.refusal_codes == ("registry_active_durable",)


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("missing", "lease_missing"),
        ("active", "lease_active"),
        ("expired_active", "lease_expired_active_uncertain"),
        ("conflicting", "lease_conflicting"),
        ("malformed", "lease_malformed"),
        ("foreign_id", "lease_identity_mismatch"),
        ("foreign_prefix", "lease_identity_mismatch"),
    ],
)
def test_lease_states_fail_closed(policy, case: str, code: str):
    leases = {
        "active": lambda: _lease(state="active", age=timedelta(minutes=1)),
        "expired_active": lambda: _lease(state="active", age=timedelta(minutes=11)),
        "conflicting": lambda: _lease(conflicting=True),
        "malformed": lambda: _lease(malformed=True),
        "foreign_id": lambda: _lease(checkpoint_id="foreign-v1"),
        "foreign_prefix": lambda: _lease(prefix="event_windows/"),
    }
    facts = replace(_facts(), lease=None) if case == "missing" else _facts(lease=leases[case]())
    decision = api.evaluate_retention(_retired(policy), facts)

    assert decision.eligible is False
    assert code in decision.refusal_codes


def test_retired_durable_is_eligible_at_full_thirty_day_quarantine(policy):
    decision = api.evaluate_retention(
        _retired(policy),
        _facts(
            lease=_lease(age=timedelta(days=30)),
            terminal=_terminal(age=timedelta(days=30)),
            inventory=_inventory(age=timedelta(days=30)),
        ),
    )

    assert decision.eligible is True
    assert decision.refusal_codes == ()
    assert decision.retention_anchor == NOW - timedelta(days=30)
    assert decision.eligible_after == NOW


def test_retired_durable_anchor_uses_newest_valid_clock(policy):
    decision = api.evaluate_retention(
        _retired(policy),
        _facts(
            lease=_lease(age=timedelta(days=29, hours=23)),
            terminal=_terminal(age=timedelta(days=34)),
            inventory=_inventory(age=timedelta(days=34)),
        ),
    )

    assert decision.eligible is False
    assert decision.retention_anchor == NOW - timedelta(days=29, hours=23)
    assert decision.eligible_after == NOW + timedelta(hours=1)
    assert decision.refusal_codes == ("retention_quarantine",)


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("recovery", "recovery_not_approved"),
        ("source", "source_unavailable"),
        ("sink", "sink_disposition_not_approved"),
        ("review", "retirement_review_missing"),
        ("state", "invalid_terminal_state"),
    ],
)
def test_durable_recovery_evidence_is_mandatory(policy, case, code: str):
    terminals = {
        "recovery": lambda: replace(_terminal(), recovery_approved=False),
        "source": lambda: replace(_terminal(), source_available=False),
        "sink": lambda: replace(_terminal(), sink_disposition_approved=False),
        "review": lambda: replace(_terminal(), retirement_review=None),
        "state": lambda: replace(_terminal(), state="completed"),
    }
    terminal = terminals[case]()
    decision = api.evaluate_retention(_retired(policy), _facts(terminal=terminal))

    assert decision.eligible is False
    assert code in decision.refusal_codes


def test_exact_generation_leaf_is_eligible_after_fourteen_days(policy):
    generation = {
        "scale": "tiny",
        "publication_id": "a" * 32,
        "manifest_sha256": "b" * 64,
    }
    decision = api.evaluate_retention(
        policy,
        _facts(
            prefix=GENERATION_PREFIX,
            lease=_lease(
                checkpoint_id="streaming-gh-archive-file-v1",
                prefix=GENERATION_PREFIX,
                state="completed",
                age=timedelta(days=14),
            ),
            terminal=_terminal(
                state="completed",
                age=timedelta(days=14),
                retirement_review=None,
                generation=generation,
            ),
            inventory=_inventory(age=timedelta(days=14)),
        ),
    )

    assert decision.eligible is True
    assert decision.retention_anchor == NOW - timedelta(days=14)


def test_generation_identity_mismatch_and_root_fail_closed(policy):
    generation = {
        "scale": "tiny",
        "publication_id": "d" * 32,
        "manifest_sha256": "b" * 64,
    }
    facts = _facts(
        prefix=GENERATION_PREFIX,
        lease=_lease(
            checkpoint_id="streaming-gh-archive-file-v1",
            prefix=GENERATION_PREFIX,
            state="completed",
            age=timedelta(days=15),
        ),
        terminal=_terminal(
            state="completed",
            age=timedelta(days=15),
            retirement_review=None,
            generation=generation,
        ),
        inventory=_inventory(age=timedelta(days=15)),
    )

    assert api.evaluate_retention(policy, facts).refusal_codes == ("generation_identity_mismatch",)
    with pytest.raises(api.PolicyError, match="unknown_prefix"):
        api.evaluate_retention(policy, replace(facts, prefix="gh_events_file/"))


def test_disposable_scratch_requires_successful_exclusive_stopped_run(policy):
    eligible = _facts(
        prefix="streaming_test/",
        lease=_lease(
            checkpoint_id="go-live-streaming-test-v1",
            prefix="streaming_test/",
            state="stopped",
            age=timedelta(days=1),
        ),
        terminal=_terminal(
            state="stopped",
            age=timedelta(days=1),
            retirement_review=None,
            exclusive_run=True,
            successful=True,
        ),
        inventory=_inventory(age=timedelta(days=1)),
    )

    assert api.evaluate_retention(policy, eligible).eligible is True
    assert (
        "exclusive_run_required"
        in api.evaluate_retention(
            policy, replace(eligible, terminal=replace(eligible.terminal, exclusive_run=False))
        ).refusal_codes
    )
    assert (
        "successful_run_required"
        in api.evaluate_retention(
            policy, replace(eligible, terminal=replace(eligible.terminal, successful=False))
        ).refusal_codes
    )


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("naive_now", "invalid_utc_timestamp"),
        ("future_heartbeat", "future_clock"),
        ("object_after_terminal", "object_after_terminal"),
        ("changed", "inventory_changed"),
        ("partial_broadened", "partial_retry_broadened"),
    ],
)
def test_clock_inventory_and_partial_state_ambiguity_refuses(policy, case, code: str):
    cases = {
        "naive_now": lambda: _facts(evaluated_at=NOW.replace(tzinfo=None)),
        "future_heartbeat": lambda: _facts(lease=replace(_lease(), heartbeat_at=NOW + timedelta(minutes=6))),
        "object_after_terminal": lambda: _facts(
            inventory=replace(_inventory(), newest_last_modified=NOW - timedelta(days=30)),
            terminal=_terminal(age=timedelta(days=31)),
        ),
        "changed": lambda: _facts(inventory=_inventory(changed=True)),
        "partial_broadened": lambda: _facts(inventory=_inventory(partial_retry_confined=False)),
    }
    facts = cases[case]()
    decision = api.evaluate_retention(_retired(policy), facts)

    assert decision.eligible is False
    assert code in decision.refusal_codes


def test_canonical_plan_is_deterministic_compact_and_digest_bound(policy):
    facts = _facts()
    first = api.evaluate_retention(_retired(policy), facts)
    second = api.evaluate_retention(_retired(policy), facts)

    assert first.plan_json == second.plan_json
    assert first.plan_sha256 == second.plan_sha256
    assert first.plan_json == json.dumps(
        json.loads(first.plan_json), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    assert first.plan_sha256 == hashlib.sha256(first.plan_json.encode()).hexdigest()
    assert len(first.plan_json.encode()) <= 65_536
    assert json.loads(first.plan_json) == {
        "checkpoint_id": "streaming-events-v1",
        "decision": "eligible",
        "eligible_after": "2026-08-12T12:00:00Z",
        "evaluated_at": "2026-08-13T12:00:00Z",
        "inventory": {
            "newest_last_modified": "2026-07-13T12:00:00Z",
            "object_count": 17,
            "sha256": "c" * 64,
            "total_bytes": 4096,
        },
        "policy_sha256": first.policy_sha256,
        "prefix": "events/",
        "refusal_codes": [],
        "retention_anchor": "2026-07-13T12:00:00Z",
    }


def test_evaluator_does_not_mutate_caller_facts(policy):
    facts = _facts()
    original = repr(facts)

    api.evaluate_retention(_retired(policy), facts)

    assert repr(facts) == original
