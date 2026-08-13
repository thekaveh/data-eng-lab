"""Low-cardinality Prometheus metrics for checkpoint retention."""

from __future__ import annotations

import math
from typing import Mapping


class MetricsFailure(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


_CHECKPOINT_IDS = {
    "streaming-events-v1",
    "streaming-event-windows-v1",
    "streaming-online-retail-cdc-v1",
    "streaming-gh-archive-file-v1",
    "go-live-streaming-test-v1",
}
_DECISIONS = {"eligible", "refused", "not_ready", "partial", "completed"}
_REFUSALS = {
    "clock_overflow",
    "concrete_prefix_required",
    "control_read_failed",
    "exclusive_run_required",
    "future_clock",
    "generation_identity_mismatch",
    "invalid_fact_type",
    "invalid_lease_terminal_state",
    "invalid_terminal_state",
    "invalid_utc_timestamp",
    "inventory_invalid",
    "inventory_empty",
    "inventory_failed",
    "lease_active",
    "lease_clock_invalid",
    "lease_missing",
    "lease_conflicting",
    "lease_etag_invalid",
    "lease_expired_active_uncertain",
    "lease_identity_mismatch",
    "lease_malformed",
    "lease_state_invalid",
    "lease_terminal_clock_conflict",
    "object_after_terminal",
    "partial_retry_broadened",
    "recovery_not_approved",
    "registry_active_durable",
    "registry_retirement_review_invalid",
    "registry_retirement_review_missing",
    "retention_quarantine",
    "retirement_clock_missing",
    "retirement_review_mismatch",
    "retirement_review_missing",
    "sink_disposition_not_approved",
    "source_unavailable",
    "successful_run_required",
    "terminal_missing",
    "terminal_identity_mismatch",
    "terminal_malformed",
    "inventory_changed",
    "policy_drift",
    "revalidation_mismatch",
}
_OUTCOMES = {"backend_failure", "invalid_request", "unauthorized", "timeout", "capability_failed"}
_METRICS = {
    "checkpoint_retention_objects": ("checkpoint_id", _CHECKPOINT_IDS),
    "checkpoint_retention_bytes": ("checkpoint_id", _CHECKPOINT_IDS),
    "checkpoint_retention_eligible_bytes": ("checkpoint_id", _CHECKPOINT_IDS),
    "checkpoint_retention_lease_heartbeat_age_seconds": ("checkpoint_id", _CHECKPOINT_IDS),
    "checkpoint_retention_last_success_unixtime": ("checkpoint_id", _CHECKPOINT_IDS),
    "checkpoint_retention_plans_total": ("decision", _DECISIONS),
    "checkpoint_retention_refusals_total": ("refusal_code", _REFUSALS),
    "checkpoint_retention_prepared_total": ("outcome", _DECISIONS),
    "checkpoint_retention_deleted_objects_total": ("outcome", _DECISIONS),
    "checkpoint_retention_deleted_bytes_total": ("outcome", _DECISIONS),
    "checkpoint_retention_partial_total": ("outcome", _DECISIONS),
    "checkpoint_retention_request_failures_total": ("outcome", _OUTCOMES),
}


def render_metrics(snapshot: Mapping[str, Mapping[tuple[str, ...], int | float]], *, max_bytes: int = 65_536) -> bytes:
    if not isinstance(snapshot, Mapping) or type(max_bytes) is not int or max_bytes < 1:
        raise MetricsFailure("metrics_invalid")
    lines: list[str] = []
    for metric_name in sorted(snapshot):
        contract = _METRICS.get(metric_name)
        values = snapshot[metric_name]
        if contract is None or not isinstance(values, Mapping):
            raise MetricsFailure("metric_unknown")
        label_name, allowed_values = contract
        for labels in sorted(values):
            value = values[labels]
            if (
                type(labels) is not tuple
                or len(labels) != 1
                or labels[0] not in allowed_values
                or type(value) not in (int, float)
                or not math.isfinite(value)
                or value < 0
            ):
                raise MetricsFailure("metric_value_invalid")
            rendered = str(value) if type(value) is int else format(value, ".17g")
            lines.append(f'{metric_name}{{{label_name}="{labels[0]}"}} {rendered}\n')
    body = "".join(lines).encode("ascii")
    if len(body) > max_bytes:
        raise MetricsFailure("metrics_body_bound")
    return body
