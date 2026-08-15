from __future__ import annotations

import pytest

from scripts.checkpoints.metrics import MetricsFailure, render_metrics


def test_metrics_render_exact_closed_names_labels_and_order():
    body = render_metrics(
        {
            "checkpoint_retention_objects": {("go-live-streaming-test-v1",): 2},
            "checkpoint_retention_bytes": {("go-live-streaming-test-v1",): 3},
            "checkpoint_retention_plans_total": {("eligible",): 1},
            "checkpoint_retention_refusals_total": {("lease_active",): 4},
            "checkpoint_retention_request_failures_total": {("backend_failure",): 1},
        }
    )

    assert body == (
        b'checkpoint_retention_bytes{checkpoint_id="go-live-streaming-test-v1"} 3\n'
        b'checkpoint_retention_objects{checkpoint_id="go-live-streaming-test-v1"} 2\n'
        b'checkpoint_retention_plans_total{decision="eligible"} 1\n'
        b'checkpoint_retention_refusals_total{refusal_code="lease_active"} 4\n'
        b'checkpoint_retention_request_failures_total{outcome="backend_failure"} 1\n'
    )


@pytest.mark.parametrize(
    "snapshot",
    [
        {"dynamic_metric": {(): 1}},
        {"checkpoint_retention_objects": {("unknown-checkpoint",): 1}},
        {"checkpoint_retention_plans_total": {("prefix/high-cardinality",): 1}},
        {"checkpoint_retention_bytes": {("go-live-streaming-test-v1",): float("nan")}},
        {"checkpoint_retention_bytes": {("go-live-streaming-test-v1",): -1}},
    ],
)
def test_metrics_reject_dynamic_labels_nonfinite_negative_and_unknown_values(snapshot):
    with pytest.raises(MetricsFailure):
        render_metrics(snapshot)


def test_metrics_body_bound_is_enforced_before_return():
    snapshot = {
        "checkpoint_retention_objects": {
            (checkpoint_id,): index
            for index, checkpoint_id in enumerate(
                (
                    "streaming-events-v1",
                    "streaming-event-windows-v1",
                    "streaming-online-retail-cdc-v1",
                    "streaming-gh-archive-file-v1",
                    "go-live-streaming-test-v1",
                )
            )
        }
    }
    with pytest.raises(MetricsFailure, match="metrics_body_bound"):
        render_metrics(snapshot, max_bytes=64)
