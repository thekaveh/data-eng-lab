# 8.9. Iceberg REST Observability

Issue #90 adds consumer-owned synthetic availability and latency observation for
the fixed Iceberg REST catalog used by this lab. It does not modify Atlas or the
pinned `infra` gitlink. The probe, Prometheus override, alert rules, and Grafana
dashboard all live in this repository and use only the internal
`backend-network`.

This is deliberately a **synthetic** contract. Apache Iceberg REST 1.10.1 does
not expose native server request telemetry in the fixture used by Atlas. Its
client metrics-reporting endpoint is not a server request counter. Consequently,
native request totals and per-route request latency remain unavailable; the
dashboard must not be interpreted as authoritative catalog traffic volume.

## 1. Probe contract

Prometheus scrapes `iceberg-rest-probe:8080/metrics` every 30 seconds with a
five-second scrape timeout. Each scrape makes exactly one `GET /v1/config`
request to `http://iceberg-rest:8181`, with a two-second end-to-end monotonic
deadline spanning DNS resolution, TCP connection, response headers, and body.
DNS uses one single-flight daemon worker, so a stalled resolver cannot create an
unbounded thread pool or block the exporter response past the deadline. The
exporter disables proxy discovery, rejects redirects, accepts only bounded JSON
objects, and closes every response. The response body is limited to 65,536
bytes, 16 levels, and 4,096 composed nodes.

The exporter has no host port, no selectable target, and no catalog credential.
It runs as UID/GID 65532 on a read-only root filesystem with all capabilities
dropped. An unavailable catalog does not prevent the exporter from starting;
that independence is what makes the failure observable.

The closed result values are:

| Result | Meaning |
|---|---|
| `success` | Valid catalog configuration returned in at most one second. |
| `slow` | Valid catalog configuration returned after more than one second. |
| `malformed` | A response used an invalid status or a 200 response violated content-type, size, JSON, or Iceberg field bounds. |
| `timeout` | The bounded catalog request timed out. |
| `http_error` | The catalog returned any status other than 200, including a redirect. |
| `unavailable` | The fixed catalog origin could not be reached. |

`success` and `slow` both count as available. All other outcomes count as
unavailable.

## 2. Metrics and retention

The exporter emits only these four gauge families. Every sample has the fixed
`target="catalog"` label; the result family adds only the six closed values from
section 1.

- `data_eng_lab_iceberg_rest_synthetic_probe_success`
- `data_eng_lab_iceberg_rest_synthetic_probe_duration_seconds`
- `data_eng_lab_iceberg_rest_synthetic_probe_http_status_code`
- `data_eng_lab_iceberg_rest_synthetic_probe_result`

Prometheus retains these samples for 30 days. It does not receive request paths,
catalog response bodies, exception strings, credentials, or URI authorities from
the exporter.

The availability objective is 99.5% over 30 days:

```promql
(sum_over_time(data_eng_lab_iceberg_rest_synthetic_probe_success{job="iceberg-rest-synthetic",target="catalog"}[30d]) or (0 * sum_over_time(up{job="iceberg-rest-synthetic",target="catalog"}[30d]))) / count_over_time(up{job="iceberg-rest-synthetic",target="catalog"}[30d]) >= 0.995
```

The latency objective is a synthetic p95 of at most 1 second over 30 days:

```promql
quantile_over_time(0.95, data_eng_lab_iceberg_rest_synthetic_probe_duration_seconds{job="iceberg-rest-synthetic",target="catalog"}[30d]) <= 1
```

The denominator uses every scheduled scrape, including `up=0`, so exporter
downtime counts as unavailable rather than disappearing from the objective.
These are operational objectives for a periodic synthetic request, not native
service-level request distributions.

## 3. Alerts and dashboard

Prometheus loads three local rules:

| Alert | Condition | Hold | Severity |
|---|---|---:|---|
| `IcebergRestSyntheticExporterMissing` | The fixed scrape target is absent. | 2 minutes | critical |
| `IcebergRestSyntheticUnavailable` | No successful synthetic probe is observed. | 2 minutes | critical |
| `IcebergRestSyntheticSlow` | Recent synthetic p95 exceeds one second. | 10 minutes | warning |

This repository provisions no Alertmanager or external notification route. The
rules appear in Prometheus and Grafana, but the stack does not page an operator.
External routing requires a separate reviewed delivery.

Grafana provisions **Iceberg REST Synthetic Observability** with UID
`data-eng-lab-iceberg-rest-synthetic`. It shows current availability and latency,
30-day availability and p95 latency, the current closed outcome, and active
Iceberg REST alerts. All panels use the fixed Prometheus datasource UID
`Prometheus`; there are no dashboard variables or arbitrary targets.

## 4. Diagnosis

Start from the alert and the current one-hot result series:

```promql
data_eng_lab_iceberg_rest_synthetic_probe_result{job="iceberg-rest-synthetic",target="catalog"} == 1
```

Then follow the matching branch:

1. **Exporter missing:** inspect Prometheus target health for the exact
   `iceberg-rest-synthetic` job and the internal `iceberg-rest-probe` service.
   Do not change the target or add a host port.
2. **`timeout` or `unavailable`:** verify the catalog container state and shared
   `backend-network` membership. Preserve the exporter so it continues recording
   the outage.
3. **`http_error`:** inspect the numeric
   `data_eng_lab_iceberg_rest_synthetic_probe_http_status_code`. Redirects are
   failures and must not be followed.
4. **`malformed`:** compare the bounded `/v1/config` shape with the pinned Iceberg
   REST contract. Do not loosen duplicate-key, JSON, or body bounds to make a bad
   response pass.
5. **`slow`:** compare the current duration and 30-day p95 with catalog CPU,
   memory, backing object-store health, and concurrent Spark/Trino activity.

The exporter intentionally suppresses raw dependency errors. Use Atlas service
logs under the normal operator access boundary; do not add exception text or
response payloads to metric labels.

## 5. Recovery and validation

Recover the underlying catalog or network condition, then require the current
result to return to `success` or `slow`. Confirm the Prometheus target is up and
all three alert expressions are inactive. A valid but slow recovery restores
availability while leaving the latency objective visible.

Repository-only validation does not require the live Atlas stack:

```bash
uv run pytest -q tests/observability
uv run python -m scripts.observability.prometheus_config --check
make docs-check
make docs-wiki
```

The Prometheus configuration is generated from the pinned Atlas source and fails
closed if that source changes. The dashboard and rules are parent-owned read-only
mounts. A pin bump must regenerate and revalidate the configuration before
promotion.

## 6. Native telemetry boundary

Issue #90 is complete with synthetic availability and latency because those are
the requested operator signals. If a later requirement needs authoritative
native request totals, per-route latency, caller identity, or server-side error
counts, first open and resolve an upstream Iceberg instrumentation issue (or an
equivalent reviewed Atlas-native integration). Do not infer those values from
the periodic probe and do not patch the Atlas submodule as part of this delivery.

Related operator material:

- [Atlas Go-Live Findings](atlas-feedback-go-live.md)
- [Atlas Pin-Bump Runbook](atlas-pin-bump-runbook.md)
- [Atlas Expectations](atlas-expectations.md)
