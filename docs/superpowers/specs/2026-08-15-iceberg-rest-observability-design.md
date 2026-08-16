# Iceberg REST synthetic observability design

**Issue:** #90  
**Date:** 2026-08-15  
**Status:** Approved by the controlling autonomous goal directive

## 1. Purpose and boundary

This design adds consumer-owned availability and latency monitoring for the
pinned Atlas Iceberg REST catalog without changing Atlas or claiming visibility
the catalog does not provide. The result is a bounded synthetic probe, a
Prometheus scrape and alert contract, a provisioned Grafana dashboard, and an
operator runbook projected to the repository, site, and wiki.

The synthetic probe answers one question: from the same Docker network as the
data-engineering clients, did a bounded `GET /v1/config` return a valid Iceberg
catalog configuration, and how long did that request take? It does not observe
real Spark, Trino, PyIceberg, Airflow, Jupyter, or Zeppelin traffic. It therefore
does not expose or estimate authoritative request totals, per-route latency,
client identities, catalog mutations, or query counts.

The work does not start Atlas, run live acceptance, change the Atlas submodule,
advance its gitlink, alter the dataset registry, create a schedule, or add an
external notification service.

## 2. Platform evidence

Atlas is pinned at gitlink `c6cf73d7168db1a7840fc45c9ed3e385071996d8`.
Its `iceberg-rest` service builds from `apache/iceberg-rest-fixture:1.10.1`,
publishes port 8181, and health-checks `GET /v1/config`. The pinned Atlas
Prometheus configuration has no Iceberg REST scrape job, and the pinned Grafana
configuration has no Iceberg REST dashboard or alert contract.

Apache Iceberg tag `apache-iceberg-1.10.1` constructs the fixture server in
`RESTCatalogServer`: one `RESTCatalogServlet` is registered on `/*`, with no
metrics servlet, request meter, Prometheus endpoint, or supported metrics
configuration. The REST catalog specification defines catalog API behavior; its
`POST .../metrics` endpoint receives client table scan and commit reports and is
not a service-request telemetry endpoint.

Consequently, no native request metric exists for the consumer to scrape. Issue
#90 requires availability and latency, which the synthetic contract can deliver
truthfully. Authoritative request totals are not required to close #90, so no
upstream Atlas issue is filed. If a later requirement needs total traffic or
per-route server latency, that work must first add native server instrumentation
upstream and then consume it after merge.

## 3. Considered approaches

### 3.1. Selected: fixed-target Python synthetic exporter

A small parent-owned Python service probes the one configured Iceberg REST
origin and exposes the result in Prometheus text format. It validates response
status, content type, body bound, duplicate-free JSON, structure, timeout, and
redirect behavior. Its metrics use a fixed target label and a closed result
enumeration.

This is the smallest approach that distinguishes healthy, slow, malformed,
timeout, HTTP-error, and unavailable outcomes while remaining fully testable
with bounded in-process HTTP servers.

### 3.2. Rejected: generic blackbox exporter

The Prometheus blackbox exporter provides the standard multi-target pattern and
basic HTTP assertions, but it cannot strictly decode the Iceberg catalog JSON
contract or reject duplicate keys and malformed bounded structures. Passing an
arbitrary target also creates an unnecessary SSRF/configuration surface. A fixed
single-purpose exporter is smaller at the security boundary.

### 3.3. Rejected: native Atlas instrumentation

Native request counters and histograms would be authoritative, but they require
an Atlas/Apache fixture change, new dependencies, and an Atlas gitlink advance.
Those actions are outside #90 and unnecessary for its availability/latency
criteria. The consumer must not patch the submodule or infer native traffic from
synthetic samples.

## 4. Probe service contract

The production package lives under `scripts/observability/` and uses only the
Python standard library. `iceberg_rest_probe.py` contains three separable units:

1. strict configuration and catalog-response parsing;
2. one bounded HTTP probe returning an immutable result; and
3. a single-threaded internal HTTP server serving `/healthz` and `/metrics`.

The service accepts only these environment values:

- `ICEBERG_REST_PROBE_ORIGIN`, default and production value
  `http://iceberg-rest:8181`;
- `ICEBERG_REST_PROBE_TIMEOUT_SECONDS`, fixed to `2.0` in Compose; and
- `ICEBERG_REST_PROBE_MAX_BODY_BYTES`, fixed to `65536` in Compose.

The origin parser requires plain internal HTTP, host `iceberg-rest`, port 8181,
no userinfo, path, query, or fragment. The probe always requests `/v1/config`;
no request parameter can select another target. It disables environment proxies
and automatic redirects, uses a monotonic deadline, closes every response, reads
at most 65,537 bytes, and never logs the response body, origin, headers, or raw
dependency exception.

Valid success requires HTTP 200, an `application/json` media type, UTF-8 JSON,
no duplicate object keys, no non-finite number, a top-level object, and bounded
depth/node counts. When present, `defaults` and `overrides` must be string maps
and `endpoints` must be a list of strings. Unknown top-level fields remain
forward-compatible but are still covered by the global size/depth/node limits.

`GET /healthz` reports only exporter readiness. `GET /metrics` executes one
probe and returns a body below 8 KiB. Other methods, paths, queries, or duplicate
headers receive a small fixed error. The server is single-threaded, has no host
port, runs as UID/GID 65532, uses a read-only root filesystem, drops all Linux
capabilities, sets `no-new-privileges`, and has explicit CPU, memory, and PID
limits. A failed target probe still returns HTTP 200 from `/metrics`, because the
failure is the metric sample; exporter failure is represented separately by
Prometheus `up`.

## 5. Metrics and ownership

The exporter owns exactly these synthetic series:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `data_eng_lab_iceberg_rest_synthetic_probe_success` | gauge | `target="catalog"` | 1 only for a valid catalog response |
| `data_eng_lab_iceberg_rest_synthetic_probe_duration_seconds` | gauge | `target="catalog"` | end-to-end synthetic request duration |
| `data_eng_lab_iceberg_rest_synthetic_probe_http_status_code` | gauge | `target="catalog"` | bounded HTTP status, or 0 without a response |
| `data_eng_lab_iceberg_rest_synthetic_probe_result` | gauge | `target="catalog", result=<closed value>` | one-hot outcome classification |

The closed result values are `success`, `slow`, `malformed`, `timeout`,
`http_error`, and `unavailable`. `slow` means an otherwise valid response took
more than the one-second SLO threshold. Only one result series has value 1 per
scrape; the other result values are emitted as 0 so stale labels cannot be
mistaken for the current outcome.

Labels never contain a URI, path, status text, exception, client, namespace,
table, request ID, credential, or free-form value. Prometheus adds
`job="iceberg-rest-synthetic"`; no metric is described as a native Iceberg
request count.

## 6. Prometheus, SLO, and alerts

The consumer overlay enables Atlas Prometheus and Grafana and mounts a generated
consumer Prometheus configuration. That file must be byte-equivalent to the
pinned Atlas configuration plus one `iceberg-rest-synthetic` scrape job. A
contract test regenerates the expected merge from the pinned source so an Atlas
pin change cannot silently drop or alter upstream jobs.

Prometheus scrapes the exporter every 30 seconds with a five-second scrape
timeout. The consumer sets `PROMETHEUS_RETENTION_DAYS=30`, matching the rolling
SLO window.

The service objectives are:

- rolling 30-day synthetic availability at least 99.5%; and
- rolling 30-day synthetic p95 latency at most 1.0 second.

The rule file provides:

- `IcebergRestSyntheticExporterMissing`: Prometheus cannot scrape the exporter
  for two minutes;
- `IcebergRestSyntheticUnavailable`: valid catalog probes remain unsuccessful
  for two minutes; and
- `IcebergRestSyntheticSlow`: 10-minute synthetic p95 exceeds one second for
  ten minutes.

Rules are evaluated and visible in Prometheus and Grafana. No Alertmanager or
external contact point exists, so #90 does not claim paging or notification
delivery. The runbook defines severity, diagnosis, recovery, and escalation for
each firing state.

## 7. Grafana and documentation

Grafana receives one provisioned dashboard file with a stable UID. Panels show
current synthetic state, current latency, rolling 30-day availability, rolling
30-day p95 latency, current outcome, and active alert state. Queries use only the
closed metrics and fixed labels above.

`docs/iceberg-rest-observability.md` is the canonical operator contract and a new
Atlas Operations manifest leaf. The documentation records metric ownership,
synthetic/native limitations, 30-day retention, SLO formulas, alert semantics,
dashboard queries, troubleshooting order, and the exact condition that would
justify an upstream native-metrics issue. The site and wiki are generated from
that source; they are not edited separately.

The stale feedback bullet that asks for unspecified Iceberg query counts is
reconciled to the delivered synthetic contract and explicitly leaves native
totals unavailable.

## 8. Testing and verification

Strict TDD covers:

- accepted healthy response and exact metric text;
- valid but slower-than-one-second response;
- duplicate-key, non-object, wrong-content-type, invalid UTF-8, oversized,
  over-deep, over-node, and wrong-field-shape responses;
- timeout, connection refusal, HTTP error, and redirect refusal;
- fixed-origin parsing, proxy disablement, response closure, monotonic timing,
  bounded output, no raw exception/URI leakage, and control-flow preservation;
- single-threaded server routes and health/metrics behavior;
- Prometheus merge equivalence, scrape/rule syntax and closed queries;
- Grafana dashboard structure, UID, datasource, panels, queries, and labels;
- Compose isolation, resource bounds, no host port, read-only mounts, and
  Prometheus/Grafana enablement; and
- canonical documentation content and three-surface generation.

Verification uses focused tests, the complete offline test suite, Ruff lint and
format checks, Compose config validation with bounded placeholders, Prometheus
configuration/rule validation in a disposable `prom/prometheus:v2.55.1`
container when locally available, strict documentation/site/wiki gates,
`make verify`, diff checks, protected hashes, the unchanged Atlas gitlink, zero
task-owned containers, and preserved volumes. The full Atlas stack is never
started.

## 9. Rollback and future work

All runtime changes are parent-owned Compose overlay additions and read-only
mounts. Rollback removes the probe service, the three Prometheus/Grafana mounts,
and the two source toggles; it does not migrate or delete application data. The
Prometheus data volume remains preserved.

A future native-metrics effort begins only when authoritative total traffic or
per-route server latency becomes a stated requirement. It must file or reuse an
upstream Atlas issue, add instrumentation in Atlas rather than this consumer,
prove label cardinality and compatibility, merge upstream, advance the pinned
gitlink through its own lifecycle, and keep the synthetic probe as an independent
outside-in signal.
