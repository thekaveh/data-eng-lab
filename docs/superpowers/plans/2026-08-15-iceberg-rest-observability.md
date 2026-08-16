# Iceberg REST synthetic observability implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver consumer-owned Iceberg REST synthetic availability and latency metrics, alerts, dashboard, and three-surface operator documentation for issue #90 without changing or starting Atlas.

**Architecture:** A fixed-target, standard-library Python exporter performs one bounded `GET /v1/config` per Prometheus scrape and emits closed low-cardinality synthetic metrics. The consumer overlay adds the hardened exporter, mounts a generated Prometheus configuration plus alert rules, provisions one Grafana dashboard, and enables 30-day Prometheus/Grafana operation. Canonical documentation states that native request totals remain unavailable.

**Tech Stack:** Python 3.11 standard library, pytest, PyYAML, Docker Compose, Prometheus 2.55.1 configuration/rules, Grafana 11.4 dashboard JSON, MkDocs/wiki projections.

## Global constraints

- Base commit is `3937eece5dcc9506bfc5ad27fe3b282472f327a4`; branch is `codex/90-iceberg-rest-observability`.
- Atlas gitlink remains `c6cf73d7168db1a7840fc45c9ed3e385071996d8`; do not edit `infra/` or advance the gitlink.
- Do not start Atlas, run `RUN_INFRA`, run live acceptance, create a schedule, or touch persistent project volumes.
- Do not modify `uv.lock`, `pyproject.toml`, `datasets/registry.yaml`, the protected untracked Atlas plan, or `graphify-out/`.
- `ICEBERG_REST_PROBE_ORIGIN` is exactly `http://iceberg-rest:8181`; runtime callers cannot select a target.
- The target body is at most 65,536 bytes, JSON depth at most 16, composed nodes at most 4,096, and probe timeout 2 seconds.
- Metrics contain only `target="catalog"` and one closed result label from `success|slow|malformed|timeout|http_error|unavailable`.
- Synthetic availability SLO is 99.5% over 30 days; synthetic p95 latency SLO is at most 1 second over 30 days.
- Prometheus scrape interval is 30 seconds, scrape timeout is 5 seconds, and retention is 30 days.
- Prometheus/Grafana artifacts may claim only synthetic observations. Native request totals and per-route request latency remain unavailable.
- Every production behavior is introduced by a focused failing test that is observed RED before the minimal implementation.
- Hand-authored edits use `apply_patch`; generated configuration is written only by its repository generator.

---

### Task 1: Implement strict catalog response decoding and one bounded probe

**Files:**
- Create: `scripts/observability/__init__.py`
- Create: `scripts/observability/iceberg_rest_probe.py`
- Create: `tests/observability/__init__.py`
- Create: `tests/observability/test_iceberg_rest_probe.py`

**Interfaces:**
- Produces: `ProbeConfig(origin: str, timeout_seconds: float, max_body_bytes: int, slow_seconds: float = 1.0)`.
- Produces: `ProbeResult(success: bool, duration_seconds: float, http_status_code: int, result: str)`.
- Produces: `decode_catalog_config(body: bytes) -> Mapping[str, object]`.
- Produces: `probe_catalog(config, *, opener=None, monotonic=time.monotonic) -> ProbeResult`.

- [ ] **Step 1: Write strict decoder RED tests**

Add tests for a valid object, duplicate keys, non-object JSON, invalid UTF-8,
non-finite constants, over-65,536-byte bodies, depth greater than 16, more than
4,096 nodes, non-string `defaults`/`overrides`, and non-string `endpoints`.

```python
def test_duplicate_catalog_keys_are_rejected():
    with pytest.raises(ProbeFailure, match="catalog_response_malformed"):
        decode_catalog_config(b'{"defaults":{},"defaults":{}}')
```

- [ ] **Step 2: Run the decoder tests and verify RED**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_probe.py`

Expected: collection fails because `scripts.observability.iceberg_rest_probe` does not exist.

- [ ] **Step 3: Implement the minimal decoder**

Use `json.loads` with `object_pairs_hook` that rejects duplicate keys and
`parse_constant` that rejects non-finite values. Walk the decoded value
iteratively to enforce exact depth and node ceilings, then validate the optional
Iceberg fields without rejecting bounded unknown top-level extensions.

- [ ] **Step 4: Write fixed-origin and transport RED tests**

Use bounded in-process `HTTPServer` fixtures for healthy, delayed-valid,
malformed, wrong content type, oversized, HTTP 500, redirect, timeout, and
connection-refused responses. Assert `ProxyHandler({})`, `/v1/config`, one closed
response, HTTP status 0 without a response, sanitized categories, monotonic
duration, and `KeyboardInterrupt`/`SystemExit` preservation.

```python
result = probe_catalog(ProbeConfig(server.origin, 0.2, 65_536, slow_seconds=0.01))
assert result == ProbeResult(True, result.duration_seconds, 200, "slow")
```

- [ ] **Step 5: Run transport tests and verify RED**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_probe.py -k 'probe or origin'`

Expected: decoder tests pass; transport tests fail because `probe_catalog` and strict origin parsing are absent.

- [ ] **Step 6: Implement the minimal probe**

Build an opener with `ProxyHandler({})` and a redirect-rejecting handler. Request
only `origin + "/v1/config"`, pass the two-second timeout, read one byte beyond
the configured bound, close every response, catch only ordinary transport
exceptions, and return a closed result category without chaining raw errors.

- [ ] **Step 7: Verify Task 1 GREEN and commit**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_probe.py`

Expected: all decoder and probe cases pass.

Commit: `feat(observability): add bounded Iceberg REST probe (#90)`

---

### Task 2: Expose bounded metrics and package the hardened exporter

**Files:**
- Modify: `scripts/observability/iceberg_rest_probe.py`
- Create: `observability/iceberg-rest-probe.Dockerfile`
- Create: `tests/observability/test_iceberg_rest_probe_service.py`
- Create: `tests/observability/test_iceberg_rest_probe_deployment.py`

**Interfaces:**
- Produces: `render_metrics(result: ProbeResult) -> bytes` below 8 KiB.
- Produces: `build_server(config, *, host="0.0.0.0", port=8080) -> HTTPServer`.
- Produces: module entry point `python -m scripts.observability.iceberg_rest_probe`.

- [ ] **Step 1: Write exact metrics RED tests**

Require four HELP/TYPE families, fixed `target="catalog"`, all six result series,
exact one-hot classification, success=1 for `success` and `slow`, no origin or
exception text, finite non-negative duration, status in 0..599, stable ordering,
and an 8 KiB body ceiling.

```python
body = render_metrics(ProbeResult(False, 0.125, 0, "unavailable"))
assert b'probe_result{target="catalog",result="unavailable"} 1\n' in body
assert b"iceberg-rest:8181" not in body
```

- [ ] **Step 2: Verify metrics RED**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_probe_service.py -k metrics`

Expected: import or attribute failure for `render_metrics`.

- [ ] **Step 3: Implement metrics rendering minimally**

Render ASCII Prometheus text with compact fixed help strings and sorted closed
labels. Reject an impossible `ProbeResult` rather than emitting ambiguous samples.

- [ ] **Step 4: Write HTTP service and image RED tests**

Require `GET /healthz` readiness JSON, `GET /metrics` probe execution, fixed
content types/content lengths, query/unknown path/non-GET rejection, no socket at
module import, single-threaded `HTTPServer`, and control-flow-safe shutdown.
Statically require the Dockerfile to use the repository's pinned Python digest,
copy only the observability package, delete bytecode, run UID 65532, and invoke
the module entry point without a shell.

- [ ] **Step 5: Verify service/image RED**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_probe_service.py tests/observability/test_iceberg_rest_probe_deployment.py`

Expected: route and Dockerfile assertions fail because the server/image contract is absent.

- [ ] **Step 6: Implement server and Dockerfile minimally**

Create an `HTTPServer` with one handler class and fixed response helpers. Parse
the three supported environment settings at startup, bind once, serve forever,
and close on ordinary shutdown. Use `python@sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47`.

- [ ] **Step 7: Verify Task 2 GREEN and commit**

Run: `uv run pytest -q tests/observability`

Commit: `feat(observability): expose Iceberg REST synthetic metrics (#90)`

---

### Task 3: Generate the Prometheus scrape contract and add alert rules

**Files:**
- Create: `scripts/observability/prometheus_config.py`
- Create: `observability/prometheus/prometheus.yml`
- Create: `observability/prometheus/rules/iceberg-rest.yml`
- Create: `tests/observability/test_iceberg_rest_prometheus.py`

**Interfaces:**
- Consumes: pinned `infra/services/prometheus/config/prometheus.yml`.
- Produces: `render_prometheus_config(base: Mapping[str, object]) -> bytes`.
- Produces CLI: `python -m scripts.observability.prometheus_config --check`.

- [ ] **Step 1: Write scrape merge RED tests**

Assert the committed config equals the canonical rendering of the pinned Atlas
file plus exactly one final job named `iceberg-rest-synthetic`; every base job,
global value, external label, and rule path remains byte-semantically equal. The
new job has interval 30s, timeout 5s, `/metrics`, and only target
`iceberg-rest-probe:8080`. Duplicate job names and a changed Atlas source fail.

- [ ] **Step 2: Verify scrape RED**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_prometheus.py -k config`

Expected: missing module/artifact failures.

- [ ] **Step 3: Implement deterministic rendering and generate config**

Use `yaml.safe_load`, validate exact mapping/list shapes, append one constant job,
and serialize with `yaml.safe_dump(sort_keys=False, allow_unicode=False)`.
Generate `observability/prometheus/prometheus.yml` via the module, then make
`--check` compare exact bytes without writing.

- [ ] **Step 4: Write alert-rule RED tests**

Require the three exact alert names, fixed job/target labels, durations 2m/2m/10m,
closed PromQL metrics, severity and runbook annotations, no URL authority, no
template that could expose labels, and valid rule group structure.

- [ ] **Step 5: Verify alert RED, implement, and run GREEN**

Run RED: `uv run pytest -q tests/observability/test_iceberg_rest_prometheus.py -k rules`

Implement the one YAML rule group, then run:

`uv run pytest -q tests/observability/test_iceberg_rest_prometheus.py`

- [ ] **Step 6: Commit**

Commit: `feat(observability): define Iceberg REST scrape and alerts (#90)`

---

### Task 4: Provision the Grafana dashboard

**Files:**
- Create: `observability/grafana/iceberg-rest.json`
- Create: `tests/observability/test_iceberg_rest_grafana.py`

**Interfaces:**
- Produces dashboard UID `data-eng-lab-iceberg-rest-synthetic` using datasource UID `Prometheus`.

- [ ] **Step 1: Write dashboard RED tests**

Require a finite strict JSON object with stable UID/title/tags/refresh, datasource
UID `Prometheus`, unique panel IDs, non-overlapping grid positions, and panels for
current availability, current latency, 30-day availability, 30-day p95 latency,
current closed outcome, and active alerts. Assert every PromQL expression uses
only the issue-owned metrics, `ALERTS`, or Prometheus `up` with fixed job/target
filters. `up` is limited to the availability denominator so missing scrapes count
as failed time.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_grafana.py`

Expected: dashboard file missing.

- [ ] **Step 3: Create the minimal provisioned dashboard**

Use classic Grafana dashboard JSON compatible with the pinned Grafana 11.4.3
provider. Define no variables, links, annotations, external URLs, or free-form
labels.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_grafana.py`

Commit: `feat(observability): provision Iceberg REST dashboard (#90)`

---

### Task 5: Wire the consumer-owned deployment without modifying Atlas

**Files:**
- Modify: `atlas.consumer.yml`
- Modify: `compose/data-eng-lab.yml`
- Modify: `tests/test_atlas_usage_contract.py`
- Modify: `tests/observability/test_iceberg_rest_probe_deployment.py`

**Interfaces:**
- Produces internal service `iceberg-rest-probe` on `backend-network` with no host port.
- Overrides Atlas `prometheus` and `grafana` only through parent-owned read-only mounts.

- [ ] **Step 1: Write consumer/deployment RED tests**

Require `PROMETHEUS_SOURCE=container`, `GRAFANA_SOURCE=container`, and
`PROMETHEUS_RETENTION_DAYS=30`. Require the probe build/image contract,
UID/GID 65532, read-only root, all capabilities dropped, no-new-privileges,
16 MiB tmpfs, CPU/memory/PID limits, health check, no host port, fixed environment,
and no dependency on Iceberg health. Require Prometheus to mount the generated
config and rule, Grafana to mount the dashboard, and every mount to be read-only.

- [ ] **Step 2: Verify deployment RED**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_probe_deployment.py tests/test_atlas_usage_contract.py -k 'iceberg_rest or observability'`

Expected: missing service/source/mount assertions fail.

- [ ] **Step 3: Implement the Compose/manifest slice**

Add only the fixed service and three existing-service volume overrides. Keep the
probe independent from `iceberg-rest` readiness so an unavailable catalog remains
observable. Do not add a volume or named persistent state for the probe.

- [ ] **Step 4: Verify assembled Compose GREEN**

Run:

```bash
cp atlas.env.user.example atlas.env.user
uv run pytest -q tests/observability/test_iceberg_rest_probe_deployment.py tests/test_atlas_usage_contract.py
```

Then run the repository's existing bounded placeholder Compose assembly command
and assert the generated services/mount targets exactly.

- [ ] **Step 5: Commit**

Commit: `feat(observability): deploy Iceberg REST synthetic probe (#90)`

---

### Task 6: Publish the canonical runbook to all three surfaces

**Files:**
- Create: `docs/iceberg-rest-observability.md`
- Modify: `docs/manifest.yaml`
- Modify: `docs/atlas-feedback-go-live.md`
- Create: `tests/observability/test_iceberg_rest_observability_docs.py`

**Interfaces:**
- Produces manifest leaf `8.9 Iceberg REST Observability`.
- Produces site page `iceberg-rest-observability.md` and wiki page `Iceberg-REST-Observability.md`.

- [ ] **Step 1: Write documentation RED tests**

Require explicit synthetic/native distinction, the four metric names, six result
values, 30-day retention, both SLO formulas, three alerts, absence of paging,
dashboard queries, operator diagnosis/recovery, no native totals claim, no Atlas
change, and the future upstream trigger. Render site/wiki and require the same
contract on both generated pages. Require the stale feedback bullet to describe
the delivered synthetic boundary.

- [ ] **Step 2: Verify docs RED**

Run: `uv run pytest -q tests/observability/test_iceberg_rest_observability_docs.py`

Expected: canonical runbook and manifest leaf are absent.

- [ ] **Step 3: Write the canonical runbook and manifest entry**

Use first heading `# 8.9. Iceberg REST Observability` and sequential document-local
H2 headings. Link only with relative in-repository paths. State operator commands
as PromQL/query guidance without requiring the live stack for validation.

- [ ] **Step 4: Reconcile the feedback record**

Replace the stale generic query-count request with a factual entry: synthetic
availability/latency delivered by #90; authoritative native request totals remain
unavailable and were not required by #90.

- [ ] **Step 5: Verify three-surface GREEN and commit**

Run:

```bash
uv run pytest -q tests/observability/test_iceberg_rest_observability_docs.py
make docs-check
make docs-wiki
```

Commit: `docs: publish Iceberg REST observability runbook (#90)`

---

### Task 7: Run exact-head verification and close review findings

**Files:**
- Modify only files required by a reproduced review finding.
- Create package outside the repository: `/private/tmp/issue90-review-<head>.patch`.

**Interfaces:**
- Produces an immutable base-to-HEAD binary diff with recorded byte count and SHA-256.

- [ ] **Step 1: Run focused and full offline tests**

Run:

```bash
uv run pytest -q tests/observability tests/test_atlas_usage_contract.py
uv run pytest -m 'not infra and not network' -q
```

Expected: zero failures; only documented opt-in skips/deselections.

- [ ] **Step 2: Run lint, format, configuration, and docs gates**

Run:

```bash
uv run ruff check scripts/observability tests/observability tests/test_atlas_usage_contract.py
uv run ruff format --check scripts/observability tests/observability tests/test_atlas_usage_contract.py
uv run python -m scripts.observability.prometheus_config --check
make verify
make docs-check
make docs-wiki
git diff --check origin/develop...HEAD
```

Validate assembled Compose with bounded placeholder values. If the pinned
Prometheus image is locally available or may be pulled without starting Atlas,
run `promtool check config` and `promtool check rules` in one disposable
`prom/prometheus:v2.55.1` container and remove it exactly.

- [ ] **Step 3: Verify invariants**

Require unchanged hashes for `uv.lock`, `pyproject.toml`, `datasets/registry.yaml`,
and gitlink `infra`; no data-eng-lab containers; the same 13 persistent
data-eng-lab volumes; and untouched user-owned untracked files in the main checkout.

- [ ] **Step 4: Freeze and verify the review package**

Create `git diff --binary 3937eece...HEAD`, record byte count/SHA-256, regenerate
independently, and require `cmp` success.

- [ ] **Step 5: Obtain two independent clean reviews**

Require specification/acceptance and quality/security/adversarial verdicts to
both be `Critical 0, Important 0, Minor 0, Ready Yes`. For each valid finding,
write and observe a focused RED, apply the smallest GREEN, rerun affected/broad
gates, regenerate the immutable package, and repeat both reviews.

- [ ] **Step 6: Promotion handoff**

Only after both reviews are clean, execute the directive's feature-to-develop,
develop-to-main, zero-file backsync, final develop CI, closeout, and exact branch/
worktree cleanup lifecycle.
