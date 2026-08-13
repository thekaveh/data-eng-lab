# Dry-run-first streaming checkpoint retention implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement issue #86 as a manual-only, dry-run-first checkpoint-retention service with exact writer leases, immutable plans/tombstones/audits, prefix-scoped MinIO authorization, metrics, and disposable live acceptance.

**Architecture:** A single-replica internal Python service is the only holder of the maintenance S3 credential. Pure checkpoint modules produce canonical plans and state transitions; a narrow boto3 gateway performs bounded exact-prefix operations; fixed CLI, notebook lease helpers, and a paused `schedule=None` Airflow DAG call the authenticated service API. Destructive scheduling is absent and manual apply remains capability-gated.

**Tech Stack:** Python 3.11, boto3/botocore, stdlib HTTP server, PyYAML, pytest/moto, Docker Compose, MinIO IAM/S3, Airflow PythonOperator, Prometheus text format, MkDocs/wiki projections.

## Global constraints

- Base commit is `3b3d4e272cbe2021e30512047d959f5a792bc512`; branch is `codex/86-streaming-checkpoint-retention`.
- Do not modify `uv.lock`, `datasets/registry.yaml`, the protected Atlas modernization plan, the `infra` gitlink, or Atlas source.
- Keep #84 Open/Todo until #86 closes; move only #86 to In Progress after this plan commit.
- MinIO is `RELEASE.2025-09-07T16-13-09Z`; pinned-client capability must be proved, not inferred.
- Service replicas equal one. Manual verified-readback CAS is accepted; automatic/scheduled destructive apply requires a later reviewed pin/capability change.
- Dry-run performs zero S3 writes. Apply requires the exact reviewed plan SHA and exact prefix confirmation.
- The Airflow DAG has `schedule=None`, `max_active_runs=1`, is paused, and contains no apply task or DagRun-conf activation path.
- Exact policy values remain: 60-second heartbeat, 600-second TTL, 300-second future tolerance, 900-second quiescence, 100 pages, 100,000 objects, 10 GiB, 1,000 delete keys, 900 active seconds, 64 KiB summaries, and 1 MiB shards.
- The disposable path becomes `streaming_test/{run_uuid}/`; `streaming_test/`, `gh_events_file/`, bucket root, control root, unknown, and malformed prefixes are never eligible.
- All network/request/response/log/error paths are bounded and redact credentials, tokens, endpoints, headers, bodies, raw keys, and dependency exception text.
- Persistent volumes are preserved; live tests use only unique disposable fixtures.

---

### Task 1: Amend the disposable checkpoint policy

**Files:**
- Modify: `checkpoints/retention-policy.yaml`
- Modify: `scripts/checkpoints/policy.py`
- Modify: `tests/checkpoints/test_policy_parser.py`
- Modify: `tests/checkpoints/test_policy_evaluator.py`
- Modify: `tests/checkpoints/test_repository_checkpoint_ownership.py`

**Interfaces:**
- Produces: `CheckpointPolicy.match_prefix("streaming_test/<uuid>/") -> MatchedCheckpoint` with `generation["run_uuid"]`.
- Preserves: every #85 fixed durable and GH-generation contract.

- [ ] **Step 1: Write failing policy tests**

Add cases that require canonical lowercase hyphenated UUID leaves, reject the family root, uppercase/braced/compact UUIDs, traversal, duplicate separators, Unicode, sibling leaves, and require terminal `generation={"run_uuid": ...}` equality.

```python
run_uuid = "550e8400-e29b-41d4-a716-446655440000"
match = policy.match_prefix(f"streaming_test/{run_uuid}/")
assert dict(match.generation) == {"run_uuid": run_uuid}
with pytest.raises(api.PolicyError, match="unknown_prefix"):
    policy.match_prefix("streaming_test/")
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_policy_parser.py tests/checkpoints/test_policy_evaluator.py tests/checkpoints/test_repository_checkpoint_ownership.py`

Expected: failures show the fixed `streaming_test/` entry still matches the root and has no generation identity.

- [ ] **Step 3: Implement the minimal policy amendment**

Change the registry prefix to `streaming_test/{run_uuid}/`; add a strict lowercase UUID regex, include `run_uuid` in the matched mapping, and make disposable terminal validation compare the exact generation mapping before checking exclusive/success flags.

- [ ] **Step 4: Verify GREEN and regressions**

Run: `uv run pytest -q tests/checkpoints`

Expected: all checkpoint tests pass and no fixed durable/GH contract changes.

- [ ] **Step 5: Commit**

```bash
git add checkpoints/retention-policy.yaml scripts/checkpoints/policy.py tests/checkpoints
git commit -m "fix(checkpoints): isolate disposable retention leaves (#86)"
```

### Task 2: Define canonical retention records and manifest shards

**Files:**
- Create: `scripts/checkpoints/records.py`
- Create: `tests/checkpoints/test_retention_records.py`

**Interfaces:**
- Produces: `ObjectRecord`, `ManifestShard`, `PlanArtifact`, `PreparedRecord`, `AttemptRecord`, `AuditRecord` frozen dataclasses.
- Produces: `canonical_json_bytes(value)`, `decode_exact_json(body, schema)`, `shard_inventory(records, max_bytes)`, `inventory_sha256(records)`.

- [ ] **Step 1: Write failing canonical-record tests**

Cover exact key/ETag/size/UTC types, duplicate JSON keys, nonfinite numbers, unknown fields, nesting/body bounds, deterministic UTF-8 key ordering, duplicate object refusal, stable inventory digest, shard bodies at or below 1 MiB, oversize single-record refusal, and deep immutability.

```python
records = [ObjectRecord("streaming_test/.../b", "b" * 32, 2, NOW),
           ObjectRecord("streaming_test/.../a", "a" * 32, 1, NOW)]
assert [r.key for r in canonical_records(records)] == [records[1].key, records[0].key]
assert all(len(shard.body) <= 1_048_576 for shard in shard_inventory(records, 1_048_576))
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_records.py`

Expected: import fails because `scripts.checkpoints.records` does not exist.

- [ ] **Step 3: Implement canonical records minimally**

Use frozen dataclasses, tuples, `MappingProxyType`, exact runtime-type checks, compact sorted ASCII JSON, SHA-256, and incremental shard sizing. Never place credentials, endpoints, headers, or payload content in a record.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest -q tests/checkpoints/test_retention_records.py tests/checkpoints/test_policy_evaluator.py`

- [ ] **Step 5: Commit**

```bash
git add scripts/checkpoints/records.py tests/checkpoints/test_retention_records.py
git commit -m "feat(checkpoints): define canonical retention records (#86)"
```

### Task 3: Implement the bounded S3 gateway and inventory

**Files:**
- Create: `scripts/checkpoints/s3_gateway.py`
- Create: `tests/checkpoints/test_retention_s3_gateway.py`
- Create: `tests/checkpoints/test_retention_s3_minio.py`

**Interfaces:**
- Consumes: `CheckpointPolicy`, `MatchedCheckpoint`, `ObjectRecord`.
- Produces: `S3Gateway.inventory(prefix)`, `read_control(key)`, `create_control(key, body)`, `replace_lease(key, etag, body)`, `head_record(record)`, `delete_records(records)`, `probe_capabilities()`.
- Produces: closed `GatewayFailure.code` values without raw dependency text.

- [ ] **Step 1: Write failing fake-S3 gateway tests**

Cover exact paginator token progress/cycles/duplicates, page/object/byte/deadline bounds, malformed ETag/size/time, exact prefix revalidation, response closure, fixed bucket/origin, path-style/SigV4/timeouts/retry configuration, environment proxy/metadata disablement, immutable create, stale replacement, verified readback, HEAD mismatch, every per-key multi-delete result, partial response, and sanitized cleanup failures.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_s3_gateway.py`

Expected: missing gateway module.

- [ ] **Step 3: Implement the minimal gateway**

Inject the boto3 client and monotonic clock. Revalidate every key through a private exact-prefix/control constructor. Use explicit `Config(signature_version="s3v4", s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=10, retries={"max_attempts": 2, "mode": "standard"})`. Disable metadata before client construction and clear proxy configuration explicitly.

- [ ] **Step 4: Add RED pinned-MinIO integration cases**

Gate with `RUN_MINIO_INTEGRATION=1`; start a disposable pinned MinIO container and prove real `s3:prefix` IAM, `IfNoneMatch`, current/stale/missing `IfMatch`, DeleteObjects result shape, denied roots/controls/data writes, and exact cleanup.

- [ ] **Step 5: Verify RED against assumptions**

Run: `RUN_MINIO_INTEGRATION=1 uv run pytest -q tests/checkpoints/test_retention_s3_minio.py`

Expected: tests record the pinned missing-object IfMatch limitation and conditional-delete capability accurately; unexpected behavior fails closed rather than being skipped.

- [ ] **Step 6: Complete capability-profile handling and verify GREEN**

Run: `uv run pytest -q tests/checkpoints/test_retention_s3_gateway.py` and the pinned integration command.

- [ ] **Step 7: Commit**

```bash
git add scripts/checkpoints/s3_gateway.py tests/checkpoints/test_retention_s3_gateway.py tests/checkpoints/test_retention_s3_minio.py
git commit -m "feat(checkpoints): add bounded MinIO gateway (#86)"
```

### Task 4: Implement lease acquisition, heartbeat, and terminal transitions

**Files:**
- Create: `scripts/checkpoints/leases.py`
- Create: `scripts/checkpoints/lease_client.py`
- Create: `tests/checkpoints/test_retention_leases.py`
- Create: `tests/checkpoints/test_retention_lease_client.py`

**Interfaces:**
- Produces: `LeaseManager.acquire(request)`, `heartbeat(request)`, `terminal(request)` returning canonical lease/status bytes.
- Produces: `LeaseSession` context manager used by notebook projections.

- [ ] **Step 1: Write failing lease-state tests**

Cover new acquire, active conflict, expired-active uncertainty, malformed/foreign/future leases, exact epoch/prefix ownership, 60/600 clocks, service-local per-checkpoint serialization, conditional write plus immediate full-body/ETag readback, start failure terminalization, heartbeat-loss query stop, final heartbeat/terminal order, cleanup failure precedence, and KI/SystemExit behavior.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_leases.py tests/checkpoints/test_retention_lease_client.py`

- [ ] **Step 3: Implement minimal lease manager/client**

Use exact whole-second UTC timestamps and a per-checkpoint `threading.Lock`; one process owns all updates. Do not treat expiry as takeover permission. The client accepts a fixed internal origin and token provider, never raw S3 credentials.

- [ ] **Step 4: Verify GREEN**

Run: the same focused test command plus `uv run pytest -q tests/checkpoints/test_policy_evaluator.py`.

- [ ] **Step 5: Commit**

```bash
git add scripts/checkpoints/leases.py scripts/checkpoints/lease_client.py tests/checkpoints/test_retention_leases.py tests/checkpoints/test_retention_lease_client.py
git commit -m "feat(checkpoints): enforce writer lease lifecycle (#86)"
```

### Task 5: Implement dry-run planning and atomic local artifacts

**Files:**
- Create: `scripts/checkpoints/planner.py`
- Create: `tests/checkpoints/test_retention_planner.py`

**Interfaces:**
- Consumes: gateway inventory/control reads, #85 evaluation, canonical records.
- Produces: `RetentionPlanner.plan(request) -> PlanArtifact` and `write_plan_exclusive(path, artifact)`.

- [ ] **Step 1: Write failing planner tests**

Assert no gateway write method is callable during planning; exact prefix/ID/facts validation; deterministic plan/shards/SHA; exact refusal accumulation; actor/time bounds; zero/huge inventory refusal; atomic mode-0600 exclusive local write, fsync/rename, existing-target refusal, and cleanup preserving the primary.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_planner.py`

- [ ] **Step 3: Implement the planner minimally**

Read the exact lease and terminal controls, convert the inventory summary to `EvaluationInput`, call `evaluate_retention`, and bind ordered shards into the local plan. Do not write an audit for dry run.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest -q tests/checkpoints/test_retention_planner.py tests/checkpoints/test_retention_records.py tests/checkpoints/test_policy_evaluator.py`.

- [ ] **Step 5: Commit**

```bash
git add scripts/checkpoints/planner.py tests/checkpoints/test_retention_planner.py
git commit -m "feat(checkpoints): generate immutable dry-run plans (#86)"
```

### Task 6: Implement prepare, apply, partial retry, and audit recovery

**Files:**
- Create: `scripts/checkpoints/operations.py`
- Create: `tests/checkpoints/test_retention_operations.py`

**Interfaces:**
- Produces: `OperationManager.prepare(request)`, `apply(request)`, `status(operation_id)`.
- Consumes: exact local artifact, gateway, planner, records, injected wall/monotonic clocks.

- [ ] **Step 1: Write failing prepare tests**

Cover plan/body/shard SHA mismatch, refused plan, policy drift, actor/review bounds, immutable shard-first/prepared-last ordering, conditional conflict, byte-identical idempotence, orphan shards remaining non-authoritative, and prepared body bounds.

- [ ] **Step 2: Verify prepare RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_operations.py -k prepare`

- [ ] **Step 3: Implement prepare minimally and verify GREEN**

Write deterministic shard keys with `IfNoneMatch="*"`; write `prepared.json` only after all exact readbacks pass.

- [ ] **Step 4: Write failing apply/recovery tests**

Cover 900-second not-ready response without sleep, manifest/body closure, confirmation mismatch, current policy/lease/inventory drift, HEAD mismatch before any batch, batch size 1,000, mixed Deleted/Error response, no later batch after failure, exact postflight empty check, partial classification, original-set-only retry, no relist broadening, completed idempotence, deleted-but-audit-failed recovery without a second delete, and cleanup/control-flow exception precedence.

- [ ] **Step 5: Verify apply RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_operations.py -k 'apply or partial or recovery'`

- [ ] **Step 6: Implement apply/retry/recovery minimally**

Keep the original manifest authoritative. A safety relist can refuse on extra keys but can never append them to the delete set. Write immutable attempt/audit records and canonical terminal status.

- [ ] **Step 7: Verify GREEN and commit**

Run: `uv run pytest -q tests/checkpoints/test_retention_operations.py tests/checkpoints/test_retention_planner.py`.

```bash
git add scripts/checkpoints/operations.py tests/checkpoints/test_retention_operations.py
git commit -m "feat(checkpoints): apply reviewed retention operations (#86)"
```

### Task 7: Add bounded HTTP service, CLI, auth, and metrics

**Files:**
- Create: `scripts/checkpoints/service.py`
- Create: `scripts/checkpoints/retention.py`
- Create: `scripts/checkpoints/metrics.py`
- Create: `tests/checkpoints/test_retention_service.py`
- Create: `tests/checkpoints/test_retention_cli.py`
- Create: `tests/checkpoints/test_retention_metrics.py`

**Interfaces:**
- Exposes the exact routes and exit codes frozen by the design.
- Produces: `create_server(...)`, `main(argv=None) -> int`, `render_metrics(snapshot) -> bytes`.

- [ ] **Step 1: Write failing service/auth tests**

Cover no import-time network, exact route/method/Content-Type/Content-Length/path rules, 64 KiB body bound before allocation, duplicate JSON fields, token missing/duplicate/bad constant-time rejection, connection/work semaphores, request timeout, bounded response, generic dependency failure, traceback-chain redaction, response/server/gateway close, KI/SystemExit preservation, and health capability failures.

- [ ] **Step 2: Verify RED and implement minimal HTTP service**

Run: `uv run pytest -q tests/checkpoints/test_retention_service.py` before and after implementation.

- [ ] **Step 3: Write failing CLI tests**

Assert default `plan`, exact fixed origin, no URL/S3 flags, token outside argv/output, canonical stdout, bounded sanitized stderr, exact exit codes 0/2/3/4/5, local plan target behavior, and no proxy/env credential discovery.

- [ ] **Step 4: Verify RED and implement minimal CLI**

Run: `uv run pytest -q tests/checkpoints/test_retention_cli.py` before and after implementation.

- [ ] **Step 5: Write metrics RED and implement fixed registry**

Reject dynamic/high-cardinality labels, escape fixed values, bound the body, and assert exact counters/gauges. Run `uv run pytest -q tests/checkpoints/test_retention_metrics.py` before and after.

- [ ] **Step 6: Run focused GREEN and commit**

Run: `uv run pytest -q tests/checkpoints/test_retention_service.py tests/checkpoints/test_retention_cli.py tests/checkpoints/test_retention_metrics.py`.

```bash
git add scripts/checkpoints/service.py scripts/checkpoints/retention.py scripts/checkpoints/metrics.py tests/checkpoints
git commit -m "feat(checkpoints): expose retention service and CLI (#86)"
```

### Task 8: Provision exact IAM and deploy the single-replica service

**Files:**
- Create: `checkpoints/retention.Dockerfile`
- Create: `checkpoints/retention-policy.json`
- Create: `checkpoints/provision-retention.sh`
- Create: `atlas.env.user.example`
- Modify: `.gitignore`
- Modify: `atlas.consumer.yml`
- Modify: `compose/data-eng-lab.yml`
- Create: `tests/checkpoints/test_retention_deployment.py`

**Interfaces:**
- Produces Compose services `checkpoint-retention-init` and `checkpoint-retention`.
- Consumes secrets from ignored `atlas.env.user`; exposes no host port.

- [ ] **Step 1: Write failing deployment/IAM tests**

Parse the IAM JSON and assert exact action/resource/condition/deny sets, no wildcard bucket/object root, no data PutObject or control DeleteObject, exact image/user/read-only mount, one replica, no root credentials in runtime service, healthcheck, no public ports, `DESTRUCTIVE_ENABLED=false`, and no Atlas source changes.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_deployment.py`.

- [ ] **Step 3: Implement provisioning and Compose minimally**

The init script uses root only to create/refresh the inline service-account policy, uses `set -eu` and disables xtrace around secrets. Add `env.file: ./atlas.env.user` to the consumer manifest, ignore the real file, and document generation in the example without real values.

- [ ] **Step 4: Verify GREEN and assembled Compose**

Run:

```bash
uv run pytest -q tests/checkpoints/test_retention_deployment.py
./infra/start.sh --consumer "$(pwd)/atlas.consumer.yml" compose validate
```

- [ ] **Step 5: Commit**

```bash
git add checkpoints compose/data-eng-lab.yml atlas.consumer.yml atlas.env.user.example .gitignore tests/checkpoints/test_retention_deployment.py
git commit -m "feat(checkpoints): provision scoped retention runtime (#86)"
```

### Task 9: Integrate notebook leases and remove the root reset

**Files:**
- Modify: all eight files under the four streaming scenario `jupyter/` and `zeppelin/` directories
- Modify: `docs/go-live.md`
- Modify: `tests/scenarios/test_notebook_reproducibility_live.py`
- Modify: `tests/scenarios/live_exec.py`
- Create: `tests/checkpoints/test_streaming_lease_integration.py`

**Interfaces:**
- Consumes: fixed lease client/API and per-scenario checkpoint identity.
- Removes: `clear_checkpoint` and every broad family-root/root-credential reset path.

- [ ] **Step 1: Write failing exhaustive integration tests**

Discover every executable `checkpointLocation`; require acquire-before-start, heartbeat lifecycle, stop-on-lost-lease, terminal-finally, exact GH generation and disposable UUID identity, equivalent Jupyter/Zeppelin contract, and no root credentials, `clear_checkpoint`, `gh_events_file/` root reset, or direct S3 deletion in the live harness.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_streaming_lease_integration.py tests/checkpoints/test_repository_checkpoint_ownership.py`.

- [ ] **Step 3: Implement minimal generated projections**

Use one canonical helper template to update notebook cells/paragraphs; keep educational transform outputs unchanged. The reproducibility harness must create unique disposable state or preserve existing streaming state rather than delete it.

- [ ] **Step 4: Verify notebook JSON and GREEN**

Run:

```bash
uv run pytest -q tests/checkpoints/test_streaming_lease_integration.py tests/checkpoints/test_repository_checkpoint_ownership.py tests/checkpoints/test_streaming_policy_warnings.py
uv run python scripts/validate_notebooks.py
```

- [ ] **Step 5: Commit**

```bash
git add scenarios tests/scenarios docs/go-live.md tests/checkpoints/test_streaming_lease_integration.py
git commit -m "feat(streaming): bind notebook checkpoints to leases (#86)"
```

### Task 10: Add the paused dry-run-only Airflow DAG

**Files:**
- Create: `airflow-dags/checkpoint_retention/__init__.py`
- Create: `airflow-dags/checkpoint_retention/dag.py`
- Create: `airflow-dags/checkpoint_retention/tasks.py`
- Create: `tests/checkpoints/test_retention_dag.py`

**Interfaces:**
- Produces DAG ID `checkpoint_retention` with one fixed plan task and bounded canonical XCom summary.

- [ ] **Step 1: Write failing DAG contract tests**

Require isolated import without network, `schedule=None`, `catchup=False`, `max_active_runs=1`, paused creation, no apply/delete symbol/task, no DagRun-conf facts/endpoints, fixed API origin, token outside XCom, retry/timeout bounds, complete prefix inventory order, and failure propagation.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_dag.py`.

- [ ] **Step 3: Implement minimal DAG and task**

The task calls `POST /v1/plans` for the fixed registry inventory and returns only bounded summaries. Set the DAG pause state through the supported Airflow creation/default contract and verify it live; never unpause in tests.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest -q tests/checkpoints/test_retention_dag.py tests/checkpoints/test_retention_service.py`.

```bash
git add airflow-dags/checkpoint_retention tests/checkpoints/test_retention_dag.py
git commit -m "feat(checkpoints): add paused retention planning DAG (#86)"
```

### Task 11: Build the genuine disposable live harness

**Files:**
- Create: `tests/scenarios/test_checkpoint_retention_live.py`
- Create: `tests/checkpoints/test_retention_live_harness.py`
- Create: `docs/superpowers/reports/2026-08-13-streaming-checkpoint-retention-live-acceptance.md`

**Interfaces:**
- Offline harness helpers are unit-tested; `RUN_INFRA=1` executes the exact 20-step design acceptance.

- [ ] **Step 1: Write failing offline harness tests**

Cover exclusive all-state container ownership, partial-start cleanup, volume preservation, exact unique fixture leaves, production before/after inventory/policy equality, service-account capability proof, active refusal, dry-run zero writes, changed-inventory refusal, real-quiescence control, exact delete, sentinel preservation, injected partial/retry confinement, metrics/log redaction, paused DAG, and zero final containers.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_retention_live_harness.py`.

- [ ] **Step 3: Implement offline helpers and safe skip**

The live test skips unless `RUN_INFRA=1`. It never auto-provisions or refreshes a dataset, never infers absence from a generic error, and never uses production checkpoint state as a fixture.

- [ ] **Step 4: Verify offline GREEN**

Run: `uv run pytest -q tests/checkpoints/test_retention_live_harness.py tests/scenarios/test_checkpoint_retention_live.py`.

- [ ] **Step 5: Run canonical live acceptance**

Run:

```bash
RUN_INFRA=1 uv run --group live pytest -q tests/scenarios/test_checkpoint_retention_live.py -s
```

Expected: exact capability profile, active/changed refusal, two deterministic dry runs, successful exact disposable deletion after real 900-second quiescence, partial retry convergence, preserved sentinels/production state/volumes, paused DAG, and zero project containers.

- [ ] **Step 6: Freeze exact evidence and rerun**

Record image/service/policy SHA values, operation IDs, plan/result/audit SHA values, object/byte/request counts, exact metrics, elapsed time, and teardown evidence. Add assertions for stable reviewed identities, then rerun the unchanged harness.

- [ ] **Step 7: Commit**

```bash
git add tests/scenarios/test_checkpoint_retention_live.py tests/checkpoints/test_retention_live_harness.py docs/superpowers/reports/2026-08-13-streaming-checkpoint-retention-live-acceptance.md
git commit -m "test(checkpoints): prove disposable retention live (#86)"
```

### Task 12: Reconcile runbooks, metrics, diagrams, and all documentation surfaces

**Files:**
- Modify: `docs/checkpoint-retention.md`
- Modify: `README.md`
- Modify: `docs/index.md`
- Modify: `docs/go-live.md`
- Modify: `docs/lakehouse.md`
- Modify: `scenarios/execution-modes.yaml`
- Modify: four streaming scenario READMEs and generated projections
- Modify: `docs/diagrams/overview.html`
- Regenerate: `docs/diagrams/img/overview.png`
- Regenerate: `docs/diagrams/img/overview.sha256`
- Modify: docs site/wiki generators and tests
- Modify: `docs/CHANGELOG.md`
- Modify: `tests/checkpoints/test_policy_docs.py`

**Interfaces:**
- Publishes one truthful manual-only contract on repository, site, and wiki surfaces.

- [ ] **Step 1: Write failing docs/projection tests**

Require exact CLI commands, IAM boundary, lease lifecycle, plan/prepare/apply/retry/recovery, metrics, manual approval, paused `schedule=None`, MinIO CAS/delete blockers, pin-upgrade requirement, live evidence, no broad reset, and #84/#86 lifecycle truth across every surface.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest -q tests/checkpoints/test_policy_docs.py tests/checkpoints/test_streaming_policy_warnings.py tests/scenarios/test_execution_modes.py`.

- [ ] **Step 3: Update canonical sources and regenerate projections**

Use the repository docs/site/wiki generators and the architecture-diagram skill for affected diagrams. Preserve historical reports as historical.

- [ ] **Step 4: Verify GREEN and visual output**

Run:

```bash
uv run pytest -q tests/checkpoints/test_policy_docs.py tests/checkpoints/test_streaming_policy_warnings.py tests/scenarios/test_execution_modes.py
make docs-check
make docs-wiki
```

Inspect regenerated PNGs at original resolution and require exact SHA sidecars.

- [ ] **Step 5: Commit**

```bash
git add README.md docs scenarios scripts tests
git commit -m "docs(checkpoints): publish retention operations contract (#86)"
```

### Task 13: Run full gates and request independent reviews

**Files:**
- Update ignored: `.superpowers/sdd/progress.md`
- Generate ignored: `.superpowers/sdd/review-issue-86.diff`
- Update tracked verification report if exact final evidence changes.

- [ ] **Step 1: Run focused and full offline gates**

```bash
uv run ruff check . --exclude graphify-out
uv run pytest -q tests/checkpoints
make lint
make test
make verify
make docs-check
make docs-wiki
./infra/start.sh --consumer "$(pwd)/atlas.consumer.yml" compose validate
git diff --check origin/develop...HEAD
```

- [ ] **Step 2: Run all six Maven application gates**

Run `mvn test package` in every `spark-apps/*/pom.xml` directory. No app source change is expected, but the repository gate must remain green.

- [ ] **Step 3: Rerun final live artifact if any runtime byte changed**

Run the canonical #86 live harness after the final service image and policy SHA are fixed; update the tracked report only from actual output.

- [ ] **Step 4: Audit protected invariants**

Require unchanged protected-plan, `uv.lock`, registry, Atlas gitlink/nested status, preserved volume count, zero containers, no staged/untracked `graphify-out`, no secrets, no root retention runtime credential, and #84 Open/Todo plus #86 Open/In Progress.

- [ ] **Step 5: Generate the exact review package**

```bash
git diff --binary 3b3d4e272cbe2021e30512047d959f5a792bc512..HEAD > .superpowers/sdd/review-issue-86.diff
shasum -a 256 .superpowers/sdd/review-issue-86.diff
```

- [ ] **Step 6: Request independent spec and quality/security reviews**

Both reviewers must verify the exact package SHA and report Critical/Important/Minor counts. Fix findings in one strict RED/GREEN wave and regenerate/re-review until both return C0/I0/M0 and Ready Yes.

- [ ] **Step 7: Stop before push**

Report exact commits, RED/GREEN evidence, live identifiers, capability limitations, gate results, package SHA, reviews, worktree state, issue/project state, protected invariants, zero containers, and preserved volumes. Do not push or open a PR until separately authorized.
