# Streaming Checkpoint Retention Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver issue #85's strict checkpoint ownership registry, non-networked parser, pure fail-closed retention evaluator, executable ownership checks, and synchronized operational documentation without deleting or mutating any checkpoint.

**Architecture:** `checkpoints/retention-policy.yaml` is the single machine-readable policy. The focused `scripts/checkpoints/policy.py` module parses it into immutable typed values and evaluates supplied lease, inventory, lifecycle, and clock facts without importing an S3 client or performing I/O beyond the caller-provided policy file; repository tests bind every executable checkpoint path to exactly one registry entry. Documentation and generated surfaces explain the policy while issue #86 remains the sole owner of networked inventory, conditional writes, deletion, credentials, scheduling, and destructive live acceptance.

**Tech Stack:** Python 3.11, standard-library dataclasses/enum/datetime/hashlib/json/re, PyYAML through the repository's existing dependency set, pytest, repository notebook parity/docs generators, MkDocs, GitHub wiki projection.

## Global Constraints

- The bucket is exactly `checkpoints`; `_retention/` is reserved and never a checkpoint target.
- The version is exact integer `1`; unknown fields, duplicate YAML keys, unsafe paths, overlapping prefixes, and invalid enum combinations fail closed.
- Owned identities are exactly `streaming-events-v1`, `streaming-event-windows-v1`, `streaming-online-retail-cdc-v1`, `streaming-gh-archive-file-v1`, and `go-live-streaming-test-v1`.
- Durable checkpoints require reviewed retirement plus a 30-day quarantine; GH Archive exact generation leaves require 14 days; exclusive acceptance scratch requires 24 hours.
- Heartbeat cadence is 60 seconds, lease TTL is 10 minutes, future-clock tolerance is 5 minutes, and deletion quiescence is 15 minutes.
- Operation bounds are 100 pages, 100,000 objects, 10 GiB, 1,000 keys per delete request, 15 minutes active operation time, 64 KiB summaries, and 1 MiB manifest shards.
- Expiry or lack of Spark-session visibility never proves inactivity. Active, expired-active, malformed, conflicting, unknown, root, control, future-dated, and changed state is ineligible.
- Issue #85 contains no boto3/MinIO request, checkpoint mutation, delete path, credential, Airflow DAG, schedule, or import-time network behavior.
- Issue #86 owns leases in running workloads, networked inventory/control writes, dedicated RBAC, dry-run/apply CLI, deletion, audit/metrics persistence, scheduling, and destructive disposable-fixture live acceptance.
- Do not modify `uv.lock`, the dataset registry, Atlas source/gitlink, the protected unrelated plan, persistent volumes, or `graphify-out/`.

---

### Task 1: Canonical registry and strict typed parser

**Files:**
- Create: `checkpoints/retention-policy.yaml`
- Create: `scripts/checkpoints/__init__.py`
- Create: `scripts/checkpoints/policy.py`
- Create: `tests/checkpoints/__init__.py`
- Create: `tests/checkpoints/test_policy_parser.py`
- Create: `tests/checkpoints/fixtures/invalid/` fixture files named by rejected condition

**Interfaces:**
- Produces: `load_policy(path: Path) -> CheckpointPolicy`, `parse_policy(text: str) -> CheckpointPolicy`, immutable `CheckpointPolicy`, `CheckpointEntry`, `OperationBounds`, `LeasePolicy`, and `PolicyError`.
- Produces: `CheckpointPolicy.match_prefix(prefix: str) -> MatchedCheckpoint`; a concrete GH Archive leaf is returned only when scale, publication ID, and manifest SHA match exactly.
- Consumes: no repository runtime service and no environment variables.

- [ ] **Step 1: Write parser RED tests**

  Add table-driven tests that load the canonical file and assert the exact bucket,
  control prefix, numeric bounds, five IDs, owners, source/sink bindings, lifecycle,
  durability, recovery, retention, and constrained GH template. Add literal YAML
  fixtures proving duplicate mapping keys are rejected before construction.

  ```python
  def test_canonical_policy_freezes_exact_owned_entries():
      policy = load_policy(ROOT / "checkpoints" / "retention-policy.yaml")
      assert policy.version == 1
      assert policy.bucket == "checkpoints"
      assert tuple(policy.entries) == EXPECTED_IDS
      assert policy.bounds.max_objects == 100_000
      assert policy.lease.ttl_seconds == 600

  @pytest.mark.parametrize("fixture", INVALID_FIXTURES)
  def test_policy_rejects_invalid_fixture(fixture):
      with pytest.raises(PolicyError, match=fixture.expected_code):
          parse_policy(fixture.text)
  ```

- [ ] **Step 2: Run parser tests and capture RED**

  Run: `uv run pytest -q tests/checkpoints/test_policy_parser.py`

  Expected: collection or import failure because `scripts.checkpoints.policy` and
  `checkpoints/retention-policy.yaml` do not exist.

- [ ] **Step 3: Implement the exact registry and immutable types**

  Define the YAML with explicit top-level `version`, `bucket`, `control_prefix`,
  `lease`, `bounds`, and `checkpoints`. Each checkpoint entry must spell out its
  exact path or constrained template, owner, workload, source, sink, lifecycle,
  durability, terminal states, retention seconds, recovery class, sink disposition,
  concurrency rule, and retirement authorization.

  ```python
  @dataclass(frozen=True)
  class OperationBounds:
      max_pages: int
      max_objects: int
      max_bytes: int
      max_delete_keys: int
      max_active_seconds: int
      max_summary_bytes: int
      max_manifest_shard_bytes: int

  @dataclass(frozen=True)
  class CheckpointPolicy:
      version: int
      bucket: str
      control_prefix: str
      lease: LeasePolicy
      bounds: OperationBounds
      entries: Mapping[str, CheckpointEntry]
  ```

  Use a `yaml.SafeLoader` subclass whose mapping constructor rejects duplicate keys.
  Then recursively validate exact key sets and exact Python types (`bool` must never
  satisfy an integer field). Reject empty/control/root/absolute/dot/backslash paths,
  unconstrained placeholders, overlapping match spaces, invalid exact enums, and
  contradictory lifecycle/terminal/recovery combinations.

- [ ] **Step 4: Add exact prefix/template matching tests and implementation**

  Prove exact fixed-prefix matching, exact generation-leaf matching, and refusal of
  the GH root, uppercase/short IDs, unsupported scale, extra suffix, stale
  `events_stream/`, upstream `redpanda/atlas_stream_events/`, bucket roots, control
  objects, URI-form inputs, Unicode ambiguity, and overlapping separators.

  ```python
  assert policy.match_prefix("events/").checkpoint_id == "streaming-events-v1"
  assert policy.match_prefix(
      "gh_events_file/tiny/" + "a" * 32 + "/" + "b" * 64 + "/"
  ).checkpoint_id == "streaming-gh-archive-file-v1"
  with pytest.raises(PolicyError, match="unknown_prefix"):
      policy.match_prefix("gh_events_file/")
  ```

- [ ] **Step 5: Run parser GREEN gates**

  Run: `uv run pytest -q tests/checkpoints/test_policy_parser.py`

  Expected: all parser and matching cases pass without importing boto3, requests,
  Airflow, Spark, or Atlas packages.

- [ ] **Step 6: Commit the parser slice**

  ```bash
  git add checkpoints/retention-policy.yaml scripts/checkpoints tests/checkpoints
  git diff --cached --check
  git commit -m "feat(checkpoints): add strict retention registry (#85)"
  ```

### Task 2: Pure fail-closed eligibility evaluator

**Files:**
- Modify: `scripts/checkpoints/policy.py`
- Create: `tests/checkpoints/test_policy_evaluator.py`

**Interfaces:**
- Consumes: `CheckpointPolicy`, one concrete matched checkpoint, and caller-supplied `EvaluationInput` containing a UTC evaluation time, registry state, lease facts, terminal facts, and bounded inventory summary.
- Produces: `evaluate_retention(policy, facts) -> RetentionDecision` with `eligible`, exact ordered refusal codes, retention anchor, eligible-after time, and canonical plan payload/digest.
- Produces: immutable `LeaseFacts`, `InventorySummary`, `TerminalFacts`, `EvaluationInput`, `RetentionDecision`; it never fetches or changes external state.

- [ ] **Step 1: Write decision-matrix RED tests**

  Cover active, current lease, expired-active lease, stopped, completed, retired,
  missing/conflicting/malformed lease, missing terminal evidence, durable-active,
  durable-retired before/at/after 30 days, GH before/at/after 14 days, scratch
  before/at/after 24 hours, wrong generation, legacy/unknown/root/control, future
  clock, object-after-terminal, changed inventory, and partial-retry confinement.

  ```python
  def test_expired_active_lease_is_uncertain_not_stopped(policy, durable_facts):
      facts = replace(durable_facts, lease=replace(
          durable_facts.lease, state="active", expires_at=NOW - timedelta(seconds=1)
      ))
      assert evaluate_retention(policy, facts).refusal_codes == (
          "lease_expired_active_uncertain",
      )

  def test_retired_durable_eligible_only_at_full_quarantine(policy, durable_facts):
      decision = evaluate_retention(policy, retired(durable_facts, days=30))
      assert decision.eligible is True
      assert decision.retention_anchor == EXPECTED_ANCHOR
  ```

- [ ] **Step 2: Run evaluator tests and capture RED**

  Run: `uv run pytest -q tests/checkpoints/test_policy_evaluator.py`

  Expected: import or attribute failures for the evaluator types and function.

- [ ] **Step 3: Implement typed clock and state validation**

  Accept only timezone-aware UTC whole-second datetimes. Compute the retention anchor
  as the maximum terminal/retired timestamp, final heartbeat, and newest object
  timestamp. Reject any clock over 300 seconds in the future, any object newer than
  terminal evidence, missing evidence, or inconsistent lease/prefix/checkpoint IDs.

- [ ] **Step 4: Implement exact class-specific eligibility**

  Durable `active` entries always refuse. Durable `retired` entries require reviewed
  retirement authorization, `stopped|retired` terminal lease, recovery disposition,
  and 2,592,000 seconds. GH exact leaves require `completed|stopped`, immutable
  generation equality, sink-reset recovery contract, and 1,209,600 seconds. Scratch
  requires exclusive successful/stopped evidence and 86,400 seconds.

- [ ] **Step 5: Implement canonical local plan bytes and digest**

  Produce sorted compact UTF-8 JSON that excludes endpoints, credentials, headers,
  raw object payloads, and full object names. Include policy digest, checkpoint ID,
  concrete prefix, decision, ordered refusal codes, clock/anchor, inventory count,
  bytes, newest timestamp, and exact caller-supplied inventory digest. Reject a plan
  over 65,536 bytes.

  ```python
  canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
  digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
  ```

- [ ] **Step 6: Add determinism, redaction, and purity regressions**

  Prove input order does not change bytes/digest, arbitrary credentials/endpoints do
  not enter output or failures, repeated calls are identical, caller objects are not
  mutated, no network module is imported, and no filesystem file changes.

- [ ] **Step 7: Run evaluator GREEN gates and commit**

  ```bash
  uv run pytest -q tests/checkpoints/test_policy_parser.py tests/checkpoints/test_policy_evaluator.py
  git add scripts/checkpoints/policy.py tests/checkpoints
  git diff --cached --check
  git commit -m "feat(checkpoints): evaluate retention fail closed (#85)"
  ```

### Task 3: Bind every executable checkpoint path to the registry

**Files:**
- Create: `tests/checkpoints/test_repository_checkpoint_ownership.py`
- Modify: `tests/test_docs_content_contract.py`

**Interfaces:**
- Consumes: canonical policy and repository notebook/go-live checkpoint literals.
- Produces: an executable coverage contract proving each owned executable location maps once and every unowned location refuses.
- Does not change `tests/scenarios/live_exec.py`; replacement of its destructive helper is #86 scope.

- [ ] **Step 1: Write ownership coverage RED tests**

  Parse both Jupyter and Zeppelin artifacts plus `docs/go-live.md` and assert the
  concrete owners are `events/`, `event_windows/`, `online_retail_cdc/`, the exact
  generation template, and `streaming_test/`. Assert `events_stream/` is absent from
  executable paths and Atlas `redpanda/atlas_stream_events/` is explicitly rejected.

  ```python
  assert executable_checkpoint_families() == {
      "events/", "event_windows/", "online_retail_cdc/",
      "gh_events_file/{scale}/{publication_id}/{manifest_sha256}/", "streaming_test/",
  }
  ```

- [ ] **Step 2: Run coverage tests and capture RED**

  Run: `uv run pytest -q tests/checkpoints/test_repository_checkpoint_ownership.py tests/test_docs_content_contract.py`

  Expected: failures because the repository has no canonical registry bindings or
  exclusive-test warning contract.

- [ ] **Step 3: Add policy parity helpers in the test module**

  Normalize only known language interpolation syntax; do not use permissive substring
  matching. Require exactly one registry match per executable family and exact source,
  sink, owner, durability, recovery, and retention values for each mapping.

- [ ] **Step 4: Freeze the unsafe helper boundary**

  Add documentation-contract tests requiring `clear_checkpoint` to remain explicitly
  exclusive-stack-only and forbidding bucket-root/control/unknown targets in the
  documented workflow. Inspect its exact call sites and require the current harness to
  supply only the four registered notebook checkpoint families. Do not harden or
  replace the function under #85 and do not add network behavior.

- [ ] **Step 5: Run ownership GREEN gates and commit**

  ```bash
  uv run pytest -q tests/checkpoints tests/test_docs_content_contract.py
  git add tests/checkpoints/test_repository_checkpoint_ownership.py tests/test_docs_content_contract.py
  git diff --cached --check
  git commit -m "test(checkpoints): bind executable owners to policy (#85)"
  ```

### Task 4: Reconcile canonical streaming scenario and notebook guidance

**Files:**
- Modify: `scenarios/streaming_ingest-events-spark-iceberg/README.md`
- Modify: `scenarios/streaming_ingest-events-spark-iceberg/jupyter/notebook.ipynb`
- Modify: `scenarios/streaming_ingest-events-spark-iceberg/zeppelin/notebook.zpln`
- Modify: `scenarios/streaming_windows-events-spark-iceberg/README.md`
- Modify: `scenarios/streaming_windows-events-spark-iceberg/jupyter/notebook.ipynb`
- Modify: `scenarios/streaming_windows-events-spark-iceberg/zeppelin/notebook.zpln`
- Modify: `scenarios/cdc_streaming-online_retail-spark-iceberg/README.md`
- Modify: `scenarios/cdc_streaming-online_retail-spark-iceberg/jupyter/notebook.ipynb`
- Modify: `scenarios/cdc_streaming-online_retail-spark-iceberg/zeppelin/notebook.zpln`
- Modify: `scenarios/streaming_ingest-gh_archive-spark-iceberg/README.md`
- Modify: `scenarios/streaming_ingest-gh_archive-spark-iceberg/jupyter/notebook.ipynb`
- Modify: `scenarios/streaming_ingest-gh_archive-spark-iceberg/zeppelin/notebook.zpln`
- Modify: `tests/scenarios/test_build_notebooks.py`
- Modify: `tests/scenarios/test_parity.py`

**Interfaces:**
- Consumes: exact policy classifications and recovery consequences.
- Produces: identical policy warnings in each Scala/PySpark notebook pair and source README while preserving every executable notebook output and checkpoint path.

- [ ] **Step 1: Add notebook/README warning RED assertions**

  Require each language to state the checkpoint ID, durability class, owner, active/
  uncertain retention rule, recovery consequence, no age-only deletion, and #86 lease
  integration boundary. Require GH to state exact-generation identity plus paired sink
  reset; require durable streams to state duplicate/replay risk.

- [ ] **Step 2: Run focused docs/parity tests and capture RED**

  Run: `uv run pytest -q tests/scenarios/test_build_notebooks.py tests/scenarios/test_parity.py tests/checkpoints/test_repository_checkpoint_ownership.py`

  Expected: missing-warning failures while executable parity remains unchanged.

- [ ] **Step 3: Add the approved warnings without changing code cells**

  Edit Markdown/paragraph cells only. Preserve query definitions, source/sink logic,
  output schemas, and exact checkpoint strings. State that these educational notebooks
  do not yet emit #86 leases, so automated deletion remains disabled.

- [ ] **Step 4: Regenerate notebook projections and run GREEN tests**

  Run the existing notebook projection command identified by
  `tests/scenarios/build_notebooks.py`, then:

  ```bash
  uv run pytest -q tests/scenarios/test_build_notebooks.py tests/scenarios/test_parity.py tests/checkpoints/test_repository_checkpoint_ownership.py
  ```

- [ ] **Step 5: Commit the scenario slice**

  ```bash
  git add scenarios/streaming_* scenarios/cdc_streaming-online_retail-spark-iceberg docs/notebooks tests/scenarios
  git diff --cached --check
  git commit -m "docs(streaming): document checkpoint ownership (#85)"
  ```

### Task 5: Publish the operational policy across all documentation surfaces

**Files:**
- Create: `docs/checkpoint-retention.md`
- Modify: `README.md`
- Modify: `docs/index.md`
- Modify: `docs/scenarios/index.md`
- Modify: `docs/scenarios/execution-modes.md`
- Modify: `scenarios/execution-modes.yaml`
- Modify: `docs/go-live.md`
- Modify: `docs/lakehouse.md`
- Modify: `scripts/scenario_execution.py`
- Modify: `scripts/docs/manifest.py`
- Modify: `tests/scenarios/test_execution_modes.py`
- Modify: `tests/scripts/docs/test_manifest.py`
- Modify: `tests/scripts/docs/test_build_docs.py`
- Modify: `tests/scripts/docs/test_push_wiki.py`
- Modify: `tests/test_docs_content_contract.py`
- Generated: `wiki/Checkpoint-Retention.md` through the repository wiki command

**Interfaces:**
- Consumes: canonical policy values and #85/#86 boundary.
- Produces: one canonical operator document projected to repository/site/wiki plus consistent scenario matrix and go-live warnings.

- [ ] **Step 1: Add documentation contract RED tests**

  Assert all surfaces show the exact five IDs, three retention durations, lease clocks,
  anchor/future/object rules, bounds, RBAC fail-closed condition, recovery classes,
  dry-run/tombstone protocol, break glass, scheduling disabled, and #86 boundary.
  Require go-live to label current reset exclusive and unsafe in shared state.

- [ ] **Step 2: Run focused docs tests and capture RED**

  Run: `uv run pytest -q tests/scripts/docs tests/scenarios/test_execution_modes.py tests/test_docs_content_contract.py`

  Expected: missing canonical page, manifest, matrix, and warning failures.

- [ ] **Step 3: Write the canonical runbook and navigation**

  The runbook must give deterministic examples of eligible and refused supplied facts,
  exact dry-run fields, recovery checklists per class, break-glass approval, audit fields,
  and an explicit statement that #85 commands cannot contact MinIO or delete data.

- [ ] **Step 4: Update matrix and scenario projections**

  Keep all streaming scenarios intentionally unscheduled. Add ownership/policy dependency
  and #86 enforcement/live dependency without claiming leases are deployed. Fix stale
  `events_stream` prose and retain the upstream Atlas prefix as non-owned.

- [ ] **Step 5: Generate repository/site/wiki projections**

  Run:

  ```bash
  uv run python scripts/scenario_execution.py --write
  make docs-build
  make docs-wiki
  ```

  Inspect generated changes and confirm only canonical projections changed.

- [ ] **Step 6: Run docs GREEN gates and commit**

  ```bash
  uv run pytest -q tests/scripts/docs tests/scenarios/test_execution_modes.py tests/test_docs_content_contract.py
  make docs-check
  make docs-wiki
  git add README.md docs scenarios/execution-modes.yaml scripts/scenario_execution.py scripts/docs/manifest.py tests
  git diff --cached --check
  git commit -m "docs(checkpoints): publish retention runbook (#85)"
  ```

### Task 6: Verify scope, non-mutation, and review readiness

**Files:**
- Create: `docs/superpowers/reports/2026-08-13-streaming-checkpoint-retention-policy-verification.md`
- Update ignored: `.superpowers/sdd/progress.md`
- Generate ignored: `.superpowers/sdd/review-issue-85.diff`

**Interfaces:**
- Consumes: all preceding implementation commits.
- Produces: exact offline gate evidence and immutable review package; no push or PR.

- [ ] **Step 1: Prove the module remains non-networked and non-destructive**

  Add/import tests asserting no boto3, requests, MinIO, Airflow, Spark, subprocess,
  socket, delete, put, or credential path is reachable from `scripts.checkpoints.policy`.
  Search the branch diff for mutation APIs and manually adjudicate documentation-only
  mentions.

- [ ] **Step 2: Run focused and full verification**

  ```bash
  uv run pytest -q tests/checkpoints tests/scenarios/test_build_notebooks.py tests/scenarios/test_parity.py tests/scenarios/test_execution_modes.py tests/scenarios/test_live_exec_unit.py tests/test_docs_content_contract.py tests/scripts/docs
  make lint
  make test
  make verify
  make docs-check
  make docs-wiki
  docker compose -f compose/data-eng-lab.yml config --quiet
  git diff --check origin/develop...HEAD
  ```

  A bounded read-only inventory may be recorded only if it uses no production mutation;
  #85 acceptance does not require a destructive or networked live gate.

- [ ] **Step 3: Audit protected invariants and Project state**

  Confirm #85 is Open/In Progress, #84 remains Open/Todo, #86 remains Open/Todo, no
  containers were started, persistent volumes remain, and protected plan/`uv.lock`/
  dataset registry/Atlas gitlink and source/`graphify-out/` are unchanged.

- [ ] **Step 4: Write the verification report and final implementation commit**

  Record exact commands, counts, hashes, commit list, non-networked proof, no-live
  rationale, docs projections, issue states, and worktree invariants.

  ```bash
  git add docs/superpowers/reports/2026-08-13-streaming-checkpoint-retention-policy-verification.md
  git diff --cached --check
  git commit -m "docs(checkpoints): record policy verification (#85)"
  ```

- [ ] **Step 5: Generate immutable review package and request independent reviews**

  ```bash
  git diff --binary origin/develop...HEAD > .superpowers/sdd/review-issue-85.diff
  shasum -a 256 .superpowers/sdd/review-issue-85.diff
  ```

  Request independent specification and quality/security reviews against the exact
  HEAD and package SHA. Address findings through strict RED/GREEN commits, rerun
  affected gates, and regenerate the package. Stop before push/PR until both verdicts
  are ready.
