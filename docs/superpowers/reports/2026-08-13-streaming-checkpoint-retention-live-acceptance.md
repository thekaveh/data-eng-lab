# Issue #86 streaming checkpoint retention live acceptance

**Date:** 2026-08-14
**Branch:** `codex/86-streaming-checkpoint-retention`
**Base:** `3b3d4e272cbe2021e30512047d959f5a792bc512`
**Boundary:** manual-only exact-leaf apply; automatic and scheduled destructive apply remain disabled

## Final artifacts

| Artifact | Exact identity |
|---|---|
| Retention service image | `sha256:f54d5ebf7b20c2c60af3b5eb3e010267d9ba789f996533049d984f77c58c6a6f` (`linux/amd64`) |
| Pinned MinIO image | `minio/minio:RELEASE.2025-09-07T16-13-09Z`, local image ID `sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253` |
| Pinned MinIO client | `minio/mc:RELEASE.2025-08-13T08-35-41Z`, local image ID `sha256:5dee113ef037d349ac22ab6c20193ade5c4701e2a38e3777fa1c1bec1c063ad1` |
| Policy YAML file SHA-256 | `8b06b3cfd439652a4f70c9b2fc7e604321e953507096e63d872ef326db7568de` |
| Canonical loaded-policy SHA-256 | `305332e957226528242e7739a5b7b0253328f529ee4c9873b8402bae48fc7a89` |
| IAM policy SHA-256 | `f99e615481c6367a2e6b857604eb50417b827895dd5398f4b15408abf1effe62` |
| Capability profile | `minio-2025-09-manual-verified-readback` |

The service ran as one non-root, read-only-filesystem replica. Destructive mode was
enabled only for the test-owned disposable proof. The deployed default remains
`false`. No Atlas source or gitlink file changed.

## Final split-token and exact-image layered replay

The final architectural-correction replay passed `1 passed in 139.45s` with zero
failures, errors, or skips. It began and ended with zero all-state project
containers and used standard volume-preserving teardown. The final service image
above proved:

- the lease-only bearer received HTTP 401 from the plan route, with exact bounded
  body `{"code":"unauthorized"}`;
- the operator bearer received the same HTTP 401 contract from lease acquire;
- startup became ready only after one randomized observed conditional-create and
  conflict proof, verified conditional replace/readback, expected-denied stale and
  missing replace, exact-leaf list/get/delete and multi-delete, plus denied family
  root, foreign bucket, data put, unknown-control put, and control delete. The
  immutable result was cached for subsequent `/healthz` calls; the harness bounded,
  read, closed, root-cleaned, and proved absence of the one test-owned capability
  control rather than creating a control on every health poll;
- the actual maintenance credential retained the exact three allowed and four
  denied IAM results described below;
- an active disposable lease made planning refuse with `lease_active`; after its
  exact stopped terminal evidence, the fresh real-wall-clock plan remained safely
  refused with exact codes `future_clock,retention_quarantine` and made zero
  checkpoint-data deletes. A second server-clock plan produced the exact same
  semantic artifact after excluding only server-owned `evaluated_at`;
- a caller-supplied `evaluated_at` field was rejected with HTTP 400
  `request_invalid`, proving that no API or configuration surface can advance the
  production evaluation clock;
- an isolated one-shot process used this exact final image, the production runtime
  composition, actual MinIO/IAM, and a test-only in-process clock to evaluate the
  disposable leaf at terminal plus 86,401 seconds, prepare it, return `not_ready`,
  then refuse an injected changed-inventory object at prepared plus 901 seconds
  with exact `revalidation_mismatch` and durable `refused` status. The root fixture
  helper removed only that exact test-owned object, after which the same immutable
  operation converged to `completed`. The injection exists only in the
  mounted acceptance script and is not reachable through service API/configuration;
- the accelerated operation deleted exactly its two 15-byte manifest objects,
  performed two immediately-before-delete HEADs and one delete request, proved the
  exact postflight empty inventory SHA-256, and wrote four append-only result attempts
  plus four complete audits for `prepared`, `not_ready`, `refused`, and `completed`;
  the refused attempt reported zero HEADs, zero deletes, and both original objects
  unattempted; and
- the paused `schedule=None` DAG, closed metrics, unrelated sentinel, and exact
  production snapshot were unchanged.

| Fixture UUID | Operation ID | Plan SHA-256 | Prepared at | State |
|---|---|---|---|---|
| `9e200024-759d-4546-a68a-5ee5e014d2a8` | `7d29c0f0-08f5-56d0-9f0e-eb813332d24a` | `d92ba26e4ff9338edb80afaab525e403bd2fba45f67828494c9d8403c950640b` | `2026-08-15T23:59:43Z` | `prepared` → `not_ready` → `refused` → `completed`; exact two-object fixture empty |

The completed attempt used result SHA-256
`bc7622c1bb833667b1aa8c9c4ed11c8bc429e72e4f80d08c55b1eb37d6f6f811`,
empty postflight inventory SHA-256
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`,
manifest and inventory SHA-256 `4dccd45d2d2ec56eb8ada8be1a5de82bf73a5f0fae0c3fba0559799453737978`,
and prefix digest `77b31356572fde9b06bcccee679766ff87a2a781d7e7202f443fe6b53a39eaa2`.
The refused attempt and audit used exact result SHA-256
`1d38a68a98d8db40a2ca80a7103e4cf07fecddf1c9e0d0ef6eb698c59a068cfd`
and exact refusal code `revalidation_mismatch` before any HEAD or delete request.
The four canonical attempt UUIDs, in sequence, were
`dbf2a692-3292-53e6-b5db-4e58cc407d46`,
`5193c282-0654-5962-8c3d-d511dd1eb5ab`,
`54ed7905-0211-5b23-b920-62af086d6b8d`, and
`1a757644-5db9-5447-8dc9-c48b52529f0e`; each is also the exact immutable
audit filename for its attempt.
The final operation implementation was also exercised against pinned disposable
MinIO inside the same live gate: `1 passed`. That layer uses the real gateway,
immutable result-classification shards, a fresh manager after partial persistence,
original-manifest-only retry, completed audit recovery, and idempotence. It supplies
the final-byte delete/restart proof without repeating the wall-clock wait below.

## Historical canonical 900-second operation proof

The exclusive-stack harness began with zero all-state project containers, preserved
all named volumes, and snapshotted the production checkpoint/control inventory. It
used two unique `streaming_test/{run_uuid}/` leaves plus one unrelated sentinel.

The successful operation was:

- operation ID `f7822b11-f0a5-47d2-a282-631e04e49706`;
- fixture UUID `a86ded18-5703-45ce-86dd-61b3b23f722d`;
- plan SHA-256 `298206249176d3f7577cf840899b48d0a362c33f357544c22a22ba7b89de2b22`;
- prepared at `2026-08-13T18:20:45Z`;
- not ready before `2026-08-13T18:35:45Z`;
- completed after the real 900-second quiescence;
- exactly two manifest objects deleted; and
- exact postflight inventory empty.

The changed-inventory operation was:

- operation ID `c16786a6-c944-4bbe-a701-3f56c9b2573d`;
- fixture UUID `a08a6245-f333-4e76-b5f9-1e75f3c9217a`;
- plan SHA-256 `5d86e03bf7b7332e7b7171af853e8edb435dde91c8bb1cf8dfab409676fb3507`;
- prepared at `2026-08-13T18:20:44Z`; and
- refused after one new fixture object appeared, with all three fixture objects
  preserved.

Before those applies, the same fixture identity was refused while its lease was
active, terminalized with exact generation evidence, and produced the same inventory,
policy, prefix, and decision binding on repeated dry run under the then-supported
caller-supplied whole-second evaluation clock. The final-runtime evidence above, not
this historical run, proves repetition under the corrected server-owned clock. The unrelated sentinel survived. The production
snapshot remained exactly
`19bd48d158628d31d62193a25a0be88714e293c9e1fa78ae754659c5b4cee217`.
The Airflow API returned `checkpoint_retention` paused; its source contract has
`schedule=None` and no apply task.

This earlier run established real wall-clock quiescence and exact deletion before
the architectural correction. It reached every destructive assertion, then the test rejected the
actual Prometheus body in its own incomplete closed-label parser. No service failure
or unverified deletion occurred. Commit `be8d74a` corrected only that test parser;
offline adversarial cases now accept every exact labeled/unlabeled metric family and
reject unknown, duplicate, malformed, or high-cardinality samples. That parser-only
correction did not change runtime bytes. The later architectural correction did;
the final split-token and pinned-MinIO evidence above supersedes the old runtime
identity while retaining this wall-clock timing proof.

## Accelerated exact-delete proof

The final isolated-clock operation is the `7d29c0f0-...` operation above. Its clock
was injected only by assigning the imported exact-image runtime's internal clock in
the mounted, test-owned process. Production service construction, HTTP routes, and
Compose configuration expose no such input. The durable evidence contains exact
attempt sequences 1–4 and four corresponding audits; the terminal completed audit
records actor `issue86-accelerated-exact-image`, review
`issue86-live-reviewed`, one delete request, two HEAD requests, two objects and 15
bytes deleted, zero remaining objects/bytes, and the exact capability, policy,
manifest, prefix, plan, result, and postflight hashes recorded above.

## Actual maintenance-credential IAM proof

The final overlay provisioned one explicit bounded live identity. A separate S3
client using that maintenance access key and secret performed the calls instead of
trusting `/healthz` metadata. Against a unique two-object disposable leaf it proved:

- allowed: one exact-leaf list, one bounded exact-object get with body closure, and
  one capability-control put;
- denied with `AccessDenied`: family-root list, unrelated-sentinel get,
  checkpoint-data put, and capability-control delete; and
- root performed only exact test-owned cleanup of the capability object and the
  three fixture/sentinel objects.

The first diagnostic attempt exposed a non-secret local ignored-env access-key drift:
the stack had intentionally provisioned the harness identity while the host helper
read an older ignored default. It failed `InvalidAccessKeyId` before any retention
operation. The harness now freezes one explicit access-key constant for both Compose
provisioning and the probe, with a regression asserting the 3–20 byte shape. No secret
or credential value is recorded here.

## Partial retry and idempotence proof

No production fault-injection route exists. A pinned disposable MinIO integration
wraps the real `S3Gateway` and delegates the first original-manifest object deletion,
then returns one deterministic mixed partial response. The production manager:

1. records exactly that successful original record in partial status;
2. retries without issuing another delete for it;
3. refuses a newly inserted foreign key and never adds it to any delete request;
4. completes only after the root test fixture removes that foreign key;
5. proves the exact original prefix empty; and
6. returns the completed result idempotently without another delete.

The final checkpoint-focused result is `389 passed, 2 expected opt-in MinIO skips in
1.64s`, and the separately enabled pinned-MinIO result is `2 passed in 2.49s`. This layered
proof exercises the final partial-result
runtime bytes without another 900-second wait and without changing the immutable
prepare/quiescence protocol.

## Final repository verification

The final service/runtime and layered acceptance correction represented by the
recorded image is `394db7f`. The subsequent evidence update changes only this
report and its traceability assertion. The exact final
validation set was:

| Gate | Result |
|---|---|
| Full offline Python suite | 3,340 passed, 2 expected opt-in skips, 72 infra/network deselections, 0 failures/errors in 56.70 s |
| Checkpoint-focused suite | 389 passed, 2 expected opt-in MinIO skips in 1.64 s |
| Final split-token/exact-image layered live replay | 1 passed, 0 skipped/failures/errors in 139.45 s |
| Pinned-MinIO operation/restart integration | 2 passed in 2.49 s |
| GH Archive Maven suite, Java 17 | 19 passed in each requested lifecycle pass; package succeeded in 6:35 |
| MovieLens Maven suite, Java 17 | 12 passed in each requested lifecycle pass; package succeeded in 3:47 |
| NYC quality Maven suite, Java 17 | 37 passed in each requested lifecycle pass; package succeeded in 4:57 |
| NYC ETL Maven suite, Java 17 | 4 passed in each requested lifecycle pass; package succeeded in 1:04 |
| NYC medallion Maven suite, Java 17 | 2 passed in each requested lifecycle pass; package succeeded in 58.175 s |
| TPC-H Maven suite, Java 17 | 9 passed in each requested lifecycle pass; package succeeded in 2:07 |
| `make verify` | 0 findings, 0 errors |
| `make docs-check` and `make docs-wiki` | strict site build and deterministic wiki check passed |
| Ruff check; scoped changed-file format check | passed |
| Compose validation with both explicit non-secret token classes | `Compose config is valid.` |
| Range `git diff --check` | passed |

The Maven runs used the pinned `maven:3.9.11-eclipse-temurin-17` tag at exact local
`linux/amd64` image ID
`sha256:bbb7e05a6487b189e3dc833b6360b4f9eaf0154299fb4e67e764cad5cca33800`
with isolated test-owned repository copies, HOME, Maven cache, and retained per-app
logs. Expected injected Spark action failures appeared only inside their negative
tests; every suite completed with zero failed, aborted, canceled, ignored, or pending
tests.

The final authority/audit correction changes no Spark application source, POM, or
JVM test. The six Java 17 Maven results above therefore remain exact for unchanged
application bytes; the final wave reran every affected Python/runtime, pinned-MinIO,
live, documentation, Compose, lint, and repository-invariant gate.

## Cleanup and preserved state

- Every test-owned data object and the exact harness-owned capability control were
  removed by exact key; immutable tombstone/audit evidence was preserved.
- Standard `stop-all` teardown preserved volumes.
- Final all-state project-container count was zero.
- All 13 named project volumes remained.
- `uv.lock`, the dataset registry, and the Atlas gitlink remained unchanged.
- Automatic and scheduled destructive apply remain disabled until a reviewed MinIO
  pin advance or equivalent cross-process CAS and conditional delete proof, sustained
  dry-run evidence, and a separate reviewed repository change.
