# Issue #86 streaming checkpoint retention live acceptance

**Date:** 2026-08-13
**Branch:** `codex/86-streaming-checkpoint-retention`
**Base:** `3b3d4e272cbe2021e30512047d959f5a792bc512`
**Boundary:** manual-only exact-leaf apply; automatic and scheduled destructive apply remain disabled

## Final artifacts

| Artifact | Exact identity |
|---|---|
| Retention service image | `sha256:e691423be6837e56754b51b3ff5404fe98235b6b50f327b1adc6d3a67563e1b9` (`linux/amd64`) |
| Pinned MinIO image | `minio/minio:RELEASE.2025-09-07T16-13-09Z`, local image ID `sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253` |
| Pinned MinIO client | `minio/mc:RELEASE.2025-08-13T08-35-41Z`, local image ID `sha256:5dee113ef037d349ac22ab6c20193ade5c4701e2a38e3777fa1c1bec1c063ad1` |
| Policy YAML SHA-256 | `8b06b3cfd439652a4f70c9b2fc7e604321e953507096e63d872ef326db7568de` |
| IAM policy SHA-256 | `f99e615481c6367a2e6b857604eb50417b827895dd5398f4b15408abf1effe62` |
| Capability profile | `minio-2025-09-manual-verified-readback` |

The service ran as one non-root, read-only-filesystem replica. Destructive mode was
enabled only for the test-owned disposable proof. The deployed default remains
`false`. No Atlas source or gitlink file changed.

## Final split-token runtime replay

The final architectural-correction replay passed `1 passed in 160.53s` with zero
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
- an active disposable lease made planning refuse with `lease_active`, then its
  exact stopped terminal evidence enabled byte-identical repeated eligible plans;
- two reviewed immutable prepares returned `not_ready` before 900 seconds and made
  zero checkpoint-data deletes; and
- the paused `schedule=None` DAG, closed metrics, unrelated sentinel, and exact
  production snapshot SHA-256
  `19bd48d158628d31d62193a25a0be88714e293c9e1fa78ae754659c5b4cee217`
  were unchanged.

| Fixture UUID | Operation ID | Plan SHA-256 | Prepared at | State |
|---|---|---|---|---|
| `b5794925-bb9b-40ad-bf88-c5c4aa946df8` | `25b4e8c6-2328-44cb-80b7-20cca692b2d3` | `fe54c6298840c4b1b160cccc157627fedced451d783aa4154ec15039c204c08b` | `2026-08-13T21:03:35Z` | `not_ready`; changed fixture remained three objects |
| `a6ba8036-792e-43ff-9acd-13f61a379124` | `239715ab-1b00-4046-bc84-bbc21b5064e6` | `5b3bf2b0ff528cfb0abaf64a044d08b809313cee1f1bda58a1f663a750f551e3` | `2026-08-13T21:03:36Z` | `not_ready`; exact two-object fixture preserved |

The final operation implementation was also exercised against pinned disposable
MinIO after the correction: `2 passed in 2.43s`. That layer uses the real gateway,
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
active, terminalized with exact generation evidence, and produced byte-identical
eligible plans on repeated dry run. The unrelated sentinel survived. The production
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

A separate isolated-clock run used disposable UUID
`d207b095-441c-43e0-9ac7-adbfec3122f7` and operation
`84c6f055-b3b5-45d9-8c0b-e7ddf685a515`. The clock moved from prepare to
`prepared_at + 901 seconds` only inside the test-owned process. The real service and
S3 protocol deleted exactly two approved objects, wrote one completed status and one
audit, left the fixture empty, preserved the production snapshot SHA above, and
finished with zero project containers.

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

The final checkpoint-focused result is `312 passed, 2 expected integration skips`,
and the separately enabled pinned-MinIO result is `2 passed in 2.43s`. This layered
proof exercises the final partial-result
runtime bytes without another 900-second wait and without changing the immutable
prepare/quiescence protocol.

## Final repository verification

The final service/runtime commit is `fd34113`; the subsequent evidence commit changes
documentation plus Ruff-only formatting with an AST-equality proof and no runtime
semantics. The exact final validation set was:

| Gate | Result |
|---|---|
| Full offline Python suite | 3,294 passed, 43 expected live skips, 0 failures/errors in 53.33 s |
| Checkpoint-focused suite | 312 passed, 2 expected skips |
| Final split-token live replay | 1 passed, 0 skipped/failures/errors in 160.518 s |
| Pinned-MinIO operation/restart integration | 2 passed in 2.43 s |
| GH Archive Maven suite, Java 17 | 19 passed in 4:10 |
| MovieLens Maven suite, Java 17 | 12 passed in 1:49 |
| NYC quality Maven suite, Java 17 | 37 passed in 2:43 |
| NYC ETL Maven suite, Java 17 | 4 passed in 1:00 |
| NYC medallion Maven suite, Java 17 | 2 passed in 56.090 s |
| TPC-H Maven suite, Java 17 | 9 passed in 1:32 |
| `make verify` | 0 findings, 0 errors |
| `make docs-check` and `make docs-wiki` | strict site build and deterministic wiki check passed |
| Ruff check and format check | passed |
| Compose validation with both explicit non-secret token classes | `Compose config is valid.` |
| Range `git diff --check` | passed |

The Maven runs used the pinned `maven:3.9.11-eclipse-temurin-17` tag at exact local
`linux/amd64` image ID
`sha256:bbb7e05a6487b189e3dc833b6360b4f9eaf0154299fb4e67e764cad5cca33800`
with isolated test-owned repository copies, HOME, Maven cache, and retained per-app
logs. Expected injected Spark action failures appeared only inside their negative
tests; every suite completed with zero failed, aborted, canceled, ignored, or pending
tests.

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
