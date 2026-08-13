# Issue #86 streaming checkpoint retention live acceptance

**Date:** 2026-08-13  
**Branch:** `codex/86-streaming-checkpoint-retention`  
**Base:** `3b3d4e272cbe2021e30512047d959f5a792bc512`  
**Boundary:** manual-only exact-leaf apply; automatic and scheduled destructive apply remain disabled

## Final artifacts

| Artifact | Exact identity |
|---|---|
| Retention service image | `sha256:0710363ce59fd42cf11dca3bc7cd5ab03b80e8a32af47003e4b625a90bd678b7` (`linux/amd64`) |
| Pinned MinIO image | `minio/minio:RELEASE.2025-09-07T16-13-09Z`, local image ID `sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253` |
| Pinned MinIO client | `minio/mc:RELEASE.2025-08-13T08-35-41Z`, local image ID `sha256:5dee113ef037d349ac22ab6c20193ade5c4701e2a38e3777fa1c1bec1c063ad1` |
| Policy YAML SHA-256 | `8b06b3cfd439652a4f70c9b2fc7e604321e953507096e63d872ef326db7568de` |
| IAM policy SHA-256 | `f99e615481c6367a2e6b857604eb50417b827895dd5398f4b15408abf1effe62` |
| Capability profile | `minio-2025-09-manual-verified-readback` |

The service ran as one non-root, read-only-filesystem replica. Destructive mode was
enabled only for the test-owned disposable proof. The deployed default remains
`false`. No Atlas source or gitlink file changed.

## Canonical 900-second operation proof

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

The run reached and passed every destructive assertion, then the test rejected the
actual Prometheus body in its own incomplete closed-label parser. No service failure
or unverified deletion occurred. Commit `be8d74a` corrected only that test parser;
offline adversarial cases now accept every exact labeled/unlabeled metric family and
reject unknown, duplicate, malformed, or high-cardinality samples. The 900-second
wait was not repeated because no runtime byte changed in that correction.

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

The pinned-MinIO integration result is `2 passed in 2.09s`. The focused gateway,
operation, IAM-helper, metrics-helper, and live-harness result is `43 passed, 1
expected live skip in 0.11s`. This layered proof exercises the final partial-result
runtime bytes without another 900-second wait and without changing the immutable
prepare/quiescence protocol.

## Cleanup and preserved state

- Every test-owned data object and non-immutable capability control was removed by
  exact key; immutable tombstone/audit evidence was preserved.
- Standard `stop-all` teardown preserved volumes.
- Final all-state project-container count was zero.
- All 13 named project volumes remained.
- `uv.lock`, the dataset registry, and the Atlas gitlink remained unchanged.
- Automatic and scheduled destructive apply remain disabled until a reviewed MinIO
  pin advance or equivalent cross-process CAS and conditional delete proof, sustained
  dry-run evidence, and a separate reviewed repository change.
