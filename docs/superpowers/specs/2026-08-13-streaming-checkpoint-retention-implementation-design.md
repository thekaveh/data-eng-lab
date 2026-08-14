# Dry-run-first streaming checkpoint retention implementation design

**Issue:** #86
**Parent:** #84
**Policy dependency:** #85
**Date:** 2026-08-13
**Status:** Approved design

## 1. Purpose and boundary

This design implements the checkpoint lifecycle defined by issue #85 without
turning an application allowlist into broad deletion authority. It adds bounded
MinIO inventory, writer leases, deterministic dry-run plans, immutable prepared
tombstones, exact-set deletion and partial retry, append-only audits, metrics, and
disposable-fixture live acceptance.

The implementation is manual-only. Its Airflow DAG has `schedule=None`, is paused,
and performs dry-run planning only. Destructive apply requires a reviewed plan SHA,
an exact prefix confirmation, and the dedicated retention service. There is no
automatic or scheduled destructive apply in issue #86.

Issue #86 does not retire an active durable stream, rewrite an educational
streaming scenario as a production application, change Redpanda retention, edit
Atlas source, add a MinIO lifecycle rule, enable bucket versioning or Object Lock,
or add audit-record retention.

## 2. Platform findings that constrain the design

The pinned object store is MinIO `RELEASE.2025-09-07T16-13-09Z`, commit
`07c3a429bfed433e49018cb0f78a52145d4bedeb`; its pinned client image is
`mc RELEASE.2025-08-13T08-35-41Z`. The root Python environment resolves boto3
`1.43.0` and botocore `1.43.0`.

The Atlas `storage:` compiler and `MINIO_EXTRA_CONSUMERS` hook create bucket-wide
service-account policies. They cannot express the action-specific prefix boundary
required here. Existing Spark and Iceberg identities can also read, write, list,
and delete the full `checkpoints` bucket. The maintenance identity therefore needs
a parent-owned custom inline IAM policy; it cannot reuse root, Spark, Iceberg,
Jupyter, Zeppelin, or the generic storage hook.

The pinned MinIO release has a known `PutObject If-Match` missing-object behavior
that was corrected immediately after the selected tag. Its multi-delete handler
does not provide reviewed evidence that per-object ETag, size, and modification-time
preconditions are enforced. Boto3 exposes the fields, but client shape is not proof
of server behavior. This design consequently uses single-replica, service-local
serialization, conditional writes plus immediate verified readback, and
HEAD-before-delete. Those controls are accepted only for manual operation. A
reviewed MinIO pin advance, or equivalent proven cross-process compare-and-swap and
conditional delete behavior, is mandatory before destructive scheduling.

MinIO IAM supports `s3:prefix` conditions on `ListBucket`, while object actions can
be restricted by exact object resource patterns. The production implementation
must prove those permissions against the pinned runtime instead of trusting policy
text alone.

## 3. Selected approach

### 3.1 Decision

Add a parent-owned, single-replica `checkpoint-retention` service and a fixed CLI.
The service is the only holder of the maintenance S3 credential and owns every S3
protocol detail. The CLI and Airflow call its authenticated, internal-only API;
they never receive MinIO credentials or arbitrary S3 endpoints.

The service is deliberately small and uses the existing Python standard-library
HTTP-server pattern plus boto3. Policy parsing and eligibility remain delegated to
`scripts.checkpoints.policy`; network code cannot redefine the #85 rules.

### 3.2 Rejected alternatives

**Host-only CLI.** A direct S3 CLI would spread credentials across operator and CI
surfaces, provide no stable lease API for interactive notebooks, and lack a
scrapeable metrics surface. It is smaller but weaker at every security and
operational boundary.

**Airflow-only implementation.** Airflow does not own the interactive notebook
queries that need leases, and a 15-minute quiescence wait is not a useful worker
lifecycle. Coupling credentials, audits, metrics, and destructive execution to
Airflow also makes the paused/manual boundary harder to verify.

## 4. Policy amendment for disposable isolation

The #85 registry changes only the disposable acceptance path:

```text
streaming_test/{run_uuid}/
```

`run_uuid` is the lowercase canonical hyphenated UUID form. The family root
`streaming_test/` is never a match and is never eligible. The checkpoint ID,
durability, owner, source, sink, 24-hour retention, exclusive-run requirement, and
terminal-state semantics remain unchanged.

`MatchedCheckpoint.generation` carries `run_uuid` for this entry, and the terminal
facts must repeat that exact identity. This is a narrow version-1 policy correction:
it makes disposable live fixtures independently listable and prevents a test plan
from absorbing pre-existing scratch state. The fixed durable paths and GH Archive
generation template do not change.

The legacy full-notebook reset is removed. It cannot call root-credential
`clear_checkpoint`, delete `gh_events_file/`, or treat durable active paths as test
scratch. A future notebook reproducibility run must create a unique disposable leaf
and use the retention protocol; existing durable/generation state is retained.

## 5. Component architecture

### 5.1 Pure domain modules

`scripts/checkpoints/policy.py` remains the strict source for registry parsing,
matching, and eligibility. New focused modules add:

- canonical JSON decoding/encoding with duplicate-key, depth, type, size, and
  unknown-field rejection;
- concrete object records and deterministic manifest sharding;
- lease and terminal-record validation;
- plan, prepared-tombstone, result, and audit state machines;
- low-cardinality metrics rendering; and
- bounded sanitized error categories.

Pure code depends on interfaces, not boto3. Unit tests can exercise state changes
without a network client.

### 5.2 S3 gateway

The S3 gateway accepts a preconfigured boto3 client and the exact policy. It exposes
only named operations such as `inventory_exact_prefix`, `read_control`,
`create_control`, `replace_lease`, `head_exact_object`, `delete_exact_batch`, and
`probe_capabilities`. No public method accepts a bucket, endpoint, or unmatched key.

The client configuration is fixed:

- endpoint `http://minio:9000`;
- bucket `checkpoints`;
- path-style addressing and SigV4;
- fixed configured region;
- environment proxies, EC2 metadata lookup, and redirects disabled;
- connect timeout five seconds, read timeout ten seconds;
- at most two SDK attempts; and
- response bodies closed exactly once.

Every operation has a monotonic deadline. Raw boto3/botocore exception strings,
request objects, endpoints, headers, credentials, and payloads are never chained or
logged. A bounded category and safe operation name replace them.

### 5.3 Internal service

The service is one Compose replica on `backend-network`, with no host port. It
mounts the policy read-only and receives only:

- `MINIO_RETENTION_ACCESS_KEY`;
- `MINIO_RETENTION_SECRET_KEY`;
- `CHECKPOINT_RETENTION_LEASE_TOKEN`;
- `CHECKPOINT_RETENTION_OPERATOR_TOKEN`;
- fixed internal MinIO endpoint, region, and bucket; and
- `CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED`, default `false`.

Both API bearers are distinct from the MinIO credential and from each other.
Notebook containers receive only the lease token; Airflow and the fixed CLI receive
only the operator token; the service receives both. Lease routes accept only the
lease token. Plan, prepare, apply, and status routes accept only the operator token.
Request tokens use a constant-time comparison. Requests use exact
`application/json`, one bounded Content-Length, no transfer encoding, strict
canonical data types, and no query string. Lease and summary bodies are at most 64
KiB; a policy-valid canonical plan body is bounded to 128 MiB and 600,128 JSON
nodes. The server admits at most 16 concurrent requests and applies a 30-second
socket timeout. The fixed routes are:

| Method and route | Purpose |
|---|---|
| `GET /healthz` | Parsed-policy, credential, and capability state; no secret detail |
| `GET /metrics` | Prometheus text without high-cardinality labels |
| `POST /v1/leases/acquire` | Create one exact writer lease |
| `POST /v1/leases/heartbeat` | Renew the caller's exact lease epoch |
| `POST /v1/leases/terminal` | Record a class-valid stopped/completed/retired transition |
| `POST /v1/plans` | Perform the read-only dry run |
| `POST /v1/operations/prepare` | Persist immutable reviewed plan evidence |
| `POST /v1/operations/{id}/apply` | Apply one prepared operation after quiescence |
| `GET /v1/operations/{id}` | Return one bounded operation status |

Paths contain only canonical operation UUIDs. There is no arbitrary SQL, S3 key,
bucket, endpoint, IAM policy, timeout, or retry parameter.

### 5.4 CLI

The fixed CLI is:

```text
uv run python -m scripts.checkpoints.retention plan \
  --checkpoint-id ID --prefix EXACT_PREFIX --facts FILE --output FILE

uv run python -m scripts.checkpoints.retention prepare \
  --plan FILE --plan-sha256 SHA --review REVIEW --actor ACTOR

uv run python -m scripts.checkpoints.retention apply \
  --operation-id UUID --plan-sha256 SHA --confirm-prefix EXACT_PREFIX

uv run python -m scripts.checkpoints.retention status --operation-id UUID
```

The service URL is fixed by environment to the internal service and validated as
exact HTTP origin; the CLI accepts no URL flag. The operator token comes from the
credential boundary and is never printed. `plan` is the default command when no
subcommand is supplied. Commands return canonical JSON to stdout, safe diagnostics
to stderr, and stable exit codes: `0` accepted/completed, `2` invalid request,
`3` refused/not ready, `4` partial, and `5` bounded service failure.

There is no interactive prompt. Manual approval consists of an immutable reviewed
plan file, its exact SHA-256, bounded `review` and `actor` identifiers, and an exact
`--confirm-prefix` value equal to the plan.

## 6. IAM and capability proof

The parent Compose overlay extends `minio-init` with a read-only mounted custom
provisioning script or policy file. It uses root only inside `minio-init` to create
or refresh the service account. Atlas source and the `infra` gitlink remain
unchanged.

The inline policy grants:

- `s3:ListBucket` on `arn:aws:s3:::checkpoints` only when `s3:prefix` matches one
  exact fixed owned family, a constrained GH Archive leaf family, the constrained
  disposable family, or the named `_retention/` control subtrees;
- `s3:GetObject` for matched checkpoint data and retention controls;
- `s3:DeleteObject` only for matched checkpoint data objects; and
- `s3:PutObject` only for leases, tombstone manifests/results, audits, and the
  capability subtree.

It grants no access to another bucket, bucket-root listing, data-object writes,
checkpoint family-root deletion, or control deletion. Explicit denies cover root,
unknown, and control deletion even if a future allow statement broadens.

Once at startup, and in live acceptance, the service probes a random capability
UUID, validates the complete report, and caches that immutable report for health
polling. It proves:

1. allowed exact-prefix list, get, control put, and disposable-fixture delete;
2. denied other-bucket, root-list, unknown-prefix, data put, and control delete;
3. `If-None-Match: *` immutable create behavior;
4. current-ETag `If-Match` replacement and stale-ETag refusal;
5. missing-object `If-Match` behavior; and
6. per-object multi-delete condition behavior when advertised.

Capability controls live only at `_retention/capability/{uuid}.json`; scoped
permission probes use only `streaming_test/{uuid}/capability-{a,b}`. The maintenance
identity proves exact absent-key deletes but cannot delete the immutable capability
control. The exclusive live harness identifies the single control created since its
owned stack start, reads and closes its bounded body, root-deletes only that exact
test-owned key, and proves absence. An ambiguous response, unexpected success,
transport failure, or cleanup mismatch makes startup fail closed.

The pinned missing-object `If-Match` defect is recorded as a known capability. For
manual rollout, lease replacement is accepted only with one service replica, a
per-checkpoint in-process lock, conditional read of the current ETag, `If-Match`
write, and immediate bounded GET that must equal the complete intended canonical
body and returned ETag. Any deletion or mismatch refuses. Cross-process lease CAS
is not claimed.

## 7. Writer lease protocol

Leases use `_retention/leases/{checkpoint_id}.json`; the body contains the exact
concrete prefix, lease epoch UUID, workload, query/run/session/owner identifiers,
whole-second UTC timestamps, state, and bounded terminal evidence defined by #85.

Acquire refuses an existing active lease, an expired-active uncertain lease, a
malformed record, a foreign prefix, a future clock, or any condition/readback
mismatch. Heartbeat occurs every 60 seconds and sets expiry to exactly 600 seconds
after the heartbeat. A writer may update only its checkpoint ID, concrete prefix,
and lease epoch. Terminal transition requires a class-valid state and exact source,
watermark/offset, sink, and recovery evidence.

Both Jupyter and Zeppelin notebooks consume projections generated from one lease
contract. The Python helper and Scala helper:

1. acquire before `writeStream.start()`;
2. terminalize safely if query start fails;
3. heartbeat while the query is active;
4. stop the query and raise a bounded failure if heartbeat ownership is lost; and
5. perform the final heartbeat and terminal transition in `finally`.

The notebook token has access only to the lease API, not plan, prepare, apply, or
MinIO. The helpers accept a fixed internal retention-service origin and exact
checkpoint identity. Notebook code cannot supply an arbitrary endpoint.

Issue #86 integrates leases into all four current streaming notebook pairs and the
disposable go-live example. It does not schedule those queries. Durable checkpoints
remain ineligible while the registry is active.

## 8. Inventory, plan, and manifest contract

Inventory lists only the exact matched concrete prefix. It follows continuation
tokens monotonically and rejects missing/non-progress/cyclic tokens, duplicate
keys, malformed ETags, nonpositive sizes, non-UTC timestamps, family-root/control
records, and any bound violation. Bounds are exactly those in #85.

Each in-memory object record contains exact key, ETag, size, and whole-second
LastModified. Records sort by UTF-8 key bytes. The inventory digest is SHA-256 over
canonical compact JSON records. Logs, metrics, and plan summaries contain only a
prefix digest and counts, never raw keys.

Dry run makes zero S3 writes, including no audit write. It combines live inventory,
the exact lease and terminal controls, the immutable policy, and the supplied
review/recovery facts through `evaluate_retention`. Its canonical summary binds:

- schema version, checkpoint ID, concrete prefix and prefix digest;
- policy SHA, decision, ordered refusal codes, anchor and eligible-after clock;
- count, bytes, newest timestamp, inventory root digest;
- ordered manifest-shard SHA list; and
- evaluation time and bounded actor.

Manifest shards contain the raw exact object records needed for apply. Deterministic
sharding keeps each canonical body at or below 1 MiB and the complete inventory at
or below 100,000 objects and 10 GiB. The plan SHA binds the exact compact summary
bytes and the ordered shard SHA list.

The local plan artifact contains summary plus shards. It is written atomically to a
caller-selected local file with mode `0600`, fsync, rename, and refusal if the target
exists. It never contains credentials, endpoints, headers, or arbitrary object
payloads.

## 9. Prepare, apply, and recovery state machine

### 9.1 Prepare

Prepare reparses the local artifact and requires exact plan/body/shard SHA equality,
an eligible decision, the same live policy SHA, and reviewed actor/review fields. It
creates a random operation UUID and writes:

```text
_retention/tombstones/{operation_id}/manifest/{index}-{sha256}.json
_retention/tombstones/{operation_id}/prepared.json
```

Every shard uses `If-None-Match: *`; `prepared.json` is written last and binds the
ordered shard list, root digest, policy/plan SHA, prefix digest, actor/review, and
whole-second preparation time. Prepared body size is bounded by the policy summary
limit. A duplicate exact prepare returns the same status only when the caller names
the same operation ID and every object is byte-identical; any collision refuses.

Orphan shards created before a failed prepared write are never authoritative and
cannot be applied. They remain evidence for a bounded operator reconciliation; #86
does not garbage-collect controls.

### 9.2 Quiescence and apply

Apply never sleeps in an HTTP request. Before `prepared_at + 900 seconds`, it returns
`not_ready`. At or after that clock, it:

1. reads and validates prepared plus every named shard with strict size, digest,
   count, body-closure, and identity checks;
2. verifies `operation_id`, plan SHA, and `confirm-prefix`;
3. reparses the current policy and requires the same policy SHA;
4. rereads lease, terminal, recovery, and inventory state;
5. reruns eligibility and requires exact count, bytes, newest time, root digest,
   shard list, and no new refusal;
6. HEADs every next batch record and requires exact ETag, size, and LastModified;
7. deletes no more than 1,000 exact approved keys;
8. parses every deleted/error result and stops on the first partial batch;
9. relists the exact prefix and requires it to be empty; and
10. writes bounded immutable result-classification shards, one immutable terminal
    result, and an append-only audit before releasing the checkpoint lock.

One shared per-checkpoint process lock serializes lease acquire/transition with the
final apply revalidation, HEAD, delete, postflight, result, and audit critical
section. This closes the single-replica acquire/apply race. It is not claimed as a
cross-process CAS and therefore does not authorize automatic or scheduled apply.

HEAD-before-delete is not described as atomic conditional deletion. The active
lease refusal, 15-minute quiescence, full inventory equality, manual approval, and
post-delete proof are the accepted manual safety boundary on this pin.

### 9.3 Partial result and retry

A partial result preserves the primary categorized failure and records only object
record digests from the original manifest as deleted, failed, or unattempted.
Classification shards live below
`_retention/tombstones/{operation_id}/results/shards/{sha256}.json`, are at most 1
MiB, and are referenced by an immutable summary below `results/attempts/`. A retry
reads that immutable result and the original shards, derives the remaining
original set, HEAD-verifies those records, and deletes only that set. It may relist
for safety but cannot add a listed key to the operation. Each attempt writes a new
immutable attempt record. The current terminal `completed`, `partial`, or `refused`
summary is selected deterministically from the append-only attempt set. Missing,
conflicting, malformed, non-canonical, or ambiguous controls refuse; they are never
interpreted as a fresh operation.

If all data deletes succeeded but the final audit write failed, recovery rereads the
original manifest and proves that every approved key is absent and no unapproved key
was part of the operation. It may then write the missing completed result/audit. It
cannot issue another delete.

Repeated completed apply is idempotent and returns the existing exact result.
Conflicting state, an unexpected key, a newly active lease, or an ambiguous missing
control refuses without mutation.

## 10. Audit and redaction

Audits live at `_retention/audits/{operation_id}/{attempt}.json` and are immutable.
They contain only:

- schema version, operation/attempt UUIDs, actor and review identifier;
- checkpoint ID and prefix digest;
- policy, plan, manifest-root, and result SHA values;
- exact whole-second timestamps;
- planned/deleted/remaining object and byte counts;
- request counts, decision, refusal codes, and primary safe category; and
- capability-profile identifier.

No credential, bearer token, raw endpoint, header, request body, object payload,
raw object key, stack trace, or arbitrary dependency exception appears in output,
logs, metrics, plans, or audits. Ordinary failures are raised `from None` at the
service boundary. `KeyboardInterrupt` and `SystemExit` remain control-flow
exceptions; cleanup cannot replace an existing primary.

Audits are retained indefinitely until a separate policy exists. The maintenance
credential cannot delete them.

## 11. Metrics contract

`GET /metrics` emits Prometheus text with a fixed maximum body size. Labels are
limited to `checkpoint_id`, `decision`, `refusal_code`, and `outcome` from closed
registries.

Metrics are:

- `checkpoint_retention_objects` and `_bytes` gauges;
- `checkpoint_retention_eligible_bytes` gauge;
- `checkpoint_retention_lease_heartbeat_age_seconds` gauge;
- `checkpoint_retention_last_success_unixtime` gauge;
- `checkpoint_retention_plans_total`;
- `checkpoint_retention_refusals_total`;
- `checkpoint_retention_prepared_total`;
- `checkpoint_retention_deleted_objects_total` and `_bytes_total`;
- `checkpoint_retention_partial_total`; and
- `checkpoint_retention_request_failures_total`.

Operation IDs, prefixes, keys, actor, review, endpoint, ETag, and digests are never
labels. Metrics are reconstructed from bounded in-memory state and current control
records; they do not introduce an unbounded local database.

The parent overlay adds a Prometheus scrape only if it can mount a consumer-owned
supplemental config without editing Atlas. Otherwise the authenticated/internal
`/metrics` endpoint ships and the runbook records the scrape integration as blocked
at the current Atlas pin. That blocker does not weaken CLI/audit/live acceptance and
must not be hidden by changing `infra`.

## 12. Airflow and scheduling

The consumer-owned DAG `checkpoint_retention` is mounted through the existing
`airflow-dags/` overlay. It has:

- `schedule=None`;
- `max_active_runs=1`;
- `catchup=False`;
- a single fixed dry-run inventory task;
- no apply operator or task; and
- no DagRun-conf override that can enable apply or change policy facts.

The DAG is created paused. A repository contract and live API check prove it remains
paused and manual. Its service token enters through a dedicated Airflow connection
or secret environment binding and is never placed in XCom. The bounded canonical
XCom contains the plan summary only, not manifest keys.

`CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED=false` is the deployment default. A
manual apply request receives a refusal while false. Enabling manual disposable
apply requires the reviewed live report and an explicit environment change.
Automatic or scheduled destructive apply additionally requires a reviewed MinIO
pin advance or equivalent proof of cross-process CAS and conditional delete,
sustained dry-run evidence, and a separate reviewed repository change. Issue #86
does not make that change.

## 13. Live acceptance

The genuine `RUN_INFRA=1` harness uses the hardened exclusive-stack pattern:

1. fail before mutation if any project container, including stopped/created, exists;
2. start and own the stack, preserving volumes;
3. snapshot the exact production checkpoint/control inventory digests and IAM
   policy identity;
4. provision the reviewed maintenance service account through `minio-init`;
5. record pinned image digests, policy SHA, service build SHA, and capability result;
6. use one unique `streaming_test/{run_uuid}/` fixture only;
7. create unrelated sentinel objects under allowed and denied namespaces;
8. run a bounded writer with the same lease helper and prove its active lease causes
   plan refusal and zero mutation;
9. stop and terminalize the writer;
10. use an injected evaluator clock to prove the 24-hour retention rule without
    falsifying S3 LastModified or changing the production policy;
11. run dry-run twice and prove byte-identical plan/SHA with zero S3 writes;
12. prepare, alter the fixture during quiescence, and prove apply refuses without
    deleting any fixture object;
13. create a second unique fixture, prepare it, and exercise the real 900-second
    quiescence before successful apply;
14. prove exact eligible-key deletion, empty exact leaf, and preservation of every
    sentinel, control object, other owned prefix, bucket, and volume;
15. inject one bounded delete failure on a third fixture, prove partial audit, retry
    confinement, convergence, and completed idempotence;
16. prove every forbidden IAM call returns AccessDenied;
17. assert exact metrics deltas and absence of high-cardinality/secret text;
18. assert the Airflow DAG is paused, `schedule=None`, and dry-run-only;
19. compare the production inventory/policy snapshots unchanged; and
20. stop only the owned stack and assert zero all-state project containers.

The harness never refreshes a dataset pointer, uses a production checkpoint as a
fixture, deletes a family root, uses root for the retention operation, changes the
retention policy clock, removes a volume, or leaves the maintenance credential in a
tracked artifact. Failures run bounded cleanup and preserve the primary diagnostic.

## 14. Testing strategy

Strict TDD covers:

- the disposable leaf policy amendment and all root/traversal/Unicode ambiguity;
- canonical object records, deterministic shards, byte/count bounds, and digests;
- pagination progress, duplicate keys, malformed metadata, and deadline failures;
- exact lease state, clock, ETag, readback, loss, and notebook cleanup behavior;
- IAM policy structure and live allowed/denied capability cases;
- dry-run zero writes and deterministic local artifact permissions;
- prepare ordering, conditional-create conflicts, orphan shards, and not-ready time;
- changed policy/inventory/lease refusal before delete;
- HEAD mismatch, partial delete, retry confinement, final-audit recovery, and
  idempotence;
- bounded service HTTP/auth/body/path/error behavior and no import-time network;
- fixed CLI commands, exact exit codes, endpoint pinning, and redaction;
- metrics names/labels/bounds;
- Airflow ownership, pause, `schedule=None`, no apply, no secrets/XCom mutation;
- streaming notebook helper parity and legacy root-reset removal;
- moto/fake-S3 behavior and pinned real-MinIO integration; and
- the complete disposable live gate.

The full acceptance includes focused tests, full offline pytest, Ruff, verifier,
strict site/wiki, Compose validation, all Spark Maven applications, diff and
protected-invariant checks, the canonical live harness, and independent spec and
quality/security reviews before push.

## 15. Rollout and recovery

Rollout is staged:

1. merge the policy amendment, service, CLI, custom IAM, lease helpers, dry-run DAG,
   tests, and documentation with destructive mode disabled;
2. prove pinned-MinIO IAM and protocol capabilities in disposable live acceptance;
3. enable manual prepare/apply only for disposable acceptance leaves after the live
   report is reviewed;
4. replace the root/family-root notebook reset with the exact retention path;
5. collect manual dry-run evidence for every owned checkpoint; and
6. consider scheduled planning or destructive apply only in a separate reviewed
   change after the documented MinIO capability blockers are removed.

An interrupted operation resumes from immutable prepared/manifests/results. A
partial operation never broadens beyond the original exact set. Unknown or
ambiguous controls are retained. Tombstones and audits provide evidence, not
rollback; source-and-sink recovery remains mandatory for any non-disposable state.

The parent epic #84 remains Open/Todo until #86 is reviewed, promoted through both
protected branches, live-evidenced, and closed.
