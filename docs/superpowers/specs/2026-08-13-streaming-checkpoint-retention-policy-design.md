# Streaming checkpoint ownership, retention, and recovery policy design

**Issue:** #85
**Parent:** #84
**Implementation child:** #86
**Date:** 2026-08-13
**Status:** Approved design

## 1. Purpose

This design defines the data-eng-lab ownership, durability, retention, active-use,
recovery, authorization, and audit contract for Spark Structured Streaming
checkpoints. It makes deletion eligibility objectively computable without adding
networked deletion or scheduling in issue #85.

Issue #85 delivers a strict machine-readable policy, a non-mutating parser and
decision evaluator, tests, and synchronized documentation. Issue #86 will consume
that contract to implement bounded MinIO inventory and deletion, dedicated runtime
credentials, metrics, disposable-fixture live acceptance, and any later schedule.

No checkpoint is disposable merely because it is old or no query is visible.
Unknown, legacy, malformed, active, or operationally uncertain state is retained.

## 2. Context and constraints

The `checkpoints` bucket lives in the persistent `${PROJECT_NAME}-minio-data`
Docker volume. An ordinary stop preserves it; `stop.sh --cold` removes the volume.
The Atlas backup service does not back up `minio-data`, and the bucket has no
versioning, Object Lock, lifecycle policy, or checkpoint restore mechanism.
Deletion is therefore irreversible unless the source and sink can be rebuilt.

The streaming scenarios run through operator-started Zeppelin or Jupyter sessions
on the shared Spark Connect service. Spark's `spark.streams.active` view is local to
one Spark session, and the standalone Spark driver API cannot enumerate streaming
queries in another notebook session. Absence from either view is not proof that a
checkpoint is inactive.

Current Spark Connect and worker S3A configuration uses MinIO root credentials for
filesystem access. Atlas also provisions Spark and Iceberg service accounts with
bucket-wide read, write, delete, and list access to `checkpoints`. The existing
test helper uses MinIO root credentials. These are inherited platform limitations;
this policy does not misrepresent them as prefix-level isolation and does not modify
Atlas source. Issue #86 must use a separate, prefix-scoped maintenance credential
for retention operations or leave scheduling disabled.

## 3. Selected approach

### 3.1 Decision

Use a reviewed machine-readable ownership registry, explicit conditional leases,
and a two-phase tombstoned deletion protocol.

The registry gives every executable checkpoint path an owner, durability class,
source, sink, recovery consequence, and fixed retention rule. A lease provides the
cross-session active-use signal that Spark cannot discover globally. A dry-run plan,
conditional tombstone, quiescence interval, and exact inventory recheck prevent an
approved operation from silently broadening between inspection and deletion.

### 3.2 Rejected alternatives

**MinIO lifecycle rules by age or prefix** are rejected. They cannot bind a
checkpoint to a stream, sink recovery, immutable generation, active lease, reviewed
dry run, or partial-operation audit. Server-side expiration could delete a live
Spark state store without an application-visible refusal.

**Manual cleanup based on best-effort runtime discovery** is rejected as the normal
policy. Notebook sessions are isolated, so Jupyter, Zeppelin, Spark Connect, and
standalone-driver inspection cannot prove global inactivity. A narrowly authorized
break-glass procedure remains available for exceptional recovery.

## 4. Owned checkpoint registry

The canonical registry is versioned as exact integer version `1`. It fixes the
bucket to `checkpoints`, reserves `_retention/` for controls, and defines these five
data-eng-lab checkpoint identifiers:

| Checkpoint ID | Exact prefix or template | Owner | Durability | Source | Sink |
|---|---|---|---|---|---|
| `streaming-events-v1` | `events/` | Streaming Data Engineering | durable stream | Redpanda topic `events` | `lakehouse.bronze.events` |
| `streaming-event-windows-v1` | `event_windows/` | Streaming Data Engineering | durable stream | Redpanda topic `events` | `lakehouse.gold.event_windows` |
| `streaming-online-retail-cdc-v1` | `online_retail_cdc/` | Streaming Data Engineering | durable stream | Redpanda topic `online_retail_cdc` | `lakehouse.silver.online_retail_cdc` |
| `streaming-gh-archive-file-v1` | `gh_events_file/{scale}/{publication_id}/{manifest_sha256}/` | Streaming Data Engineering Education | generation reproducibility | one resolver-verified immutable GH Archive generation | `lakehouse.bronze.gh_events_stream` |
| `go-live-streaming-test-v1` | `streaming_test/` | Lab Acceptance Engineering | disposable acceptance | bounded synthetic go-live input | `s3a://lakehouse/bronze/streaming_test` |

`scale` is exactly one of `tiny`, `small`, or `medium`; `publication_id` is 32
lowercase hexadecimal characters; `manifest_sha256` is 64 lowercase hexadecimal
characters. The GH Archive root `gh_events_file/` is never a deletable target.

`events_stream` is stale example prose rather than an executable writer and must be
corrected. The Atlas example `redpanda/atlas_stream_events/` is not owned by this
repository. If present, it is treated as unknown and retained.

The registry records, per entry:

- checkpoint ID, exact prefix or constrained template, owner, lifecycle state, and
  durability class;
- runtime and source identity, sink identity, and output behavior;
- retention duration and required terminal state;
- recovery class, source availability requirement, and sink disposition;
- whether concurrent writers are forbidden;
- the issue or reviewed change that authorizes a retirement transition.

The registry rejects duplicate keys, unknown fields, duplicate checkpoint IDs,
overlapping prefixes, unsafe templates, empty values, invalid classes or states,
unbounded patterns, and inconsistent recovery or retention fields.

## 5. Durability and retention clocks

### 5.1 Durable streams

`events/`, `event_windows/`, and `online_retail_cdc/` are never eligible through age
alone while their registry lifecycle is `active`.

Eligibility requires all of the following:

1. a reviewed registry transition to `retired`;
2. an exact whole-second UTC `retired_at` timestamp;
3. an approved source and sink recovery disposition;
4. an ETag-bound terminal lease state of `stopped` or `retired`;
5. no valid or conflicting active lease;
6. a 30-day quarantine after the retention anchor; and
7. an unchanged exact inventory through the deletion quiescence check.

### 5.2 Generation reproducibility

An exact GH Archive generation leaf becomes eligible 14 days after a valid terminal
`completed` or `stopped` record, provided its scale, publication ID, and manifest
digest exactly match the path and immutable source record. Rerunning an expired
generation requires resetting the shared `gh_events_stream` sink first; otherwise a
fresh checkpoint would append duplicate output.

### 5.3 Disposable acceptance

`streaming_test/` becomes eligible 24 hours after a terminal successful and stopped
record from an exclusive acceptance run. It is not evidence for deleting any other
checkpoint.

### 5.4 Retention anchor and clock validity

The retention anchor is the maximum of:

- terminal completion or retirement time;
- final lease heartbeat; and
- newest object `LastModified` under the exact prefix.

Object age alone never grants eligibility. A clock more than five minutes in the
future, an object modified after the accepted terminal record, a missing timestamp,
or contradictory clocks cause refusal. Legacy objects without valid ownership and
terminal metadata remain indefinitely ineligible for automated deletion.

## 6. Active lease contract

The control key is `_retention/leases/{checkpoint_id}.json`. Its canonical compact
JSON contains only typed, bounded fields:

- schema version and checkpoint ID;
- exact concrete checkpoint prefix;
- workload, query, run, and session identifiers;
- owner identity;
- random lease epoch UUID;
- whole-second UTC `acquired_at`, `heartbeat_at`, and `expires_at`;
- state: `active`, `stopped`, `completed`, or `retired`;
- optional bounded terminal source-offset, watermark, and sink-snapshot evidence.

The writer heartbeats every 60 seconds. A lease has a 10-minute TTL. Acquire,
renewal, terminal transition, and stale takeover use conditional S3 writes with the
observed ETag. A current active lease prevents another owner from acquiring the same
checkpoint and prevents retention.

Expiry alone does not mean stopped. An expired active lease is uncertain and causes
refusal until an operator performs a reviewed, ETag-bound terminal transition or
break-glass recovery. Duplicate, malformed, foreign-prefix, future-dated, or
condition-failed leases also cause refusal.

## 7. Dry-run, tombstone, and deletion protocol

### 7.1 Dry run

Dry run is the default and performs zero S3 writes. It reads only the exact registry
entry, control record, and checkpoint prefix. It emits a canonical local plan and
SHA-256 containing the policy digest, concrete prefix, decision, refusal reasons,
retention anchor, inventory count, total bytes, newest timestamp, and exact inventory
digest. Credentials, raw endpoints, and arbitrary object payloads are excluded.

### 7.2 Apply

Apply requires the exact reviewed dry-run SHA and a dedicated maintenance
credential. It follows this sequence:

1. reparse the same policy and plan and reject drift;
2. conditionally create immutable
   `_retention/tombstones/{operation_id}/prepared.json`;
3. wait a 15-minute quiescence interval;
4. recheck the lease, lifecycle, clocks, concrete prefix, policy SHA, object count,
   total bytes, newest timestamp, and exact inventory digest;
5. delete only the keys enumerated by the approved plan, in batches of at most 1,000;
6. relist the exact prefix and require it to be empty; and
7. create an immutable `completed`, `partial`, or `refused` audit result.

A changed inventory, renewed lease, changed policy, clock ambiguity, or new object
refuses deletion. A partial failure preserves the primary error and records only the
original exact key set. A retry may target remaining keys from that set; it may not
relist and broaden the operation.

Tombstones and audits are evidence, not rollback. The checkpoint-retention tool
never deletes its `_retention/` controls.

### 7.3 Bounds

One operation is limited to:

- 100 listing pages;
- 100,000 objects;
- 10 GiB of objects;
- 1,000 keys per delete request; and
- 15 minutes of active operation time, excluding the fixed quiescence interval.

Larger state refuses for explicit operator decomposition and review. Canonical
summaries are at most 64 KiB and inventory manifest shards at most 1 MiB. Responses,
logs, diagnostics, and exceptions are bounded and redact credentials, raw endpoints,
headers, and object payloads.

## 8. Authorization and security boundary

Issue #86 must run with a parent-owned checkpoint-maintenance service account, not
MinIO root, Spark, Iceberg, Jupyter, or notebook credentials. Its policy allows:

- `ListBucket` only for the five exact owned prefix families and `_retention/`;
- `GetObject` for owned checkpoint and control objects;
- `DeleteObject` only for exact owned checkpoint objects; and
- `PutObject` only for lease, tombstone, and audit controls.

It has no access to another bucket or the bucket root. Secrets enter only through an
environment or credential-binding boundary and never appear in plans, logs, metrics,
or documentation.

If the pinned MinIO release cannot enforce prefix conditions, issue #86 must fail
closed, leave scheduling disabled, and record the exact capability gap. An
application allowlist backed by bucket-wide delete credentials is not described as
least privilege.

The broader Spark Connect root-credential limitation is upstream Atlas scope. Issue
#85 documents it; neither #85 nor #86 modifies Atlas internals or claims that the
maintenance credential removes permissions held by existing workloads.

## 9. Recovery and break glass

Deleting a checkpoint is coordinated source-and-sink recovery, never isolated file
cleanup.

For a durable stream, the operator must stop the producer and query, prove source
offset availability, preserve a sink snapshot or approve a sink reset, select a new
versioned checkpoint prefix, replay, and verify source/sink correctness before the
old checkpoint can leave quarantine. If broker history or sink recovery evidence is
unavailable, deletion remains permanently ineligible.

Deleting `events/` or `event_windows/` and restarting from `startingOffsets=earliest`
can duplicate append output. The CDC MERGE may converge some keys, but broker
retention, event ordering, and corrections can alter results; it is not assumed safe.

For GH Archive, recovery re-resolves the exact still-published immutable generation,
resets `lakehouse.bronze.gh_events_stream`, and reruns that generation. A current
resolver pointer to another generation is not recovery evidence.

Break glass remains manual and unscheduled. It requires a reviewed policy exception,
typed exact prefix confirmation, the dry-run SHA, source and sink rebuild evidence,
and explicit repository-owner authorization. Bucket-root, unknown-prefix, and
control-prefix deletion remain forbidden even in break glass.

## 10. Audit and metrics contract

Issue #86 will emit deterministic canonical audit results with operation ID, actor,
checkpoint ID, prefix digest, policy SHA, plan SHA, timestamps, object and byte
counts, decision, refusal codes, request counts, and primary outcome. Audit records
are append-only for this tool and retained until a separate approved audit-retention
policy exists.

Metrics are low-cardinality and contain no object keys or credentials:

- checkpoint objects and bytes by checkpoint ID;
- eligible bytes;
- last heartbeat age;
- refusals by reason;
- planned and deleted objects and bytes; and
- partial operation and request failure counts.

## 11. Issue #85 deliverables

Issue #85 is deliberately non-networked and non-destructive. It delivers:

1. the strict version-1 canonical registry;
2. a strict parser and validator;
3. a pure decision evaluator over supplied inventory, lease, and clock values;
4. exact tests for policy and decision semantics;
5. synchronized repository, generated-site, and wiki documentation;
6. warnings in all four streaming notebook pairs and the go-live scratch path; and
7. a documented replacement requirement for the existing exclusive-stack test
   reset helper.

The implementation contains no boto3/MinIO mutation, no delete API, no Airflow DAG,
no credentials, no scheduler, and no import-time network access. A live check, if
used, is bounded and read-only.

## 12. Issue #86 boundary

Issue #86 alone owns:

- networked MinIO inventory and conditional control writes;
- dedicated prefix-scoped credential provisioning and capability proof;
- dry-run/apply CLI behavior and bounded deletion;
- lease integration for supported streaming starts and stops;
- tombstone, partial retry, audit, and metrics persistence;
- replacement or hardening of the current root-credential test reset;
- disposable-fixture live acceptance; and
- scheduling, only after the complete live gate passes and explicit enablement is
  reviewed.

Its live gate must prove dry-run zero mutation, active and uncertain refusal, exact
eligible-fixture deletion, unrelated-object preservation, changed-inventory refusal,
partial-retry confinement, bounded audit and metrics, credential scope, and
volume-preserving teardown. It must not use production checkpoint state as a fixture.

## 13. Testing and acceptance for issue #85

The issue is accepted when:

- every executable lab checkpoint location and `streaming_test/` maps exactly once;
- stale and upstream examples are explicitly non-owned and fail closed;
- parser tests reject duplicate or unknown fields, invalid exact integers, unsafe
  paths, prefix overlap, invalid templates, clocks, classes, states, and recovery
  rules;
- evaluator tests cover active, expired-but-unterminated, stopped, retired,
  quarantine, generation identity, disposable, legacy, unknown, root, control,
  future-clock, changed-inventory, and partial-state inputs;
- every entry documents exact source, sink, ownership, recovery consequence, and
  retention rule;
- active or uncertain checkpoints always remain ineligible;
- the current reproducibility reset is labeled exclusive-test-only and unsafe for a
  shared environment, with replacement assigned to #86;
- scheduling remains absent;
- canonical repository docs, generated site, wiki, scenario pages, notebook index,
  execution matrix, lakehouse guide, and go-live runbook agree; and
- focused and full offline tests, lint, verifier, strict docs, wiki, compose, and diff
  gates pass without modifying the protected plan, `uv.lock`, dataset registry,
  Atlas gitlink or source, persistent volumes, or `graphify-out/`.

## 14. Out of scope

- Any checkpoint deletion or mutation.
- An Airflow maintenance DAG or schedule.
- MinIO lifecycle configuration, bucket versioning, or Object Lock.
- Atlas source or credential-model changes.
- Redpanda topic retention tuning.
- Rewriting the four educational streaming pipelines as production applications.
- Implementing issue #86.
