# 8.8. Streaming Checkpoint Retention

This runbook is the operator-facing projection of
`checkpoints/retention-policy.yaml`. Issue #85 defines ownership and computes
eligibility from supplied facts. It is deliberately non-networked: the policy module
cannot contact MinIO, performs zero S3 writes, and does not delete checkpoints.
Networked inventory, leases, tombstones, deletion, metrics, and scheduling belong to
issue #86; scheduling remains disabled until that implementation passes its live gate.

## 1. Owned checkpoints

| ID | Exact prefix or leaf | Owner | Class | Retention | Recovery consequence |
|---|---|---|---|---:|---|
| `streaming-events-v1` | `events/` | Streaming Data Engineering | durable stream | 30-day quarantine after reviewed retirement | Prove topic offsets and sink snapshot/reset; a fresh checkpoint can duplicate append output. |
| `streaming-event-windows-v1` | `event_windows/` | Streaming Data Engineering | durable stream | 30-day quarantine after reviewed retirement | Prove topic offsets and sink snapshot/reset; a fresh checkpoint can duplicate append output. |
| `streaming-online-retail-cdc-v1` | `online_retail_cdc/` | Streaming Data Engineering | durable stream | 30-day quarantine after reviewed retirement | CDC replay is not assumed safe; review broker retention, event order, corrections, and sink recovery. |
| `streaming-gh-archive-file-v1` | `gh_events_file/{scale}/{publication_id}/{manifest_sha256}/` | Streaming Data Engineering Education | generation reproducibility | 14 days after completed/stopped | Re-resolve the exact generation and reset `lakehouse.bronze.gh_events_stream` before replay. |
| `go-live-streaming-test-v1` | `streaming_test/` | Lab Acceptance Engineering | disposable acceptance | 24 hours after a successful, stopped, exclusive run | Recreate the bounded fixture and its scenario-owned sink. |

`gh_events_file/` is a family root, not an eligible target. `events_stream/` is
stale prose, and `redpanda/atlas_stream_events/` is an upstream Atlas example. Root,
control, unknown, legacy, malformed, or overlapping prefixes remain unowned and are
retained.

For each durable entry, `retired_at` and `retirement_review` are explicit registry
fields. Both are `null` while `lifecycle: active`. A reviewed transition changes all
three fields together: `lifecycle: retired`, an exact whole-second UTC `retired_at`,
and a bounded reviewed-change identifier. A terminal record must repeat that exact
identifier. An old stopped timestamp cannot substitute for the registry retirement
clock.

## 2. Lease and active-use safety

Issue #86 will store leases at
`_retention/leases/{checkpoint_id}.json`. A writer heartbeats every 60 seconds;
the lease has a 10-minute TTL, and acquire, renewal, takeover, and terminal
transition are ETag-conditional. Expiry alone never proves stopped. An expired
active lease is operationally uncertain until a reviewed, ETag-bound terminal
transition or break-glass recovery establishes the state.

The retention anchor is the maximum of the terminal or retirement time, final lease
heartbeat, and newest object `LastModified`. A clock more than five minutes in the
future refuses evaluation. An object newer than the terminal record also refuses;
object age alone never grants eligibility.

The local YAML boundary reads at most 262,144 bytes and rejects aliases, more than
4,096 composed nodes, or nesting deeper than 32 nodes before policy construction.
Supplied facts are exact runtime types: Boolean evidence is never interpreted by
truthiness, hashes and ETags must be strings, and all clocks are whole-second UTC.
Lease clocks must satisfy `acquired_at <= heartbeat_at <= expires_at`, with
`expires_at - heartbeat_at = 600 seconds`. The lease and terminal states must be the
same class-approved state: durable `stopped|retired`, generation
`completed|stopped`, and disposable `stopped`.

## 3. Dry-run and apply contract

Dry run is the default and makes zero S3 writes. Its canonical compact local JSON
contains the policy SHA-256, concrete prefix, decision, refusal codes, retention
anchor, inventory count and bytes, newest timestamp, and inventory digest. The plan
SHA-256 binds apply to those exact facts without exposing credentials, raw endpoints,
headers, object payloads, or arbitrary key names.

Issue #86 must conditionally create an immutable prepared tombstone, wait a
15-minute quiescence interval, and recheck the lease, clocks, policy, prefix, count,
bytes, newest timestamp, and exact inventory digest. It may then delete only the
enumerated keys and must prove the prefix is empty. Changed state refuses. A partial
retry is confined to the original remaining key set and cannot broaden by relisting.

One operation is bounded to 100 listing pages, 100,000 objects, 10 GiB, 1,000 keys
per delete request, and 15 minutes of active operation time excluding quiescence.
Canonical summaries are at most 64 KiB; inventory manifest shards are at most 1 MiB.

### Deterministic supplied-fact examples

An eligible retired durable example uses registry lifecycle `retired`,
`retired_at=2026-07-01T12:00:00Z`, and
`retirement_review=issue-85-reviewed-transition`. At
`evaluated_at=2026-08-01T12:00:00Z`, its exact `retired` lease has acquire,
heartbeat, and expiry clocks `2026-07-01T11:50:00Z`,
`2026-07-01T12:00:00Z`, and `2026-07-01T12:10:00Z`; its terminal record is
`retired` at `2026-07-01T12:00:00Z`, repeats the review identifier, and has all
three recovery approvals set to the Boolean `true`. Its inventory has a valid
SHA-256, positive bounded count/bytes, newest object time no later than the terminal,
Boolean `changed_since_plan=false`, and Boolean `partial_retry_confined=true`. The
retention anchor is `2026-07-01T12:00:00Z`, the eligible-after clock is
`2026-07-31T12:00:00Z`, and the refusal list is empty.

A deterministic refusal changes that same registry back to `lifecycle: active`,
with `retired_at=null` and `retirement_review=null`, and supplies a conflicting lease
plus a changed inventory. The evaluator retains all applicable audit reasons in
order—`lease_conflicting`, `inventory_changed`, and
`registry_active_durable`—and emits `decision=refused`. Active durable status does
not erase the other malformed or contradictory supplied-fact diagnostics.

## 4. Authorization and fail-closed deployment

Issue #86 uses a dedicated checkpoint-maintenance service account, never MinIO root,
Spark, Iceberg, Jupyter, or notebook credentials. Its access is limited to listing the
five owned prefix families and `_retention/`, reading owned state, deleting only exact
owned checkpoint objects, and writing only lease, tombstone, and audit controls.

If the pinned MinIO release cannot enforce prefix-scoped authorization, deployment
fails closed and scheduling remains disabled. An application allowlist combined with
bucket-wide delete credentials is not least privilege.

## 5. Recovery procedures

### Durable streams

1. Stop the producer and query and record a reviewed retirement transition.
2. Prove source offsets remain available.
3. Preserve a sink snapshot or approve a complete sink reset.
4. Select a new versioned checkpoint prefix, replay, and verify source/sink results.
5. Keep the old checkpoint through its full 30-day quarantine.

If source history or sink recovery evidence is unavailable, the checkpoint remains
ineligible indefinitely.

### GH Archive generation

Re-resolve the same scale, publication ID, and manifest SHA-256. A current pointer to
another generation is not evidence. Reset the shared stream sink, replay exactly that
immutable generation, and verify results before the old exact leaf can expire.

### Disposable acceptance

Only an exclusive successful and stopped run qualifies. Recreate the bounded input and
sink when needed. Disposable status never transfers to another prefix.

## 6. Break glass

Break glass is manual, reviewed, and unscheduled. It requires an exact typed prefix,
the dry-run SHA, source and sink rebuild evidence, a policy exception, and repository-
owner approval. Bucket-root, unknown-prefix, family-root, and `_retention/` deletion
remain forbidden. Tombstones and audits are evidence, not rollback.

## 7. Audit and monitoring boundary

Issue #86 records immutable operation ID, actor, checkpoint ID, prefix digest, policy
and plan SHAs, timestamps, object/byte/request counts, decision, refusal codes, and
primary outcome. Low-cardinality metrics cover owned object/byte counts, eligible
bytes, heartbeat age, refusal reasons, planned/deleted counts, and partial failures.
Object keys, credentials, endpoints, headers, and payloads are excluded.

## 8. Current acceptance boundary

Issue #85 is accepted entirely through strict offline parser, evaluator, repository
ownership, parity, and documentation tests. It adds no Airflow DAG or schedule and
does not mutate persistent volumes. Issue #86 must use disposable fixtures to prove
credential scope, dry-run zero mutation, active/uncertain refusal, exact deletion,
unrelated-object preservation, changed-inventory refusal, bounded audits, partial
retry confinement, and volume-preserving teardown before scheduling can be reviewed.
