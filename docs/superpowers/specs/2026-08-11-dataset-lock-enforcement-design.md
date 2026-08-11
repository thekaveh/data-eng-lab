# Dataset Lock Enforcement Design

**Date:** 2026-08-11

**Issue:** [#81](https://github.com/thekaveh/data-eng-lab/issues/81)

**Parent:** [#79](https://github.com/thekaveh/data-eng-lab/issues/79)

**Dependency:** [#80](https://github.com/thekaveh/data-eng-lab/issues/80)

**Status:** Approved direction; written specification pending final review

## 1. Objective

Enforce the version-2 dataset provenance lock at every runtime boundary:
download, archive extraction, deterministic generation, publication, existing
object reuse, and consumer resolution. A successful run must prove that every
byte and physical schema selected by a dataset scale matches the committed
registry. A failed run must leave the previously active verified generation
available.

This design replaces existence-based reuse and flat multi-object replacement
with immutable generations plus one atomic active pointer. It also
defines the reader migration required for the word *atomic* to remain true
under concurrent access.

## 2. Decision and rejected alternatives

### 2.1 Selected: immutable generations and an atomic pointer

Each selected dataset plan is published beneath an immutable generation
prefix. After every object in that generation has been verified locally and
remotely, one conditional `PUT` changes the dataset's active pointer. Readers
resolve its immutable manifest once and use only paths from that generation.
The former active generation remains intact for readers that resolved it before
the switch and for rollback.

This is the only selected approach that prevents a reader from observing a
mixture of two scales or releases without requiring an external maintenance
window.

### 2.2 Rejected: serialized replacement of flat keys

An exclusive writer lease, staging area, rollback log, and maintenance window
could make flat-key replacement crash-recoverable. It cannot make a sequence of
S3 `PUT` and `DELETE` operations atomic to an unsynchronized reader. It would
therefore weaken the atomic-replacement requirement approved in #80.

### 2.3 Rejected: overwrite each object after validating it

Object-by-object replacement is simple but exposes mixed releases, strands
stale objects when a scale shrinks, and cannot recover an interrupted
multi-object update without reader-visible inconsistency.

## 3. Logical and physical identities

The version-2 registry remains the single source of truth. Its
`landing_prefix` and `object_name` fields continue to define stable logical
identities. Runtime publication maps those logical identities to physical S3
keys:

```text
s3://landing/<landing_prefix>/_generations/<plan_id>/<publication_id>/<object_name>
```

Control objects live outside dataset data prefixes:

```text
s3://landing/_data-eng-locks/current/<dataset>.json
s3://landing/_data-eng-locks/manifests/<dataset>/<manifest_sha256>.json
s3://landing/_data-eng-locks/leases/<dataset>.json
```

No data reader may glob `_data-eng-locks` or infer a physical key directly.
Generation objects never overwrite another generation.

`plan_id` (also exposed as `selected_plan_sha256`) is the lowercase SHA-256 of
canonical JSON containing:

- registry schema version and global lock policy;
- dataset identifier and selected scale;
- effective dataset and artifact provenance;
- the selected source or generator contract;
- every selected logical object name, size, SHA-256, schema identifier, and
  schema fingerprint;
- the publication-layout version.

Every plan, manifest, pointer, lease, and resolution document uses
`datasets.locking.canonical_json`: UTF-8, keys sorted recursively by Python's
JSON encoder, separators `,` and `:` with no insignificant whitespace,
non-ASCII preserved, list order preserved, no trailing newline, and only
registry-validated finite JSON numbers. No other serializer may produce hashed
or compared control bytes.

The full 64-character digest is used. Volatile retrieval timestamps and S3
metadata are excluded. Equal selected contracts therefore produce the same
plan identity.

`publication_id` is `uuid.uuid4().hex`: exactly 32 lowercase hexadecimal
characters. It is also the publisher transaction identifier for that one
immutable upload attempt. It prevents an incomplete or corrupt orphan from an
interrupted attempt from occupying the deterministic generation namespace.
Neither a publisher nor a recovery run overwrites a publication instance. The
lease owner nonce is a separate 128-bit random lowercase hexadecimal value.

This design explicitly supersedes #80's flat physical interpretation of
`landing_prefix/object_name`. The registry `object_name` remains the stable
logical name relative to `landing_prefix`; only the runtime resolver constructs
the generation-qualified physical key.

## 4. Immutable manifest history and active pointer

Every publication first writes an immutable canonical UTF-8 JSON manifest to
the content-addressed history key shown above using `If-None-Match: *`. The
manifest contains:

- dataset identifier and selected scale;
- the raw whole-registry SHA-256 as audit evidence;
- the canonical selected-plan SHA-256 as the runtime correctness identity;
- plan identifier, publication identifier, and physical prefix;
- ordered objects with logical name, physical key, byte size, SHA-256, schema
  identifier, and schema fingerprint;
- publication identifier and publication timestamp;
- the preceding immutable manifest key and SHA-256, when one was active.

The manifest must exactly match the selected registry plan and the immutable
objects it names. Unknown fields fail closed. URLs, credentials, local paths,
and temporary staging locations are forbidden. The manifest key digest is
recomputed from its bytes on every resolution.

The mutable active pointer is a separate minimal canonical JSON object. It
contains only its format version, dataset identifier, immutable manifest key,
and manifest SHA-256. Overwriting the pointer never destroys manifest history.
An unrelated registry edit does not invalidate an active dataset when the
canonical selected-plan digest and global lock policy remain exact; the
whole-registry digest is retained only for audit traceability.

The pointer changes with a conditional `PUT`:

- first publication requires `If-None-Match: *`;
- replacement requires `If-Match` with the ETag read before staging;
- a failed precondition reports a concurrent publisher and performs no retry
  against an unreviewed state.

The implementation must prove this MinIO behavior in the live acceptance test.
If the configured object store cannot provide conditional single-key writes,
publication fails as unsupported rather than falling back to a non-atomic
sequence.

Explicit rollback selects an immutable historical manifest, verifies its
digest, selected-plan identity, complete remote bytes, and schemas, then changes
the pointer with the same `If-Match` rule. A rollback remains valid after an
unrelated registry edit because its selected plan is unchanged. If the selected
dataset contract changed, rollback fails until that registry change is itself
reviewed and reverted; historical bytes never override the current lock.

## 5. Writer lease and crash safety

The pointer compare-and-swap is the correctness boundary. A short-lived,
dataset-scoped writer lease additionally avoids duplicate expensive downloads
and generation. A missing lease is created with `If-None-Match: *`. An expired
lease is taken over only with `If-Match` against its observed ETag. The lease
contains the owner nonce, publication identifier, server-derived creation and
expiry times, and state. S3 response `Date` is the clock authority; a missing
value or an absolute difference greater than 300 seconds from local UTC fails
closed as implausibly skewed. Local monotonic time measures the five-second
request window and lease work elapsed time.

Each acquire or renewal begins with a `GET`/`HEAD` whose response `Date` is the
authoritative instant used to construct the proposed body. The conditional
write must begin within five monotonic seconds of that response or restart from
a fresh read. A missing lease is created with `If-None-Match`; a `released`
lease is acquired immediately with `If-Match`; an `active` lease is taken over
with `If-Match` only when that server-derived instant is at or after its expiry.
The successful write response's `Date` must fall between the proposed creation
and expiry times; otherwise the writer reconciles the ambiguous write and then
fails closed.

Renewal uses `If-Match` against the owner's latest lease ETag and returns the
new ETag. Release is a conditional `PUT` to a released/expired state rather
than an unconditional delete. A stale owner therefore cannot renew, release,
or delete its successor's lease. Losing the lease aborts candidate work before
pointer mutation. The lease never grants permission to bypass the pointer
precondition.

Publication follows this state machine:

1. Load and validate the registry and selected plan; read the pointer bytes and
   ETag independently of whether the pointer body is valid.
2. Resolve the immutable manifest and verify the complete active generation
   when the pointer is valid.
3. If it is exact and refresh was not requested, return idempotent success.
4. Acquire the dataset writer lease and re-read the pointer plus ETag.
5. Acquire or generate the complete candidate in an owned temporary directory.
6. Verify raw bytes, archive structure, output names, output bytes, and schemas.
7. Upload with `If-None-Match: *` only to the candidate's unique immutable
   publication prefix, with lock-bound metadata.
8. Stream every remote candidate object back and verify size and SHA-256.
9. Write and re-read the immutable content-addressed manifest with
   `If-None-Match: *`.
10. Write the active pointer using the original pointer precondition.
11. Release the lease. Manifest history and prior generations remain retained.

An interruption before step 9 leaves the prior pointer unchanged. Candidate
objects are harmless orphans and are never completed by overwriting them; a
later refresh uses a new publication identifier. An interruption after step 9
but before step 10 leaves an unreferenced immutable manifest and a complete
candidate. An interruption after step 10 leaves a complete active generation.
Lease expiry does not change the pointer.

A conditional write whose response is lost or times out has an ambiguous
outcome and is reconciled by reading, never by blind retry:

- an object or immutable-manifest write is successful only when a subsequent
  `GET` proves the exact intended bytes and lock metadata at the intended key;
- a pointer write is successful only when a subsequent `GET` proves that it
  references the exact intended immutable manifest;
- a lease write is successful only when the reread owner nonce, transaction,
  state, and ETag are the exact intended successor values;
- a differing value is a genuine conflict, while an absent value is a failed
  write; neither case retries a pointer CAS with the stale ETag.

The same reconciliation follows an unexpected `409` or `412` when the client
cannot prove whether the server committed its request. Exact self-reconciliation
is reported separately from a competing writer.

## 6. Desired-state and refresh semantics

The desired state is exactly the object set selected by one dataset and scale.
Scale changes do not delete or overwrite another scale's bytes; the new active
manifest simply names the new generation. Stale keys from a previous
generation are unreachable through the active pointer and immutable manifest.

CLI behavior is:

- default: verify the active pointer, immutable manifest, and every referenced
  object; return without source requests, uploads, or deletes when exact;
- `--verify-only`: perform the same complete verification but never acquire,
  generate, upload, repair, or switch a pointer;
- `--refresh`: reacquire or regenerate the complete selected plan, verify it,
  publish its immutable generation, and conditionally switch the pointer;
- `--rollback-manifest <sha256>`: verify a retained manifest against the current
  selected plan and conditionally repoint without acquiring new source bytes;
- `--force`: retained as a deprecated compatibility alias for `--refresh`; it
  never bypasses a lock or verification;
- `--dry-run`: resolve and validate the plan, read current S3 state, then
  describe the intended generation and pointer action without any S3 mutation
  or upstream source request.

`--verify-only`, `--refresh`, `--force`, and `--rollback-manifest` are mutually
exclusive actions; omitting all four selects default behavior. `--dry-run` is
an orthogonal no-mutation modifier for default, refresh/force, or rollback, but
not verify-only because verify-only is already non-mutating. Rollback requires
one explicit `--only <dataset>`, one explicit `--scale`, and one manifest
digest. Refresh/force and default/verify-only retain repeated `--only` support.

A missing active pointer makes default mode perform initial publication. A
corrupt pointer, corrupt manifest, incomplete generation, or selected-plan
mismatch fails closed in default and verify-only modes. Intentional refresh may
replace a corrupt pointer only by preserving its raw ETag and using `If-Match`
after a complete new publication verifies; it never trusts fields from the
corrupt body. Refresh never edits `datasets/registry.yaml` and never accepts
bytes that differ from it.

When no active pointer exists but legacy flat objects are present beneath the
dataset's historical `landing_prefix`, initial publication first streams and
verifies the exact selected legacy set, including physical schemas. An exact
set may be migrated without an upstream source request by downloading into the
owned staging area and publishing a new immutable instance. A missing, extra,
or mismatched legacy set is reported and is never silently copied, deleted, or
partially repaired; intentional refresh acquires the complete selected plan.
Legacy discovery uses a paginated listing and considers only exact top-level
historical keys whose suffix after `landing_prefix/` contains no `/`. The
reserved `_generations/` subtree and control namespace are excluded. Comparison
uses the registry's complete selected logical-name set; a server-side copy
cannot replace the required streamed download, schema check, immutable upload,
and remote re-verification.

Unknown objects outside `_data-eng-locks` and generation prefixes are not
deleted. Unknown objects inside a candidate generation make that generation
invalid. Garbage collection is deliberately not implemented by #81. The CLI
reports orphan and retained history, while an operator runbook documents that
manual removal is allowed only after proving an object is unreachable from
every immutable manifest. Automatic destructive cleanup is deferred to a
separate reviewed issue.

Existing `--scale` and repeated `--only` selection remain supported. Each
dataset has its own pointer and transaction, so publication is atomic per
dataset, not across a multi-dataset command. The command processes datasets in
registry order, reports each result independently, and returns nonzero if any
selected dataset fails.

`DATASET_SCALE` is the canonical run-scoped consumer input and accepts only
`tiny`, `small`, or `medium`. An explicit CLI/API/notebook parameter has highest
precedence, then `DATASET_SCALE`; launchers use the documented default `small`
only when neither was supplied. The resolved JSON always records the effective
scale. `make datasets SCALE=...`, notebook reproducibility, scenario launchers,
Airflow run configuration, and smoke tests propagate that same value. No
consumer derives its expected scale from the active pointer or hard-codes
`small` internally.

## 7. Verification model

### 7.1 Local and remote bytes

All verification streams bytes through one bounded size-and-SHA-256 routine.
The routine stops once the locked size is exceeded and distinguishes missing,
truncated, oversized, and digest-mismatched data.

S3 `HEAD`, ETag, and user metadata are never sufficient proof. Lock-bound
metadata is useful diagnostics and may avoid parsing a manifest, but correctness
requires a streamed `GET` and SHA-256 comparison for every active or candidate
object. A successful idempotent run therefore performs no upstream source
request and no S3 mutation, but it does read active S3 content.

Uploads include the plan ID, publication ID, logical object name, expected size,
and expected SHA-256 as metadata. The uploader verifies the remote stream
after upload before the object may enter an immutable manifest.

### 7.2 Physical schemas

Byte verification and schema verification are separate gates. Each produced or
reused object is inspected using its registry `SchemaContract`:

- Parquet: exact DuckDB 1.5.4 runs offline and one authoritative
  `normalize_parquet_schema` function projects root-column order, type, and
  repetition from `parquet_schema`; values are not scanned to infer a different
  schema;
- CSV: strict Python CSV parsing rejects a BOM, invalid UTF-8, malformed
  quoting, and inconsistent row width; the configured delimiter and header are
  exact and every value is validated against its declared logical type;
- JSONL-GZIP: gzip integrity and UTF-8 are strict; each nonblank line is one
  JSON object, dotted field names traverse nested mappings, and every record is
  checked across the complete stream;
- XLSX: after the same hardened ZIP preflight, a bounded standard-library OOXML
  reader streams workbook relationships, shared strings, styles, and worksheet
  XML; it requires exactly the declared visible worksheet set and header row,
  rejects DTD/entity declarations and every formula cell, and validates every
  value against the declared logical type;
- text: UTF-8 decoding and the required empty tabular-field contract.

Parquet normalization maps `BOOLEAN`; signed and unsigned 8/16/32/64-bit
integers; `FLOAT`/`REAL` and `DOUBLE`; `DATE`; timestamp seconds, milliseconds,
microseconds, and nanoseconds; timezone-adjusted timestamps; UTF-8
`BYTE_ARRAY`; unannotated binary; and `DECIMAL(P,S)` to the corresponding #80
logical vocabulary. Timestamp unit differences normalize to `timestamp`, while
the Parquet logical UTC/timezone annotation alone selects `timestamp-tz`.
`REQUIRED` maps to non-nullable and `OPTIONAL` to nullable. Repeated or nested
list/map/struct fields, unsupported physical types, missing annotations needed
to disambiguate string/binary or signed width, duplicate names, and decimal
precision/scale outside the registry vocabulary fail closed. Tests freeze this
mapping against representative Parquet metadata and every production schema.

For delimited text, JSON, and XLSX, empty CSV fields, absent JSON paths, JSON
`null`, and blank cells are null. Null is accepted only for nullable fields;
every non-nullable field must be present and non-null in every record. Integer
types accept only base-10 integral values in range. Float types accept finite
integer or decimal literals in range. Boolean accepts only actual booleans or
case-insensitive `true`/`false`. Date and timestamp values must be strict ISO
8601; `timestamp-tz` requires an explicit offset. Decimal precision and scale
are exact. String values must be valid text and are not numerically coerced.
JSON values must match their JSON logical kind. XLSX numeric cells may satisfy
an integer only when finite, integral, and in range; date-formatted cells map
only to date/timestamp contracts. Mixed incompatible values fail rather than
promoting the observed column.

`exact` requires the declared ordered field set and forbids undeclared fields.
`minimum` requires every declared field/path with the rules above but ignores
undeclared JSON fields; it does not weaken nullability or type checks. Header
order is significant for CSV and XLSX. JSON object member order is not.

The inspector recomputes the fingerprint of the declared contract to detect
registry corruption, then separately compares the observed projection and
every value to that contract. The fingerprint alone is never treated as proof
of observed bytes. Wrong-schema data fails even if a test fixture supplies a
matching ad-hoc byte digest.

Inspection is bounded and network-free. GZIP and OOXML decompression share the
archive limits, with a per-object expanded-byte ceiling of
`min(max(64 MiB, 200 * locked_size), 8 GiB)`. JSON nesting is limited to 64
levels and an individual decoded string or record to 16 MiB. Exceeding any
bound fails closed.

### 7.3 Storage and reader trust boundary

The supported threat model assumes MinIO storage and administrators are trusted
after verification and that only the compliant publisher mutates generation,
manifest, pointer, or lease keys. Dataset consumer code treats those keys as
read-only. Existing development credentials may be broader, so this design does
not claim protection from root credentials, malicious consumer code, disk
corruption after the gate, or a storage administrator. Application-level
content addressing and conditional writes prevent compliant publishers from
overwriting immutable keys. Out-of-band changes are detected by the next
complete verification and fail closed.

Before launching a dataset-dependent run, the bootstrap resolver streams and
schema-checks the complete resolved generation. After that gate, the run holds
one immutable manifest and relies on the storage trust boundary while Spark,
Trino, or a notebook reads its objects. This is the explicit TOCTOU boundary.
Backend version IDs or Object Lock may strengthen deployment policy later but
are not silently assumed by #81.

## 8. HTTP and archive acquisition

Production and audit code share one hardened transport/archive package. The
production downloader must not import private code from `scripts/`, and the
audit CLI must not maintain a divergent security policy.

HTTP acquisition:

- accepts only the already validated authoritative HTTPS URL;
- disables environment-provided proxies; a future proxy requires a separately
  reviewed explicit trust configuration;
- resolves A and AAAA records before each connection, rejects the hop unless
  every returned address satisfies the shared authoritative-public-IP policy,
  pins the chosen address for the connection, and preserves the original host
  for TLS SNI, certificate hostname verification, and the HTTP `Host` header;
- verifies the actual connected peer equals an approved resolved address;
- redoes URL, DNS, peer, TLS, and credential checks independently for every
  redirect, with a fixed redirect limit, preventing DNS rebinding and private
  redirect pivots;
- requests identity encoding and uses bounded raw reads with a monotonic
  deadline;
- redacts credentials and query values from diagnostics;
- writes exclusively created temporary files and verifies the raw size and
  digest before any extraction.

Archive handling applies the #80 audit preflight before constructing `ZipFile`:

- bounded member count, central-directory size, total expanded bytes, and
  compression ratio;
- no encrypted, unsupported, duplicate, ambiguous, absolute, traversal,
  control-character, symlink, hard-link, or special-file members;
- only safe empty structural directory entries;
- exact regular-file member multiset required by the selected artifact;
- no missing or extra regular files;
- exact `member_path` to `object_name` mapping with no flattened collision;
- extraction only to exclusively created owned paths, followed by byte and
  schema verification.

Direct artifacts similarly bind the authoritative URL basename, raw identity,
logical object name, size, digest, and schema.

## 9. Canonical TPC-H generation

Host DuckDB generation and runtime `INSTALL tpch` are removed from the
production path. `generate_tpch` consumes the typed `GeneratorContract` and
selected `GeneratorScale`, then uses the committed linux/amd64 Dockerfile and
exporter from #80.

The registry's image digest identifies the pinned linux/amd64 `FROM` manifest,
not the locally built final exporter image. The build proves that exact base
digest and platform, and `pip --require-hashes` proves the downloaded DuckDB
wheel archive before installation. The wheel archive need not remain in the
runtime image. Inside the container, the exporter re-verifies the copied
`uv.lock`, installed DuckDB version, installed TPC-H extension digest, locale,
timezone, thread count, export settings, scale factor, and eight output
contracts. The built image is labelled with the plan contract and inspected for
the canonical entrypoint and environment; no impossible comparison to a
precommitted final-image digest or discarded wheel archive is made.

Runtime generation uses `--network=none`. It creates files only in the owned
local staging mount; no S3 object is published until all eight local outputs and
the exporter metadata match the selected lock.

Docker or required linux/amd64 execution support is an explicit operational
prerequisite. Missing support fails with a diagnostic; the implementation does
not fall back to host generation.

## 10. Reader migration

Atomic publication is effective only when readers resolve the active pointer
and immutable manifest.

The reference implementation is the Python API:

```text
resolve_active_dataset(client, registry, dataset_id, expected_scale)
    -> ResolvedDataset
```

`scripts/resolve_dataset.py` exposes the same operation to host workflows. For
containers, the consumer overlay builds a pinned, internal-only
`dataset-resolver` image from the repository and `uv.lock`. It contains the
registry, resolver package, boto3, DuckDB, and XLSX dependency once, has no host
port, and exposes a read-only `POST /v1/resolve` operation on the compose
network. Its request is exactly dataset plus expected scale; its successful
body is the same canonical resolution JSON. The image healthcheck performs no
S3 mutation.

Inside containers the resolver uses the compose-network MinIO endpoint and
credentials injected through the consumer overlay; the host CLI uses the
exported host endpoint. Endpoint and credential values never enter resolution
documents or logs. Airflow calls the resolver only from task execution after
the DAG is parsed. DAG module import performs no DNS, HTTP, S3, or resolution
work.

The caller must provide an expected scale; accepting whichever scale is active
is forbidden. The result contains dataset, scale, selected-plan SHA-256,
immutable manifest SHA-256, plan/publication identifiers, and the
registry-ordered tuple of logical names, immutable S3 URIs, sizes, digests, and
schema identifiers. Failure returns no partial URI list.

Resolution validates pointer and immutable manifest structure, exact
dataset/scale identity, selected-plan SHA-256, generation prefix, ordered object
mapping, every remote byte stream, and every physical schema before returning.
One result is cached only inside the requesting process/run and is never reused
across runs. A later pointer switch cannot mix generations because the run
retains the immutable manifest and URI tuple it already verified.

All current direct landing consumers are in scope:

- Jupyter/PySpark and Zeppelin/Scala notebooks call the internal resolver in a
  bootstrap paragraph and retain its run-scoped canonical JSON result;
- both NYC Taxi Airflow DAGs resolve before Spark submission and pass immutable
  URI arguments from task execution, never module import;
- both NYC Taxi Spark applications stop embedding flat landing prefixes and
  require those arguments;
- `scripts/bronze_smoke.py`, scenario bootstrap, notebook reproducibility, and
  generated scenario templates resolve expected scales rather than constructing
  keys;
- Trino DDL is created from resolved immutable locations, never from a dataset
  root glob;
- every NYC Taxi, GH Archive, MovieLens, Online Retail, and TPC-H Jupyter or
  Zeppelin notebook discovered by the repository-wide path inventory is
  migrated, including all TPC-H `.parquet` names.

The compose overlay adds only the consumer-owned resolver service and endpoint
configuration needed by Airflow, JupyterHub, and Zeppelin; Atlas source remains
untouched. Container-level smoke tests call it from all three services, and a
DAG-import regression proves zero resolver/S3 access. Tests also keep an exact
inventory of runtime files containing legacy flat landing paths and fail until
that inventory is empty. Documentation and diagrams may show logical prefixes
but must not teach a flat physical read path.

General `make up` remains service-only and does not download large datasets.
Dataset-dependent scenario/bootstrap entry points run a read-only manifest
verification gate before starting work. Missing or invalid active data produces
an actionable `make datasets ...` instruction.

## 11. Diagnostics and errors

Verification failures are typed and include dataset, scale, artifact or
generator output, logical object, stage, expected value, and actual value.
Diagnostics distinguish registry invalidity, source drift, archive-policy
failure, schema mismatch, generator drift, S3 corruption, concurrent publish,
and unsupported runtime capability.

Errors never print credentials, signed query strings, full response bodies,
temporary paths outside an owned root, or secret-bearing environment values.
All candidate mismatch paths stop before pointer mutation. Cleanup removes only
transaction-owned temporary files and never deletes foreign S3 objects.

## 12. Implementation boundaries

Expected core components are:

- `datasets/verification.py`: byte, set, schema, and structured mismatch types;
- `datasets/acquisition.py`: shared bounded HTTP and ZIP primitives;
- `datasets/publication.py`: generation IDs, manifests, S3 transactions,
  leases, compare-and-swap, and active resolution;
- `datasets/resolver_service.py` and `datasets/resolver.Dockerfile`: pinned
  internal read-only cross-language resolution service;
- `datasets/sources/http.py`: typed artifact acquisition;
- `datasets/sources/tpch.py`: canonical container generation;
- `datasets/s3.py`: streamed verification, metadata, immutable upload, and
  conditional manifest operations;
- `scripts/download_datasets.py`: CLI orchestration and modes;
- consumer helpers and every direct consumer discovered by repository-wide
  path inventory.

#81 does not change the root `uv.lock`, whose SHA-256 is part of the committed
TPC-H generator contract. The resolver image installs only dependencies already
present in that lock. Any future third-party schema dependency requires either
a separate hash-locked resolver environment or a reviewed provenance-lock
update with complete TPC-H regeneration and evidence refresh.

The implementation must not modify Atlas source. The `infra` gitlink remains
exactly `c6cf73d7168db1a7840fc45c9ed3e385071996d8`. The unrelated untracked Atlas
modernization plan remains untouched with SHA-256
`f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`.

## 13. Test strategy

Every behavior change follows RED then GREEN TDD. Required focused coverage
includes:

- missing, truncated, oversized, and wrong-digest local and remote bytes;
- trustworthy-looking metadata with corrupt content;
- direct-download URL/name/size/digest/schema mismatches;
- redirect and peer-address policy, timeouts, bounds, and redaction;
- missing, extra, duplicate, unsafe, encrypted, special, colliding, truncated,
  and resource-exhausting ZIP members;
- valid structural directory entries and official archive layouts;
- wrong physical schemas for every supported format;
- XLSX shared-string/style/date handling plus formula, DTD/entity, worksheet,
  header, cell-type, and expansion-limit rejection;
- TPC-H image, platform, dependency, environment, parameter, metadata, name,
  size, digest, schema, and eight-output-set drift;
- first publish, exact reuse, legacy flat-object migration, refresh, scale
  transition, orphan candidate isolation, pointer compare-and-swap conflict,
  corrupt/missing active objects, two writers on one contract, lease expiry
  during upload, interrupted upload, interrupted manifest write, interrupted
  pointer switch, and rollback retention;
- immutable history and rollback after scale change, unrelated registry change,
  selected-plan change, corrupt current pointer, and concurrent publish;
- lease create/takeover/renew/release ETag behavior, owner nonce, server clock,
  stale-owner ABA attempts, and lost-lease abort;
- lost-response reconciliation for generation object, immutable manifest,
  active pointer, and lease writes, distinguishing exact self-commit from a
  competing value;
- concurrent reader consistency before and after pointer change;
- CLI default, verify-only, refresh, force alias, dry-run, diagnostics, and
  no-mutation failure paths;
- every migrated consumer resolving one immutable generation per run;
- tiny, small, and medium propagation through `DATASET_SCALE`, explicit
  overrides, Airflow run configuration, notebook bootstrap, scenario launch,
  smoke tests, and the documented default;
- a real MinIO acceptance test proving conditional pointer semantics,
  post-upload verification, idempotence, corruption detection, and recovery;
- adapter tests preserving quoted opaque ETags and mapping first-publish and
  replacement races, `409 Conflict`, and `412 PreconditionFailed` without an
  unsafe automatic retry; Moto is not evidence of MinIO conditional semantics;
- canonical network-disabled linux/amd64 TPC-H generation at a bounded scale;
- resolver image build plus Airflow, JupyterHub, and Zeppelin network smokes,
  and import-isolated DAG parsing with zero resolver/S3 access;
- documentation source, MkDocs, and wiki parity.

Full completion also requires lint, the complete offline suite, repository
verification, strict documentation build, wiki synchronization, diff checks,
registry/evidence parity, Atlas cleanliness, and the protected-plan checksum.

## 14. Documentation and lock updates

`docs/datasets.md` remains the canonical public surface. README, Getting
Started, Go-live, Changelog, generated MkDocs, and wiki surfaces must explain:

- verified immutable generations, immutable manifest history, and active pointers;
- default verification, verify-only, refresh, and deprecated force behavior;
- failure and recovery procedures;
- retained generations, orphan reporting, and the deferred manual-removal
  safety rule;
- intentional lock refresh through the #80 audit/review workflow;
- the rule that runtime mismatch never rewrites the registry or blesses new
  bytes.

The docs must remove the current statement that enforcement is pending only
after the implementation and live acceptance are complete.

## 15. Acceptance summary

Issue #81 is complete only when:

1. every raw, extracted, generated, uploaded, reused, and consumed object is
   checked against its selected lock;
2. physical schema contracts are enforced;
3. a complete immutable generation is remotely verified before one atomic
   manifest switch;
4. readers resolve and retain exactly one generation per run;
5. corruption, source drift, generator drift, crashes, and concurrent compliant
   publishers cannot switch the active pointer to an unverified generation,
   within the storage/administrator trust boundary in section 7.3;
6. an unchanged rerun performs no source acquisition or S3 mutation;
7. the MinIO and TPC-H live gates prove the design on the supported platform;
8. canonical and generated documentation agree; and
9. all offline, live, documentation, evidence, Atlas, and protected-file gates
   pass before Gitflow promotion.

## 16. Storage protocol references

The conditional adapter follows the official S3 contracts for
[`If-None-Match` and `If-Match` writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
and conditional reads. MinIO Object Lock and versioning can provide a stronger
backend-enforced WORM policy, but require explicit bucket configuration and are
not assumed by this consumer-owned design. Live acceptance against the pinned
Atlas MinIO deployment remains authoritative over generic SDK or Moto behavior.
