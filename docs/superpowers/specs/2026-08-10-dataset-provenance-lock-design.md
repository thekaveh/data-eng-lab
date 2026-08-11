# Dataset Provenance Lock Design

**Date:** 2026-08-10

**Issue:** [#80](https://github.com/thekaveh/data-eng-lab/issues/80)

**Parent:** [#79](https://github.com/thekaveh/data-eng-lab/issues/79)

**Status:** Approved design

## 1. Objective

Upgrade `datasets/registry.yaml` from a source catalog into a lock-grade,
reviewable provenance contract for every supported dataset and scale. The
contract must identify the exact source or generator inputs, raw artifacts,
landing objects, byte sizes, SHA-256 digests, and schema expectations needed to
reproduce the repository's landing data.

This issue defines, validates, parses, populates, tests, and documents the lock.
Child issue #81 will enforce the lock while downloading, extracting, generating,
uploading, and reusing MinIO objects. Issue #80 must not silently change current
downloader execution or object-store reuse behavior.

## 2. Design decision

The registry advances to version 2 and owns one normalized artifact catalog per
dataset. Scale tiers reference artifact identifiers instead of repeating URLs
and checksums. This keeps the same January NYC Taxi or GH Archive hour from
acquiring contradictory metadata when it appears in multiple tiers.

The rejected alternatives are:

1. Inline complete artifact metadata beneath every scale. It is easy to parse
   but duplicates shared artifacts across `tiny`, `small`, and `medium`.
2. Add a separate `datasets/lock.yaml`. It separates descriptive metadata from
   lock data, but creates two files whose dataset, tier, source, and object
   identities can drift.

The version-2 registry remains the single source of truth.

## 3. Global lock policy

Registry version 2 includes a global lock policy with these fixed semantics:

| Field | Contract |
|---|---|
| Digest algorithm | SHA-256, lowercase hexadecimal, exactly 64 characters |
| Source drift | Fail closed; no alternate or tolerated digest list |
| Object drift | Fail closed; existence alone never proves validity |
| Schema fingerprint | SHA-256 of the canonical committed schema contract |
| Registry updates | Explicit maintainer review and commit |

An upstream URL may be labelled `immutable` or `mutable`. Both kinds still have
one accepted byte size and digest. If an upstream project replaces bytes at a
mutable URL, the eventual #81 verifier must reject the new bytes. A maintainer
then reviews provenance, licensing, schema, and downstream compatibility before
updating the registry. The repository never silently blesses changed bytes.

## 4. Shared dataset metadata

Every dataset retains its existing description, format, license,
`landing_prefix`, fetch kind, and scale tiers. It adds a `provenance` mapping:

| Field | Meaning |
|---|---|
| `publisher` | Human-readable authoritative publisher or generator owner |
| `homepage` | Canonical HTTPS source or specification page |
| `license_name` | Reviewable license identifier or published license name |
| `license_url` | Canonical HTTPS license or terms page |
| `attribution` | Required source attribution carried into documentation |
| `source_stability` | `immutable` or `mutable` |
| `update_policy` | Fixed value `reviewed-lock-update` |

Committed provenance free text prohibits non-global IP addresses,
user-home or temporary paths, host ports, MinIO endpoints, URI credentials,
and credential key/value forms.
Provenance free text allows standardized semantic `Label:number` references;
all other endpoint-shaped tokens fail closed regardless of label casing.
The case-insensitive allowlist is `DOI`, `ISBN`, `ISSN`, `PMID`, `PMCID`,
`ORCID`, `RFC`, `CVE`, `GHSA`, `ISO`, `IEC`, `IEEE`, `ANSI`, `NIST`, `SOC`,
`HIPAA`, `GDPR`, `Version`, `Volume`, `Year`, `Edition`, `Standard`, `Issue`,
`Section`, `Page`, `Chapter`, `Figure`, `Table`, `Article`, and `AS`.
An HTTP artifact may carry a complete `provenance` override with the same
validated fields. When present, the artifact-level terms govern that release;
the dataset-level mapping remains the conservative default for artifacts that
do not override it.

## 5. Schema contracts

Each dataset defines a normalized `schemas` mapping. An artifact or generated
object references one schema identifier. A schema entry contains:

- the physical format;
- `exact` or `minimum` compatibility mode;
- an ordered field contract with field name, normalized logical type, and
  nullability;
- format-specific structure needed to interpret the bytes, such as CSV header
  presence or JSON record shape;
- a SHA-256 fingerprint calculated from the canonical JSON encoding of the
  schema entry excluding the fingerprint itself.

Canonical encoding uses UTF-8 JSON with keys sorted, no insignificant
whitespace, non-ASCII characters preserved, and the ordered field list left in
declared order. Logical types use one repository vocabulary rather than
engine-specific display strings. This makes the fingerprint computable without
network or file access and identical on every supported Python platform.

`exact` means no undeclared field or type change is accepted. `minimum` is used
for extensible records such as GH Archive JSON: all declared fields and types
must remain compatible, while additional fields may appear. Artifact byte
digests still detect any upstream byte change even when schema mode is
`minimum`.

Schema identity is per artifact where required. This is necessary for the
reviewed NYC Taxi source variation: January 2023 encodes `passenger_count` and
several identifier fields differently from February through June. The registry
must record the source schemas;
normalization performed later by notebooks or Spark jobs does not erase that
provenance.

## 6. HTTP artifact contract

HTTP datasets define unique entries in an `artifacts` mapping. Each entry has:

### 6.1 Source identity

- canonical HTTPS URL;
- an optional complete artifact-level provenance override for release-specific
  publisher, license, attribution, and stability terms;
- a `version` selector with `kind` equal to `revision` or `publication-date`
  and a non-empty string value;
- source stability that must exactly match its effective provenance: the
  artifact-level override when present, otherwise the dataset provenance;
- optional upstream ETag or Last-Modified value as supporting evidence only.

ETag and Last-Modified never replace the required size and SHA-256 digest.

### 6.2 Raw artifact

- raw filename;
- exact byte size;
- SHA-256 digest.

### 6.3 Landing outputs

Each output records:

- the exact object name relative to the dataset's `landing_prefix`;
- exact byte size;
- SHA-256 digest;
- schema identifier;
- for archive sources, the exact archive member path;
- for direct sources, an explicit raw-object identity.

Direct downloads therefore lock both the source identity and the landing object
identity, while the validator guarantees that their size and digest agree.
Archives lock the downloaded archive plus every extracted object independently.
Flattened archive filenames must be unique within one artifact and across all
artifacts selected by one scale, so extraction cannot overwrite one member with
another unnoticed. The same flattened name may occur in mutually exclusive
release artifacts because only one is selected for a scale; this scale-local
landing identity is intentional.

Each HTTP scale contains only an ordered `artifacts` list. Shared artifacts are
referenced, not copied.

## 7. TPC-H generator contract

TPC-H has no downloaded data artifact. Its provenance instead locks:

- generator owner and specification;
- engine name (`duckdb`);
- exact DuckDB package version resolved by `uv.lock`;
- extension name and version relationship;
- the reviewed installed extension artifact SHA-256 and official repository URL;
- an OCI image identified by immutable digest and platform `linux/amd64`;
- the repository `uv.lock` SHA-256, locale, and timezone used by the generator;
- generator command and fixed parameters;
- scale factor for each tier;
- deterministic export settings;
- the eight expected table/object names;
- exact output sizes and SHA-256 digests for the canonical generation
  environment;
- one schema identifier per table.

The canonical generation environment is the locked `linux/amd64` OCI image plus
the committed `uv.lock` digest and recorded locale/timezone. The image copies
and verifies that lockfile, acquires the version-matched TPC-H extension once at
build time, and verifies the installed extension artifact digest. Reference
generation runs with networking disabled and loads that verified extension; it
never installs an extension at runtime. The deterministic export contract sets
one DuckDB thread, preserves insertion order, and includes stable row ordering
and explicit Parquet settings rather than relying on host CPU count, scheduling,
or DuckDB defaults. Issue #80 records and parses these settings and the reviewed
reference outputs. Issue #81 will make
`generate_tpch` consume them and verify the produced bytes before upload. Until
#81 merges, the production downloader continues its current generation behavior
and makes no new verification claim.

The committed export settings are Zstandard compression and a row-group size of
100,000 rows. Output order is fixed by TPC-H keys: `c_custkey` for customer;
`l_orderkey, l_linenumber` for lineitem; `n_nationkey` for nation;
`o_orderkey` for orders; `p_partkey` for part; `ps_partkey, ps_suppkey` for
partsupp; `r_regionkey` for region; and `s_suppkey` for supplier.

Every TPC-H scale contains its generator parameters and complete output lock;
there is no accepted object that lacks an exact byte and schema identity.

## 8. Python model and compatibility boundary

`datasets/registry.py` gains frozen value objects for the new contract:

- `Provenance`;
- `SchemaContract` and `SchemaField`;
- `SourceVersion`;
- `RawArtifact`;
- `LandingObject`;
- `HttpArtifact`;
- `GeneratorContract` and generated-output records.

`Dataset` owns the normalized contracts. `ScalePlan` retains the existing
`urls` and `sf` views needed by the current downloader and also exposes resolved
locked artifacts or generator outputs. This preserves current call sites in
issue #80 while giving issue #81 typed verification inputs.

Version 1 is not retained as an accepted production schema after the committed
registry is migrated. Rejecting it prevents an old, unlocked registry from
looking valid. Tests may construct deliberately invalid version-1 fixtures to
prove that behavior.

## 9. Validation and errors

`datasets/schema.py` remains a pure, no-I/O validator and returns all discovered
path-qualified errors in one pass. It rejects:

- a registry version other than 2;
- missing or unknown lock fields;
- missing source revision/publication identity;
- non-HTTPS source and license URLs;
- malformed artifact identifiers or unsafe object/member paths;
- non-positive or non-integer byte sizes;
- non-lowercase or non-64-character SHA-256 values;
- missing schemas or schema-fingerprint mismatches;
- duplicate landing object names within an artifact or across artifacts
  selected by the same scale;
- unknown scale artifact or schema references;
- direct-object size/digest disagreement;
- archive outputs without member paths;
- incomplete TPC-H engine, version, parameter, export, table, or output locks;
- machine-local values within committed provenance fields.

`load_registry` raises the existing aggregated `ValueError` format when the
validator returns errors. Parsing does not perform network, filesystem-artifact,
or object-store verification.

## 10. Lock population and review workflow

The issue #80 implementation gathers each artifact from its authoritative URL
or canonical generator environment, computes its size and SHA-256, inspects its
schema, and populates the registry. Temporary raw data and generated outputs are
not committed.

A maintainer-only `scripts/audit_dataset_lock.py` command makes this repeatable.
It is read-only with respect to the registry, emits candidate metadata for
review, never uploads to MinIO, and never silently rewrites accepted digests.
The documented update sequence is:

1. identify the upstream revision or publication date and review its license;
2. obtain bytes from the authoritative source or canonical generator;
3. calculate raw, extracted, and generated sizes/digests;
4. derive and review canonical schema contracts;
5. inspect downstream compatibility;
6. update the registry in a dedicated reviewed change;
7. run focused validation plus all repository and documentation gates.

## 11. Documentation surfaces

`docs/datasets.md` is the canonical public explanation and must describe:

- registry version 2 and its single-source role;
- immutable artifact and generator identities;
- strict fail-on-drift behavior;
- the difference between contract definition (#80) and runtime enforcement
  (#81);
- source attribution and licensing;
- the reviewed update procedure.

The changelog records the new contract. Existing README opener facts and counts
do not change, so the opener, badges, and architecture artwork need no content
edit. The standard docs build and wiki projection checks prove that the
canonical update reaches the MkDocs site and native wiki without hand-edited
generated copies.

## 12. Testing strategy

TDD starts with failing schema and model tests. Coverage includes:

- the populated real registry;
- a minimal valid HTTP direct-download contract;
- a minimal valid archive contract;
- a minimal valid TPC-H generator contract;
- every required-field and format failure listed in section 9;
- artifact and schema reference resolution;
- shared-artifact scale resolution without metadata duplication;
- preservation of the current downloader-facing URL and scale-factor views;
- fail-closed validation of the copied `uv.lock` and installed TPC-H extension,
  plus repeat generation of every scale under network isolation;
- documentation claims and three-surface projection checks.

The full completion matrix is `make lint`, `make test`, `make verify`,
`make docs-check`, `make docs-wiki`, and `git diff --check`. Network acquisition
used to populate lock values is recorded separately and does not replace the
offline regression suite.

## 13. Issue boundary and sequencing

Issue #80 ends when the populated version-2 registry, validation/model support,
tests, documentation, and reviewed provenance evidence are merged through
`develop` and `main`. It makes no claim that existing downloaded or MinIO bytes
are yet checked.

Issue #81 then consumes this exact contract to implement fail-before-use
verification for HTTP download, archive extraction, TPC-H generation, upload,
and existing-object reuse. For release alternatives such as MovieLens, #81 must
atomically replace the selected release and prevent mixed stale-release objects
under the shared landing prefix. Scheduled live acceptance remains downstream
of both children.

## 14. Acceptance mapping

| Issue #80 acceptance criterion | Design evidence |
|---|---|
| Missing or malformed lock metadata is rejected | Sections 3, 5, 6, 7, and 9 |
| Every dataset and scale validates | Normalized HTTP artifacts and complete per-scale TPC-H outputs |
| Required-field and format failures are tested | Section 12 |
| Provenance, update, license, and attribution are documented | Sections 4, 10, and 11 |
| Downloader behavior is not silently changed | Sections 1, 8, and 13 |
