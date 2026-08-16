# Repository Security Protections Design

## Context

Issue #92 installed dependency, OSV, and CodeQL automation. Issue #93 owns the
repository settings that make GitHub's server-side protections authoritative.
The pre-change repository API reports vulnerability alerts, Dependabot security
updates, secret scanning, push protection, non-provider patterns, and validity
checks disabled. CodeQL and OSV analyses are already visible after merge.

## Decision

Enable and verify the protections through GitHub's documented repository APIs,
then retain only a bounded, secret-free evidence summary in the repository. A
small pure validator accepts that summary and refuses malformed, duplicate-key,
oversized, stale, wrong-repository, or incomplete evidence. It does not call
GitHub, read credentials, mutate settings, or treat workflow presence as proof.

The selected required state is:

- vulnerability alerts and Dependabot security updates enabled;
- secret scanning and push protection enabled;
- non-provider patterns and validity checks enabled only if GitHub accepts them
  for this public user-owned repository; otherwise the exact plan/authority
  limitation is recorded;
- at least the Python and Actions CodeQL analyses visible at the merged commit;
- the secret-scanning alerts endpoint readable after enablement; and
- an official GitHub dummy token rejected by push protection, with the probe
  remote ref proved absent afterward.

## Evidence contract

The canonical JSON document is UTF-8, at most 64 KiB, duplicate-key free, at
most 16 levels and 4,096 nodes, and contains only these top-level fields:

- `schema_version`, fixed at `1`;
- `repository`, fixed at `thekaveh/data-eng-lab`;
- `captured_at`, canonical whole-second UTC;
- `commit_sha`, a lowercase 40-character Git object ID;
- `settings`, a closed map of the five repository settings;
- `dependabot`, binding both alert API results and the exact Dependabot
  pull-request numbers, with security and version updates kept disjoint;
- `secret_scanning`, binding alert API readability and the exact probe ref and
  commit for the safe rejection result;
- `code_scanning`, binding the exact commit, required categories, and exact
  CodeQL analysis IDs;
- `limitations`, a bounded list containing only unsupported optional features.

The verifier emits one canonical JSON summary on success and a single bounded
error code on failure. It never echoes an input document or a credential.

## Safe mutation and probe

The operator reads the pre-state first, enables one documented protection at a
time, reads the post-state, and validates the secret-scanning and code-scanning
endpoints. The push-protection probe uses GitHub's published dummy token in a
temporary local commit and an exact, unique probe ref. Success means GitHub
rejects the push and the remote ref is absent. If a push unexpectedly succeeds,
the exact remote probe ref is deleted immediately; no real credential is ever
used. The temporary checkout and branch are removed after evidence is captured.

## Boundaries

This issue adds no recurring workflow, live Atlas execution, cloud runner,
repository secret, release, registry mutation, Atlas submodule edit, or Docker
mutation. It does not enable private vulnerability reporting, AI detection,
delegated bypass, or custom patterns because they are outside issue #93.

## Acceptance

Tests prove strict evidence parsing and every refusal boundary. The live API
evidence must show required settings enabled, readable alert APIs, current
CodeQL analyses, and the safe push rejection. Documentation must name any
unsupported optional feature and its exact settings URL or required plan. The
normal dual review, GitFlow, final CI, backsync, closeout, and cleanup gates then
apply. Promotion requires both independent reviews to report C0/I0/M0 Ready Yes.
