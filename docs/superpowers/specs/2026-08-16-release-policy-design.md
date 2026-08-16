# Release Policy Design

## Context

Issue #94 begins after the higher-priority maintenance backlog is complete and
the final `develop` verification is green. The repository declares version
`0.1.0` in `pyproject.toml`, but GitHub has no tags or releases. The root
`CHANGELOG.md` contains a short early subset while `docs/CHANGELOG.md` is the
actively maintained, three-surface change history. The README does not explain
whether `0.1.0` has shipped.

No tag or release is authorized by this issue. The policy must therefore make
the existing intentionally-unreleased state explicit without inventing a
release, changing an application artifact coordinate, or treating the Atlas
submodule's version as this repository's version.

## Alternatives

1. Keep root and documentation changelogs independently editable. This is
   rejected because the current files already demonstrate silent drift.
2. Generate and commit a second full changelog mirror. This removes some human
   editing but adds a tracked derived artifact and a synchronization mechanism
   solely to duplicate content.
3. Use one detailed changelog authority and make every other release surface
   identify it. This is selected because it is the smallest enforceable design
   and fits the existing three-surface documentation pipeline.

## Authorities and current state

- `pyproject.toml` `[project].version` is the sole project-version authority.
  Its current value is `0.1.0`, the intended first release version.
- `docs/CHANGELOG.md` is the sole detailed changelog authority. Its current
  `Unreleased` section contains all changes since repository inception and is
  projected into the generated site and wiki from `docs/manifest.yaml`.
- Root `CHANGELOG.md` is a stable repository index. It identifies the canonical
  changelog and current unreleased state, but carries no separately maintained
  change entries.
- Git tags and GitHub Releases are publication evidence. Their current empty
  sets mean `0.1.0` is not released; package metadata alone does not establish a
  release.
- README and the release-policy page state `0.1.0 (unreleased)` and link to the
  canonical changelog.

## Versioning and tag policy

The project uses Semantic Versioning 2.0.0. During initial development,
`0.y.z` may change incompatibly; after `1.0.0`, incompatible public-contract
changes increment MAJOR, compatible additions increment MINOR, and compatible
fixes increment PATCH. The version value contains no `v` prefix. An annotated
release tag is exactly `v<version>`, for example `v0.1.0`, and must point to the
verified merge commit on `main`.

The Python project version and six Maven application versions are different
authorities. The Maven `0.1.0` values and MinIO object coordinates version the
individual Spark artifacts; issue #94 does not couple or change them.

## Release trigger and transaction

There is no automatic release trigger. A release starts only when the owner
explicitly authorizes a named version in a dedicated release pull request.
That pull request must:

1. start from the current verified `develop` branch;
2. select a valid next Semantic Version and update `[project].version` if the
   selected version differs;
3. move the complete `Unreleased` contents into one dated
   `## N. [<version>] - YYYY-MM-DD` section and recreate an empty
   `## 1. [Unreleased]` section;
4. update README and release-policy state to the same version;
5. pass the repository, documentation, security, and release-contract gates;
6. promote through `feature -> develop -> main`; and
7. only after the exact `main` commit is verified, create the annotated
   `v<version>` tag and a GitHub Release whose notes are derived from that
   version's canonical changelog section.

If any pre-tag step fails, no tag or release is created. Once published, a
version is immutable; corrections require a new version. Tag and GitHub Release
creation remain manual owner actions unless a later, separately reviewed issue
authorizes automation.

## Static release contract

A pure Python validator enforces the repository state without network access.
It reads bounded regular UTF-8 files only, parses `pyproject.toml` with
`tomllib`, and validates:

- one strict SemVer project version, currently `0.1.0`;
- one numbered `Unreleased` section in the canonical changelog and no released
  version section while the policy says intentionally unreleased;
- the root changelog index, README, policy page, and manifest all identify the
  same authority and state;
- the manifest exposes the policy as repository operations `9.2` and preserves
  the changelog as page `10`; and
- no release/tag command or automatic release workflow is introduced.

Success emits only `release_contract_ok`. Failures emit one bounded stable
error code and never echo file contents. Live tag, release, and branch state is
verified separately during promotion and closeout because an offline validator
cannot prove GitHub server state.

## Documentation projection

`docs/release-policy.md` is the canonical user-facing policy. Adding it to
`docs/manifest.yaml` projects the same text to the strict MkDocs site and native
wiki. README contains only a concise user-facing current-state summary. Root
`CHANGELOG.md` links to `docs/CHANGELOG.md`; generated surfaces contain the
canonical changelog directly, so no surface links to another publication.

## Boundaries

This issue creates no tag, GitHub Release, package publication, release
workflow, registry mutation, dataset update, Atlas source or gitlink edit,
Docker resource, or live Atlas execution. It does not renumber Maven artifacts,
rewrite historical changelog facts, or infer authorization from version
metadata.

## Acceptance

Strict RED-to-GREEN tests cover version syntax and type, missing/duplicate
changelog sections, contradictory release state, manifest numbering, bounded
files, symlinks, and accidental release automation. The final tree must pass
the release contract, full offline suite, repository verifier, Ruff, strict
site/wiki generation, Compose validation, protected-file checks, and two
independent C0/I0/M0 Ready Yes reviews. Promotion must re-read GitHub and prove
no tags or releases before closing issue #94.
