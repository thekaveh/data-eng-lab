# 9.2. Release Policy

## 1. Current state

The project version is **0.1.0 (unreleased)**. No tag or GitHub Release exists
for it. The static value in `pyproject.toml` identifies the intended first
release; package metadata does not mean that a release has been published.

The repository is in initial development. Users should consume an exact commit
rather than infer stability or immutability from the planned version.

## 2. Authorities

`pyproject.toml` `[project].version` is the sole project-version authority.
`docs/CHANGELOG.md` is the sole detailed changelog authority and its
`Unreleased` section records every change not yet published. The root changelog
is only an index to that canonical history. Git tags and GitHub Releases are
the publication evidence.

The six Maven Spark applications retain their own artifact versions and stable
MinIO coordinates. Their current `0.1.0` values do not publish or determine the
repository version. The Atlas submodule pin is also an independent dependency
identity.

## 3. Version and tag convention

The project follows [Semantic Versioning 2.0.0](https://semver.org/). During
initial development, `0.y.z` may change incompatibly. After `1.0.0`, an
incompatible public-contract change increments MAJOR, a compatible addition
increments MINOR, and a compatible fix increments PATCH.

The metadata version contains no prefix. A release uses one annotated tag named
exactly `v<version>`, such as `v0.1.0`, pointing to the verified `main` commit.
A published version and its tag are immutable; a correction requires a new
version.

## 4. Release transaction

A release has no timer, push, merge, or package-metadata trigger. It begins only
with explicit owner authorization for a named version in a dedicated release
pull request. That pull request must:

1. start from verified `develop`;
2. select a valid next version and update `pyproject.toml` when needed;
3. move the complete `Unreleased` contents into one dated version section and
   recreate an empty `Unreleased` section;
4. update README and this policy to the same state;
5. pass repository, documentation, security, and release-contract checks; and
6. promote through `feature -> develop -> main`.

Only after the exact merged `main` commit is verified may the owner create its
annotated tag and GitHub Release. The release notes are derived from that
version's section in the canonical changelog. GitHub's release transaction and
artifact attachment behavior are documented in
[Managing releases in a repository](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository).

## 5. Failure and rollback boundary

If any pre-tag gate fails, no tag or GitHub Release is created. A release pull
request may be corrected or closed without changing the last published state.
After publication, rollback means issuing a new version or documenting a
withdrawal; it never means moving or rewriting the published tag.

Tag and GitHub Release creation remain manual owner actions. Automation requires
a separately authorized, reviewed change to this policy.
