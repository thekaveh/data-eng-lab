# Release policy implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the repository's intentionally-unreleased `0.1.0` state, version authority, changelog authority, and future owner-authorized release transaction exact and executable without creating a tag or release.

**Architecture:** A small offline Python contract reads only bounded repository-owned files and checks one static project version, one canonical detailed changelog, the root index, README state, and three-surface manifest. A canonical release-policy page defines SemVer, `v<version>` tags, notes, and the manual transaction; root and generated documentation identify the same current state.

**Tech Stack:** Python 3.11 (`argparse`, `html.parser`, `pathlib`, `re`,
`tomllib`), Python-Markdown with the repository's Pymdown extension set,
pytest, YAML-backed documentation manifest, MkDocs/wiki projections, GitHub
Releases.

**Spec:** `docs/superpowers/specs/2026-08-16-release-policy-design.md`

## Global constraints

- Base commit is `6d901400f47b08267407fa84fc04bad1125dad04`; branch is `codex/94-release-policy`.
- `pyproject.toml` `[project].version` is the sole project-version authority and remains `0.1.0`.
- `docs/CHANGELOG.md` is the sole detailed changelog authority; root `CHANGELOG.md` becomes an index, not a second history.
- The current state is exactly `0.1.0 (unreleased)`, with zero Git tags and zero GitHub Releases verified live at promotion.
- Version values use Semantic Versioning 2.0.0; annotated tag names use exact `v<version>` form.
- Release creation requires an explicit owner-authorized release pull request and a verified exact `main` commit.
- Do not create or push a tag or release; do not add an automatic release trigger or package-publishing workflow.
- Do not change Maven application versions or MinIO JAR coordinates.
- Atlas gitlink remains `c6cf73d7168db1a7840fc45c9ed3e385071996d8`; do not edit `infra/` or its source.
- Do not modify `uv.lock` or `datasets/registry.yaml`, start Atlas, run live acceptance, mutate Docker resources, or touch persistent volumes.
- Every behavior begins with a focused failing test observed RED before minimal implementation.
- Hand-authored changes use `apply_patch`; generated documentation is produced only by repository generators.

---

### Task 1: Build the bounded offline release contract

**Files:**
- Create: `scripts/release/__init__.py`
- Create: `scripts/release/contract.py`
- Create: `tests/release/__init__.py`
- Create: `tests/release/test_release_contract.py`

**Interfaces:**
- Produces: `ReleaseState(version: str, status: str, changelog: str)`.
- Produces: `validate_repository(root: Path) -> ReleaseState`.
- Produces CLI: `python -m scripts.release.contract --root .`, with success token `release_contract_ok` and exit `0`; one stable bounded code and exit `1` on contract failure.

- [ ] **Step 1: Write strict file and metadata RED tests**

Require repository-owned, regular, non-symlink UTF-8 files of at most 1 MiB;
one `[project]` table; string version `0.1.0`; strict SemVer syntax; and closed
failure codes for missing, oversized, malformed, symlinked, non-string, or
wrong-version inputs.

```python
state = validate_repository(repo_root)
assert state == ReleaseState(
    version="0.1.0",
    status="intentionally_unreleased",
    changelog="docs/CHANGELOG.md",
)

project["version"] = True
with pytest.raises(ReleaseContractFailure, match="project_version_invalid"):
    validate_repository(mutated_root)
```

- [ ] **Step 2: Run and observe RED**

Run: `uv run pytest -q tests/release/test_release_contract.py`

Expected: collection fails because `scripts.release.contract` does not exist.

- [ ] **Step 3: Implement bounded reads and exact project metadata**

Use `Path.resolve(strict=True)`, `is_symlink()`, `is_file()`, a 1 MiB binary
read cap, strict UTF-8 decoding, and `tomllib.loads`. Validate the project table
and version with this full expression:

```python
SEMVER = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)
```

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest -q tests/release/test_release_contract.py`

Commit: `feat(release): define offline state contract (#94)`

---

### Task 2: Reconcile the changelog authority and release state

**Files:**
- Modify: `scripts/release/contract.py`
- Modify: `tests/release/test_release_contract.py`
- Modify: `CHANGELOG.md`
- Modify: `docs/CHANGELOG.md`

**Interfaces:**
- Consumes: bounded reader and `ReleaseState` from Task 1.
- Produces: exact canonical-changelog and root-index validation.

- [ ] **Step 1: Write changelog RED tests**

Require one visible numbered `Unreleased` H2 in `docs/CHANGELOG.md`, at least
one nonempty Added/Changed entry, no dated `0.1.0` release H2, and root
`CHANGELOG.md` containing only its title, the exact current-state statement,
and a relative link to `docs/CHANGELOG.md`. Render through the same
Python-Markdown/Pymdown extension set as the documentation site and inspect the
bounded HTML H2 structure, so formatting, nesting, fences, comments, raw HTML,
LF, and CRLF follow one authority. Reject duplicate Unreleased headings, a
released `0.1.0` section, root change bullets, wrong links, and contradictory
state. Require governance headings and Added/Changed evidence to remain ordinary
rendered Markdown; reject CSS/classes, duplicate HTML attributes, and foreign
HTML-integration containers while allowing benign SVG/MathML graphics.

```python
canonical = read("docs/CHANGELOG.md")
assert canonical.count("## 1. [Unreleased]") == 1
assert "## 2. [0.1.0] -" not in canonical

root_changelog = read("CHANGELOG.md")
assert "0.1.0 is intentionally unreleased" in root_changelog
assert "[canonical changelog](docs/CHANGELOG.md)" in root_changelog
```

- [ ] **Step 2: Run and observe RED**

Run: `uv run pytest -q tests/release/test_release_contract.py -k changelog`

Expected: failures show the root changelog is an independently maintained stale
history and neither changelog binds the declared current version.

- [ ] **Step 3: Implement the single-authority changelog state**

Replace root `CHANGELOG.md` with a short index. Add one leading entry to the
canonical Unreleased section recording the release-policy reconciliation. Keep
all existing detailed entries byte-for-byte except for necessary wrapping.
Validate headings with anchored line expressions rather than substring search.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest -q tests/release/test_release_contract.py -k changelog`

Commit: `docs: establish canonical unreleased changelog (#94)`

---

### Task 3: Publish one release policy to all documentation surfaces

**Files:**
- Create: `docs/release-policy.md`
- Modify: `README.md`
- Modify: `docs/manifest.yaml`
- Modify: `scripts/release/contract.py`
- Create: `tests/release/test_release_documentation.py`

**Interfaces:**
- Consumes: `ReleaseState` and canonical changelog rules from Tasks 1–2.
- Produces: repository-operations leaf `9.2` with id `release-policy`, title `Release Policy`, source `docs/release-policy.md`.

- [ ] **Step 1: Write documentation and projection RED tests**

Require the README and policy to state `0.1.0 (unreleased)`; identify
`pyproject.toml` and `docs/CHANGELOG.md`; distinguish Maven artifact versions;
define SemVer, annotated `v<version>` tags, immutable released versions,
changelog-derived notes, verified-main tagging, and explicit owner
authorization; forbid claims that metadata means released. Require exact
manifest page `9.2` and preserve changelog page `10`.

```python
policy = (root / "docs/release-policy.md").read_text()
for phrase in (
    "0.1.0 (unreleased)",
    "pyproject.toml",
    "docs/CHANGELOG.md",
    "v<version>",
    "explicit owner authorization",
):
    assert phrase in policy
```

- [ ] **Step 2: Run and observe RED**

Run: `uv run pytest -q tests/release/test_release_documentation.py`

Expected: the policy page, README state, and manifest leaf are missing.

- [ ] **Step 3: Implement the canonical policy and projections**

Write numbered, user-facing sections covering current state, authorities,
version selection, the release transaction, rollback/no-tag behavior, and
Maven/Atlas boundaries. Add a concise README Release state section and link to
the policy and canonical changelog. Register the page under Repository
Operations immediately after Security Automation.

- [ ] **Step 4: Extend the validator and verify all three surfaces**

Read the manifest through the bounded owned-file boundary, call the canonical
`scripts.docs.manifest.parse_manifest`, locate exact leaves, and reject wrong
three-surface metadata, ids, numbers, titles, or sources. The strict docs gate
continues to validate every referenced filesystem path. Then run:

```bash
uv run pytest -q tests/release
make docs-check
make docs-wiki
```

Expected: release tests pass; strict site and wiki generation produce no
warnings or drift.

- [ ] **Step 5: Commit**

Commit: `docs: publish unreleased version policy (#94)`

---

### Task 4: Integrate and close the offline release gate

**Files:**
- Modify: `Makefile`
- Modify: `tests/release/test_release_contract.py`
- Modify: `tests/release/test_release_documentation.py`
- Modify: `docs/CHANGELOG.md`

**Interfaces:**
- Consumes: CLI from Task 1 and all documentation contracts.
- Produces: `make release-check` as the stable maintainer command.

- [ ] **Step 1: Write command and no-automation RED tests**

Require a phony `release-check` target invoking only
`uv run python -m scripts.release.contract --root .`. Enumerate exactly the
audited `.github/workflows/*.yml` inventory and bind every owned workflow byte
sequence to its reviewed SHA-256 digest. Reject additions, removals, symlinks,
malformed YAML, or byte drift; a later workflow change must deliberately update
the closed digest contract under review. Run the sole token-bearing local
wiki-push script in a dedicated contents-write job after a read-only artifact
build, using isolated system Python with no dependency synchronization or prior
local-code execution. Bind that script to its reviewed digest as part of the
same closed surface.

- [ ] **Step 2: Run and observe RED**

Run: `uv run pytest -q tests/release -k 'command or automation'`

Expected: the Make target is missing.

- [ ] **Step 3: Implement the target and exact CLI behavior**

Add `release-check` to `.PHONY` and `help`, invoke the module command exactly,
and ensure successful stdout is one line:

```text
release_contract_ok
```

On failure, stderr contains only one stable `release_*` code and the CLI exits
`1` without a traceback.

- [ ] **Step 4: Run focused and full verification**

Run:

```bash
make release-check
uv run pytest -q tests/release
uv run ruff check scripts/release tests/release
uv run ruff format --check scripts/release tests/release
make verify
make docs-check
make docs-wiki
make test
```

Expected: every command succeeds; the offline suite deselects live/network
tests; no tag, release, Docker resource, protected file, or Atlas gitlink
changes occur.

- [ ] **Step 5: Verify live GitHub no-release state and commit**

Read `gh release list`, repository tags, and exact `main`/`develop` trees.
Require zero releases, zero tags, and content-identical promoted branches before
issue closeout.

Commit: `test: enforce unreleased repository state (#94)`

---

### Task 5: Review, GitFlow promotion, and cleanup

**Files:**
- No product files beyond Tasks 1–4.

**Interfaces:**
- Consumes: exact `origin/develop..HEAD` binary review package and complete gate evidence.
- Produces: merged feature-to-develop PR, merged develop-to-main PR, zero-file main-to-develop backsync, final issue/project closeout, and no dangling task refs.

- [ ] **Step 1: Freeze and independently review the exact package**

Generate the binary diff twice, compare bytes and SHA-256, then obtain one
specification/acceptance review and one quality/security/adversarial review.
Both must report exactly C0/I0/M0 Ready Yes; otherwise establish strict RED,
fix minimally, rerun proportionate gates, regenerate, and re-review.

- [ ] **Step 2: Promote through protected GitFlow**

Push `codex/94-release-policy`, open a ready PR to `develop`, wait for all
required/advisory checks, and merge without bypass. Open ready
`develop -> main` with `Closes #94`, wait for all checks and security analyses,
merge, then open and merge a proven zero-file `main -> develop` backsync.

- [ ] **Step 3: Verify terminal state and clean exact refs**

Wait for final `develop` CI and final CodeQL/OSV conclusions. Prove zero Git
tags/releases again, close #94 as Done with exact evidence, delete only the
task feature branch locally/remotely and its worktree, preserve unrelated
branches/worktrees/untracked files, and confirm `main` and `develop` trees are
identical.
