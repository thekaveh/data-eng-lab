# Dependency and code-scanning automation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver exact parent-owned Dependabot coverage, fail-closed OSV dependency auditing, advanced CodeQL analysis, and synchronized security-reporting/remediation documentation for issue #92.

**Architecture:** A strict repository contract derives the current dependency inventory and validates three pinned GitHub security configurations. Dependabot updates the exact GitHub Actions, uv, and six Maven surfaces; OSV scans the seven exact manifests without recursion; CodeQL analyzes Python and Actions only; canonical documentation states all exclusions and GitFlow remediation rules.

**Tech Stack:** Python 3.11, PyYAML, pytest, GitHub Actions, Dependabot v2 configuration, OSV-Scanner Action v2.5.0, CodeQL Action v4, MkDocs/wiki projections.

## Global constraints

- Base commit is `1becafad8cfcae652e009dfab581cc0191f88ab1`; branch is `codex/92-security-automation`.
- Atlas gitlink remains `c6cf73d7168db1a7840fc45c9ed3e385071996d8`; do not edit `infra/` or advance the gitlink.
- Do not start Atlas, run live acceptance, mutate repository security settings, change the dataset registry, create a release, or touch persistent project volumes.
- Do not modify `uv.lock`, `pyproject.toml`, `datasets/registry.yaml`, the protected untracked Atlas plan, or `graphify-out/`.
- Dependency authority is exactly root `uv.lock`, root GitHub Actions, and the six current parent-owned Spark-app POMs.
- OSV scan arguments contain only explicit `--lockfile=` operands; recursion and directory scans are forbidden.
- CodeQL languages are exactly `python` and `actions`; Scala is unsupported and must never be represented as Java/Kotlin coverage.
- Every `uses:` reference is a full immutable commit SHA. Pull-request scans have read-only token permissions.
- Every production/configuration behavior begins with a focused failing test observed RED before minimal implementation.
- Hand-authored file changes use `apply_patch`; generated documentation is written only by repository generators.

---

### Task 1: Define the strict dependency inventory and configuration parser

**Files:**
- Create: `scripts/security/__init__.py`
- Create: `scripts/security/contract.py`
- Create: `tests/security/__init__.py`
- Create: `tests/security/test_security_contract.py`

**Interfaces:**
- Produces: `DependencyInventory(uv_lock: str, action_directory: str, maven_directories: tuple[str, ...])`.
- Produces: `discover_inventory(root: Path) -> DependencyInventory`.
- Produces: `load_yaml_exact(path: Path) -> Mapping[str, object]` with duplicate-key rejection.
- Produces CLI: `python -m scripts.security.contract --root .`.

- [ ] **Step 1: Write inventory and strict-YAML RED tests**

Require one root uv lock, one Actions root, six sorted Maven directories, rejection
of symlinks/out-of-root paths, duplicate YAML keys, non-mapping documents, aliases,
and oversized configuration files.

```python
assert discover_inventory(repo_root).maven_directories == EXPECTED_MAVEN_DIRS
with pytest.raises(ContractFailure, match="yaml_duplicate_key"):
    load_yaml_exact(write_yaml("version: 2\nversion: 3\n"))
```

- [ ] **Step 2: Run and observe RED**

Run: `uv run pytest -q tests/security/test_security_contract.py`

Expected: collection fails because `scripts.security.contract` does not exist.

- [ ] **Step 3: Implement minimal discovery and parsing**

Resolve only repository-relative regular files, derive Maven directories from
parent-owned `spark-apps/*/pom.xml`, use a duplicate-rejecting `SafeLoader`, and
apply bounded byte/node/depth validation before returning immutable values.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest -q tests/security/test_security_contract.py`

Commit: `feat(security): define automation contract (#92)`

---

### Task 2: Add exact Dependabot coverage

**Files:**
- Create: `.github/dependabot.yml`
- Modify: `scripts/security/contract.py`
- Modify: `tests/security/test_security_contract.py`

- [ ] **Step 1: Write Dependabot RED tests**

Require version 2; ecosystems exactly `github-actions`, `uv`, and `maven`;
GitHub Actions and uv directories `/`; Maven directories equal discovered POM
directories; weekly staggered schedules; target branch `develop`; bounded open
PRs; routine grouping; and no `infra` or generated path.

- [ ] **Step 2: Observe RED**

Run: `uv run pytest -q tests/security/test_security_contract.py -k dependabot`

Expected: the required configuration is missing.

- [ ] **Step 3: Implement configuration and validator**

Create one Actions entry, one uv entry, and six Maven entries. Validate exact
keys/types, package-ecosystem values, schedules, target branches, directory
equality, and bounds without silently ignoring unknown fields.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest -q tests/security/test_security_contract.py -k dependabot`

Commit: `ci: cover parent dependency manifests with Dependabot (#92)`

---

### Task 3: Add fail-closed OSV dependency scans

**Files:**
- Create: `.github/workflows/dependency-security.yml`
- Modify: `scripts/security/contract.py`
- Modify: `tests/security/test_security_contract.py`

- [ ] **Step 1: Write workflow RED tests**

Require pull-request and merged/manual paths; exact seven lockfile
operands; no recursive/directory operand; `fail-on-vuln: true`; PR SARIF false
with only `contents: read`; merged SARIF true with only contents/actions read and
security-events write; immutable OSV SHA; concurrency bounds; and no secrets.

- [ ] **Step 2: Observe RED**

Run: `uv run pytest -q tests/security/test_security_contract.py -k osv`

Expected: the OSV workflow is missing.

- [ ] **Step 3: Implement workflow and validation**

Call `google/osv-scanner-action/.github/workflows/osv-scanner-reusable.yml` at
`8deb546fdb875b9996d27d4950be7312dac076a1` from separate PR and full-analysis
jobs with their own permissions and mutually exclusive event guards.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest -q tests/security/test_security_contract.py -k 'osv or action_pin or permissions'`

Commit: `ci: audit exact dependency manifests with OSV (#92)`

---

### Task 4: Add advanced CodeQL for supported source languages

**Files:**
- Create: `.github/workflows/codeql.yml`
- Create: `.github/codeql-config.yml`
- Modify: `scripts/security/contract.py`
- Modify: `tests/security/test_security_contract.py`

- [ ] **Step 1: Write CodeQL RED tests**

Require exact `python` and `actions` matrix, build mode none, security-extended
queries, immutable checkout/CodeQL SHAs, no submodules or persisted credentials,
least permissions, push/PR main+develop, manual triggers, concurrency,
and closed exclusions for Atlas/generated/build/user-owned paths.

- [ ] **Step 2: Observe RED**

Run: `uv run pytest -q tests/security/test_security_contract.py -k codeql`

Expected: CodeQL workflow/config are missing.

- [ ] **Step 3: Implement workflow and config**

Use CodeQL commit `ff2f1c621b7f889edc0d3c761ac2e6a3f8cdb0dd` and checkout
commit `d23441a48e516b6c34aea4fa41551a30e30af803`. Analyze the two
matrix languages independently and upload uniquely categorized analyses.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest -q tests/security/test_security_contract.py -k codeql`

Commit: `ci: analyze Python and Actions with CodeQL (#92)`

---

### Task 5: Publish security reporting and remediation documentation

**Files:**
- Create: `.github/SECURITY.md`
- Create: `docs/security-automation.md`
- Modify: `docs/README.md`
- Modify: `docs/manifest.yaml`
- Modify: repository-generated site/wiki projections
- Create: `tests/security/test_security_documentation.py`

- [ ] **Step 1: Write documentation RED tests**

Require supported-line policy, private-reporting path and safe fallback,
acknowledgement/remediation/disclosure flow, scanner triage, exact GitFlow
handling for default-main security PRs, exception expiry, Scala/Test-Maven/Atlas
limitations, seven-manifest inventory, #93 settings ownership, and three-surface
manifest/projection declarations.

- [ ] **Step 2: Observe RED**

Run: `uv run pytest -q tests/security/test_security_documentation.py`

Expected: policy, canonical source, and manifest leaf are absent.

- [ ] **Step 3: Implement canonical policy/runbook and projections**

Write `.github/SECURITY.md` and `docs/security-automation.md`, add the manifest
entry/navigation link, then run only the repository's documentation generator
to update site/wiki surfaces.

- [ ] **Step 4: Verify GREEN and commit**

Run:
- `uv run pytest -q tests/security`
- `make docs-check`
- strict MkDocs and wiki gates named by the repository

Commit: `docs: define security reporting and remediation (#92)`

---

### Task 6: Complete local verification and dual review

- [ ] **Step 1: Run focused and complete offline gates**

Run security tests, full `make test`, Ruff lint and scoped format check,
`make verify`, strict docs/site/wiki checks, YAML/config validation, and
`git diff --check`.

- [ ] **Step 2: Prove protected invariants**

Compare `uv.lock`, `pyproject.toml`, dataset registry, Atlas gitlink, protected
untracked files, task-owned containers, and preserved volumes to the base.

- [ ] **Step 3: Freeze exact review package**

Create an immutable `git diff --binary BASE..HEAD`, record byte count and SHA-256,
regenerate independently, and require byte identity.

- [ ] **Step 4: Obtain two independent final reviews**

Require one issue/spec/acceptance review and one quality/security/adversarial
review against the exact package. Promotion requires both to report
Critical 0 / Important 0 / Minor 0 / Ready Yes. Fix findings under strict TDD,
rerun proportional/full gates, rebuild the package, and repeat both reviews.

---

### Task 7: Promote through GitFlow and prove real GitHub analysis

- [ ] **Step 1: Push and merge feature to develop**

Push the reviewed exact head, open a ready PR to `develop` without a closure
keyword, wait for every required/advisory check, and merge without bypass.

- [ ] **Step 2: Merge develop to main**

Open a ready `develop` to `main` PR with `Closes #92`, wait for all checks, and
merge without bypass.

- [ ] **Step 3: Verify security analyses before closure**

Wait for the merged OSV full scan and CodeQL advanced workflow. Confirm GitHub
exposes current analyses for both `python` and `actions` on the merged SHA and
record exact run/analysis links in the issue closeout.

- [ ] **Step 4: Backsynchronize and verify final develop CI**

Open a zero-file `main` to `develop` PR, prove tree identity, wait for checks,
merge, and wait for the final develop push CI to complete successfully.

- [ ] **Step 5: Close and clean up**

Move #92 to Done only when GitHub analyses are visible, add an evidence-backed
closeout comment, remove local/remote feature refs and worktree safely, preserve
user-owned untracked paths, and confirm no open PRs, equal main/develop trees,
zero task containers, preserved volumes, and unchanged protected hashes.
