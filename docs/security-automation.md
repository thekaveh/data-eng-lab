# 9.1. Security Automation

This runbook defines the repository-owned dependency update, known-vulnerability
audit, source-analysis, reporting, and remediation boundaries. It does not claim
coverage for the pinned Atlas submodule or for languages unsupported by the
configured scanners.

## 1. Automation inventory

| Control | Authority | Trigger | Result |
|---|---|---|---|
| Dependabot | `.github/dependabot.yml` | GitHub's weekly ecosystem update cadence | Version-update pull requests targeting `develop` |
| OSV-Scanner | `.github/workflows/dependency-security.yml` | Pull request, merged `develop`/`main` push, or manual dispatch | Failing PR regression audit; complete SARIF baseline after merge/manual dispatch |
| CodeQL | `.github/workflows/codeql.yml` | Pull request, `develop`/`main` push, or manual dispatch | Python and GitHub Actions code-scanning analyses |
| Reporting policy | `.github/SECURITY.md` | Reporter action | Private intake and coordinated remediation |

Validate the complete repository contract locally:

```bash
uv run python -m scripts.security.contract --root .
```

The command is read-only, emits `security_contract_ok` on success, and fails
closed when dependency paths, workflow permissions, triggers, action pins,
languages, or exclusions drift.

## 2. Dependency authority

OSV scans exactly these parent-owned manifests:

- `uv.lock`;
- `datasets/tpch-lock-requirements.txt`;
- `spark-apps/gh-archive-pipeline/pom.xml`;
- `spark-apps/movielens-feature-pipeline/pom.xml`;
- `spark-apps/nyc-taxi-data-quality/pom.xml`;
- `spark-apps/nyc-taxi-etl/pom.xml`;
- `spark-apps/nyc-taxi-medallion/pom.xml`; and
- `spark-apps/tpch-star-schema/pom.xml`.

Dependabot covers both Python authorities, the Maven authorities, and GitHub
Actions at the repository root. Configuration tests derive the current Maven
application directories and require exact equality, while the hashed TPC-H
requirements file is a fixed parent-owned inventory member. A new or removed
POM cannot silently remain unconfigured.

The scanners never recurse from `./`. They do not scan `infra/` (the pinned
Atlas submodule), `site/`, `wiki/`, generated documentation, Maven `target/`
output, caches, or user-owned `graphify-out/` data. Those exclusions are
ownership boundaries, not evidence that the excluded content is safe.

## 3. Vulnerability policy

Every OSV finding is actionable by default. A pull request scans both its exact
target revision and proposed revision, then fails only when the proposal adds a
vulnerability. This is OSV's supported differential PR model and prevents the
repository's inherited Spark and protected-lock baseline from making every PR
permanently red. The target result is SHA-256-bound before the proposed checkout
and its type, size, JSON structure, and digest are revalidated afterward; the
proposed result and SARIF paths must still be absent. A proposed tree entry
therefore cannot replace or alias the trusted baseline. Merged and manually
dispatched scans report the complete
baseline and upload SARIF to GitHub code scanning without converting existing
findings into a workflow-infrastructure failure. Neither path relies on a
severity threshold because vulnerability records do not expose one uniform
severity scheme across ecosystems.

The differential gate is not an exception or an assertion that baseline
findings are safe. Triage each current finding through the owning dependency
surface and remediate it in a separately reviewed change when its authority can
move. Do not weaken the PR comparison or delete a SARIF result to obtain a green
check.

The event-only OSV and CodeQL workflow does not provide continuous late-disclosure detection.
A vulnerability published after the last repository event is not discovered
until another event or manual dispatch. Repository Dependabot alerts are now the
continuous compensating monitor; issue #93 proved the setting through the API
and observed both version-update and security-update pull requests. Manually
dispatch the dependency audit when investigating a newly disclosed issue or
when authoritative Dependabot state is unavailable.
Workflow scans remain the merge and source-analysis evidence rather than a
recurring workflow.

Triage a finding in this order:

1. record the OSV identifier, package, installed version, manifest, and workflow
   run without copying credentials or environment output;
2. confirm the affected dependency path and whether the vulnerable behavior is
   reachable in this repository;
3. prefer an upstream fixed release and regenerate the owning lock or POM through
   its normal toolchain;
4. run focused tests, the complete offline gate, and both independent reviews;
5. promote the fix through GitFlow and confirm the merged scan; and
6. close or document the alert only after the merged revision is authoritative.

A temporary exception must name one exact vulnerability ID and package, state
the reachability evidence and compensating control, name an owner, and include a
UTC expiry. Broad package, ecosystem, directory, or severity ignores are
forbidden. This issue provides no executable exception mechanism and ships no
exceptions. Adding one requires a separate reviewed change that defines a strict
record schema, validates expiry, and tests fail-closed enforcement before any
exception can be accepted.

OSV resolves Maven production dependencies available from each POM. Complete
Maven test dependencies are not currently guaranteed by OSV's computed Maven
graph and therefore remain a documented limitation.

## 4. Dependabot and GitFlow

Routine version-update pull requests target `develop`. GitHub security updates
are different: the platform creates those pull requests against the default branch
and does not apply every version-update customization.

Do not merge a default-branch security pull request directly. Reproduce or
supersede the update on a feature branch from `develop`, verify it, merge feature → develop → main, then open a zero-file `main` → `develop`
backsynchronization pull request. Link the superseding pull request before
closing the platform-generated update.

## 5. CodeQL coverage

Advanced CodeQL analyzes exactly `python` and `actions` with the
`security-extended` query suite. Pull requests and merged branches use the same
configuration; post-merge analyses are visible in the repository's code-scanning
view.

CodeQL does not support Scala. The `java-kotlin` language selector would not
analyze the six Scala Spark applications and must not be used as a substitute.
OSV covers known vulnerable Maven dependencies, but Scala source-code security
analysis remains unavailable until GitHub adds support or a separate reviewed
scanner is adopted.

## 6. Reporting and remediation

Follow the repository [security policy](../.github/SECURITY.md). Keep an
unremediated report, exploit, or secret out of public issues and pull requests.
Reproduce with synthetic data and the smallest affected component. Rotate any
credential exposed during reporting before continuing technical investigation.

For a parent-owned defect, prepare the smallest fix from `develop`, add a
regression test that proves the original failure, complete the normal review and
GitFlow path, and coordinate disclosure after mitigation. For an Atlas defect,
report upstream privately and consume a reviewed Atlas release through a
separate gitlink-advance lifecycle; do not patch `infra/` in place.

## 7. Permissions and supply-chain controls

Every action or reusable workflow is referenced by a full commit SHA. Pull-
request OSV scanning receives only `contents: read`. SARIF-producing OSV and
CodeQL jobs receive `contents: read`, `actions: read`, and
`security-events: write`; no job receives repository-content write, package,
deployment, identity-token, or secret permission.

Issue #93 enabled and verified the selected repository settings. #92 defines
repository files and analysis workflows only; workflow files are not settings
evidence.

## 8. Repository security protections

The authoritative post-change state is:

| Protection | State | Evidence |
|---|---|---|
| Vulnerability alerts | Enabled | `GET /repos/thekaveh/data-eng-lab/vulnerability-alerts` returns HTTP 204 |
| Dependabot security updates | Enabled | The automated-security-fixes endpoint returns `enabled: true`; version PRs #140 through #148 and security PRs #149 and #150 are recorded separately |
| Secret scanning | Enabled | The repository settings object reports enabled and the alerts endpoint is readable |
| Push protection | Enabled | GitHub rejected the official dummy-token probe with `GH013` and `GITHUB PUSH PROTECTION` |
| Non-provider patterns | Unsupported | GitHub retained disabled after an enable request |
| Partner validity checks | Unsupported | GitHub retained disabled after an enable request |
| Code scanning | Active | Actions analysis `1624984013` and Python analysis `1624984946` are visible through the analyses API at commit `45512297ed0b31837c01bdf8222e3a521bea7362` |

The enabling operations were the documented
`PUT /repos/thekaveh/data-eng-lab/vulnerability-alerts`,
`PUT /repos/thekaveh/data-eng-lab/automated-security-fixes`, and narrowly scoped
`PATCH /repos/thekaveh/data-eng-lab` requests for each `security_and_analysis`
field. Always read the current state before a write and read it back afterward.
Use `GET /repos/thekaveh/data-eng-lab/secret-scanning/alerts` and
`GET /repos/thekaveh/data-eng-lab/code-scanning/analyses` to prove the server-side
features, rather than inferring them from repository files. The authoritative
UI is [Security and analysis settings](https://github.com/thekaveh/data-eng-lab/settings/security_analysis).

Scanning for non-provider patterns and automatic partner validity checks
requires an organization-owned repository on GitHub Team or Enterprise with
Secret Protection. This public user-owned repository cannot enable either
feature: both API attempts completed without error but the authoritative state
remained disabled. Moving the repository to an eligible organization and
licensing Secret Protection is the required authority change. Private
vulnerability reporting remains outside issue #93 and is not claimed enabled.

### Safe push-protection probe

Use only [GitHub's published dummy token](https://docs.github.com/en/get-started/learning-to-code/storing-your-secrets-safely),
never a live or generated credential. Create one unique temporary local branch
and exact remote probe ref, commit the fixture in a dedicated file, and attempt
the push. Require GitHub to reject it with `GH013`, `GITHUB PUSH PROTECTION`, and
`Push cannot contain secrets`. Cancel the operation and never bypass the rule.
Then prove the exact remote probe ref is absent with `git ls-remote --heads`,
remove the temporary worktree and local branch, and confirm the secret-alert
count did not increase. If the ref unexpectedly lands, delete only that exact
remote ref immediately. This process uses no real credential.

The secret-free canonical evidence is
`docs/evidence/repository-security-protections.json`. Validate it locally:

```bash
uv run python -m scripts.security.repository_protections \
  --evidence docs/evidence/repository-security-protections.json
```

Success emits only `repository_security_ok`. The evidence binds the exact
settings, readable APIs, exact Dependabot pull-request numbers, exact CodeQL
analysis IDs, and the exact probe ref and commit
(`refs/heads/codex/93-push-protection-probe-20260816T0640Z` at
`31baba903f63ff457d19659c74af40b0f5869245`). It also binds the official dummy
probe outcome, cleanup, and plan limitations without storing a token or raw API
response.

## 9. Closure evidence

#92 is complete only after focused and full local gates pass, two independent
reviews return no findings, GitFlow promotion and backsynchronization finish,
OSV completes on merged code, CodeQL exposes current `python` and `actions`
analyses, final `develop` CI is green, and protected repository state remains
unchanged.

#93 is complete only after the evidence validator and documentation gates pass,
two independent reviews return C0/I0/M0 Ready Yes, GitFlow promotion and
backsynchronization finish, final API reads and CI are green, and every temporary
probe and feature ref is absent.
