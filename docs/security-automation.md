# 9.1. Security Automation

This runbook defines the repository-owned dependency update, known-vulnerability
audit, source-analysis, reporting, and remediation boundaries. It does not claim
coverage for the pinned Atlas submodule or for languages unsupported by the
configured scanners.

## 1. Automation inventory

| Control | Authority | Trigger | Result |
|---|---|---|---|
| Dependabot | `.github/dependabot.yml` | GitHub's weekly ecosystem update cadence | Version-update pull requests targeting `develop` |
| OSV-Scanner | `.github/workflows/dependency-security.yml` | Pull request, merged `develop`/`main` push, or manual dispatch | Failing known-vulnerability audit; SARIF after merge/manual dispatch |
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

Every OSV finding is actionable by default. The pull-request scan fails before
merge; merged and manually dispatched scans also upload SARIF to GitHub code
scanning. The policy does not rely on a severity threshold because vulnerability
records do not expose one uniform severity scheme across ecosystems.

The event-only OSV and CodeQL workflow does not provide continuous late-disclosure detection.
A vulnerability published after the last repository event is not discovered
until another event or manual dispatch. Until issue #93 enables repository
Dependabot alerts, manually dispatch the dependency audit at least every seven days;
do not proceed past #92 to another backlog item except #93. Once #93 proves the
repository settings, Dependabot alerts become the continuous compensating monitor.
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

Issue #93 owns repository security settings such as the dependency graph,
Dependabot security updates, private vulnerability reporting, secret scanning,
and push protection. #92 defines repository files and analysis workflows only;
it must not report those settings as enabled before #93 proves them.

## 8. Closure evidence

#92 is complete only after focused and full local gates pass, two independent
reviews return no findings, GitFlow promotion and backsynchronization finish,
OSV completes on merged code, CodeQL exposes current `python` and `actions`
analyses, final `develop` CI is green, and protected repository state remains
unchanged.
