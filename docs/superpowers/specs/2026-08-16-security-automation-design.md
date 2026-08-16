# Dependency and code-scanning automation design

**Issue:** #92  
**Date:** 2026-08-16  
**Status:** Approved by the controlling autonomous goal directive

## 1. Purpose and boundary

This design adds parent-repository dependency updates, known-vulnerability
auditing, CodeQL analysis, and a security-reporting policy. It covers every
current dependency surface owned by this repository: the root `uv.lock`, GitHub
Actions workflows, and the six Maven Spark applications. It does not scan or
modify the pinned Atlas submodule, generated site/wiki output, Maven build
output, user-owned graph data, or an untracked working copy.

The work is offline-safe except for the scanners' ordinary CI downloads and
vulnerability-database queries. It does not start Atlas, run live acceptance,
change the dataset registry, create a release, alter the Atlas gitlink, or
enable repository security settings owned by issue #93.

## 2. Current inventory and ownership

The dependency inventory is closed and exact:

- Python: `/uv.lock` (managed by uv through `/pyproject.toml`);
- GitHub Actions: `/.github/workflows/`;
- Maven:
  - `/spark-apps/gh-archive-pipeline/pom.xml`;
  - `/spark-apps/movielens-feature-pipeline/pom.xml`;
  - `/spark-apps/nyc-taxi-data-quality/pom.xml`;
  - `/spark-apps/nyc-taxi-etl/pom.xml`;
  - `/spark-apps/nyc-taxi-medallion/pom.xml`; and
  - `/spark-apps/tpch-star-schema/pom.xml`.

`infra/` is a pinned submodule and remains an upstream-owned trust boundary.
`site/`, `wiki/`, `target/`, caches, and `graphify-out/` are generated,
vendored, build, or user-owned surfaces and are not dependency authorities.

## 3. Considered approaches

### 3.1. Selected: exact-manifest OSV plus advanced CodeQL

Dependabot maintains the three package ecosystems. A pinned OSV-Scanner
reusable workflow scans only the seven exact dependency manifests on pull
requests, merged branches, and manual dispatch. Advanced
CodeQL analyzes the two supported source classes present here: Python and
GitHub Actions workflows.

This approach is ecosystem-neutral, supports `uv.lock` and Maven POM resolution,
emits GitHub code-scanning results after merge, and makes exclusions explicit
by never recursively scanning the repository.

### 3.2. Rejected: split pip-audit and OWASP Dependency-Check

Separate scanners would provide ecosystem-specific reports, but introduce two
policies, an NVD state/download path, and more cache and failure modes. They do
not improve the closed-manifest coverage needed by this repository.

### 3.3. Rejected: dependency review alone

GitHub dependency review is useful for changed dependencies, but it depends on
dependency-graph settings owned by #93 and does not independently prove the
complete current lock/POM set. It is not a substitute for #92's full audit.

## 4. Dependabot contract

`.github/dependabot.yml` defines one weekly update entry for GitHub Actions at
`/`, one for uv at `/`, and Maven entries for exactly the six application
directories above. Version-update PRs target `develop`, use bounded open-PR
limits, and group safe routine updates within each ecosystem.

GitHub security updates are different: GitHub always opens them against the
default branch and does not apply all version-update customizations. Because
this repository uses GitFlow, maintainers must not merge a default-branch
security update directly. They reproduce or supersede it on a feature branch
from `develop`, merge feature to `develop`, promote `develop` to `main`, and
backsynchronize `main` to `develop`. The security policy and runbook make this
platform constraint explicit.

A repository contract test derives current parent-owned POM directories and
requires exact equality with Dependabot's Maven directories, so adding or
removing an application cannot silently leave configuration stale.

## 5. Dependency-vulnerability audit contract

`.github/workflows/dependency-security.yml` calls OSV-Scanner v2.5.0 at immutable
commit `8deb546fdb875b9996d27d4950be7312dac076a1`. Scan arguments contain only
seven `--lockfile=` values: the root uv lock and six POMs. Recursive flags and
repository-directory operands are forbidden.

Pull requests to `develop` or `main` run a fail-on-vulnerability full-manifest
scan without SARIF upload and with `contents: read` only. Pushes to `develop`
or `main` and manual dispatch run the same fail-on-
vulnerability scan with `contents: read`, `actions: read`, and
`security-events: write`, and upload SARIF. Thus a PR cannot gain code-scanning
write permission, while a merged analysis can satisfy the issue's GitHub
analysis criterion.

Every detected vulnerability is actionable by default, independent of severity,
because OSV records frequently lack uniform severity metadata. A temporary
exception must name an exact vulnerability ID and package, explain why the
repository is not affected or cannot yet upgrade, name an owner, and include a
UTC expiry. Expired or broad exceptions are forbidden and tested. No exception
is introduced by this issue.

OSV Maven resolution covers production dependency graphs available from the
POMs. OSV does not currently promise complete Maven test-dependency coverage;
that limitation is documented rather than silently overstated.

## 6. CodeQL contract

`.github/workflows/codeql.yml` is an advanced setup using CodeQL Action v4 at
immutable commit `ff2f1c621b7f889edc0d3c761ac2e6a3f8cdb0dd`. It runs on pull
requests and pushes for `develop` and `main`, and by manual dispatch.
The matrix contains exactly `python` and `actions`; both use `build-mode: none`
and `security-extended` queries. Checkout is pinned, does not persist
credentials, and does not initialize submodules.

The job grants only `contents: read`, `actions: read`, and
`security-events: write`. The CodeQL configuration excludes `infra/`, generated
documentation, build output, caches, and `graphify-out/`.

GitHub CodeQL does not support Scala. Selecting `java-kotlin` would not analyze
the six Scala Spark applications and would create a false coverage claim, so
the workflow does not do so. Known dependency vulnerabilities in those Maven
applications are covered by OSV; Scala source-code vulnerability analysis is
explicitly unavailable until GitHub adds support or a separately reviewed
scanner is adopted.

## 7. Security reporting and remediation

`.github/SECURITY.md` identifies `main` as the only supported line until the
repository publishes releases. It asks reporters to use GitHub private
vulnerability reporting once #93 enables it and prohibits public issues for
unfixed vulnerabilities or secrets. If the private reporting control is not
visible, reporters contact the maintainer with only a request for a private
channel—not vulnerability details.

Maintainers acknowledge a private report, reproduce it without exposing secrets,
classify affected surfaces, prepare the smallest reviewed fix through GitFlow,
and coordinate disclosure only after remediation. Scanner findings follow the
same process. The runbook describes safe exception handling, Dependabot's
default-branch behavior, SARIF triage, false-positive documentation, and
escalation.

## 8. Repository contract and documentation

A small standard-library validator under `scripts/security/` parses the three
security configurations with strict, duplicate-safe YAML handling through the
repository's existing PyYAML dependency. It enforces the exact manifest set,
triggers, immutable action pins, least permissions, scan operands, CodeQL
languages/exclusions, bounded schedules, and security-policy links. Its CLI is
read-only and is exercised by the normal static/unit suite.

`docs/security-automation.md` is the canonical operational source and is added
to the documentation manifest. Site and wiki projections are generated from it.
The document distinguishes dependency updates, known-vulnerability scans,
source analysis, and unsupported surfaces so no dashboard or workflow is
described as broader than it is.

## 9. Testing and acceptance

Strict TDD covers duplicate YAML keys, missing/extra POMs, recursive scan
arguments, mutable action references, excessive permissions, unsafe triggers,
wrong target branches, unsupported CodeQL languages, absent exclusions,
malformed exception records, and stale documentation. The validator also runs
against the committed files.

Verification includes focused security tests, the full offline suite, Ruff
lint/format checks, strict documentation/site/wiki gates, `make verify`, YAML
parsing, range diff checks, protected hashes, the unchanged Atlas gitlink, zero
task-owned containers, and preserved volumes. After GitFlow promotion, #92 is
not closed until the advanced CodeQL workflow has completed on merged code and
GitHub exposes an analysis for both configured languages, the OSV merged scan
has completed, the final `develop` CI is green, and the trees are backsynchronized.

## 10. Primary references

- [OSV-Scanner GitHub Action](https://google.github.io/osv-scanner/github-action/)
- [OSV supported manifests](https://google.github.io/osv-scanner/supported-languages-and-lockfiles/)
- [GitHub Dependabot configuration](https://docs.github.com/en/code-security/dependabot/working-with-dependabot/dependabot-options-reference)
- [GitHub CodeQL supported languages](https://docs.github.com/en/code-security/concepts/code-scanning/codeql/codeql-code-scanning)
- [GitHub advanced CodeQL setup](https://docs.github.com/en/code-security/code-scanning/creating-an-advanced-setup-for-code-scanning)
- [GitHub security policies](https://docs.github.com/en/code-security/getting-started/adding-a-security-policy-to-your-repository)
