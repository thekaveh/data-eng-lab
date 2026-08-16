# Security policy

## Supported versions

This repository has not published versioned releases. Security fixes are applied
to the current default branch only.

| Version | Supported |
|---|---|
| `main` | Supported |
| Other branches, tags, and forks | Not supported |

## Report a vulnerability privately

Use [GitHub private vulnerability reporting](https://github.com/thekaveh/data-eng-lab/security/advisories/new)
when that control is available. Include the affected component and revision,
reproduction prerequisites, impact, and a minimal proof that contains no real
credentials or unrelated data.

Do not open a public issue, discussion, or pull request for an unremediated
vulnerability, leaked secret, or exploit. If private vulnerability reporting is
not available, use the [maintainer profile](https://github.com/thekaveh) to
request a private channel without including vulnerability details.

Expect an initial acknowledgement within three business days. Remediation and
disclosure timing depend on reproducibility, impact, dependency availability,
and coordination with affected upstream projects. The maintainer will provide
status updates in the private channel until resolution or documented closure.

## Remediation and disclosure

The maintainer validates the report at the smallest safe boundary, identifies
affected supported revisions, and prepares a reviewed fix through the
repository's feature-to-`develop`-to-`main` GitFlow. Public disclosure is
coordinated only after a fix or effective mitigation is available. Reports that
concern the pinned Atlas submodule are coordinated with the Atlas project rather
than patched silently in this consumer repository.

See the [security automation runbook](../docs/security-automation.md) for scanner
coverage, triage, exceptions, and the dependency-update workflow.
