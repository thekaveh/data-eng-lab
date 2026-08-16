# Repository Security Protections Implementation Plan

> **Execution:** complete the tasks in order with strict RED/GREEN evidence.
> This issue adds no recurring workflow and promotion requires dual C0/I0/M0
> Ready Yes review verdicts through the repository's GitFlow path.

## Task 1: Freeze the evidence contract

1. Add tests for size, UTF-8, duplicate keys, depth, node count, canonical UTC,
   repository identity, commit identity, closed settings, endpoint facts,
   analysis categories, limitations, and push-probe cleanup.
2. Run the focused tests and record the missing-module RED.
3. Implement the smallest pure validator and CLI.
4. Run focused tests, Ruff, formatter, and diff checks.

## Task 2: Document the operator procedure

1. Add documentation tests for exact read-before-write API commands, the
   selected state, safe official dummy-token probe, failure cleanup, settings
   URL, and unsupported-feature disclosure.
2. Extend the canonical security runbook and three-surface manifest without
   duplicating mutable evidence.
3. Add the final secret-free evidence JSON after the live proof.
4. Run strict repository/site/wiki documentation gates.

## Task 3: Enable and verify GitHub protections

1. Capture the authoritative disabled pre-state.
2. Enable vulnerability alerts, Dependabot security updates, secret scanning,
   and push protection one at a time through documented APIs.
3. Attempt non-provider patterns and validity checks separately; preserve an
   accepted enabled state or record the exact unsupported plan/policy result.
4. Read the post-state, Dependabot endpoints, secret-alert endpoint, and current
   CodeQL analyses.
5. Run the official dummy-token push probe against a unique temporary ref,
   require rejection, and prove the remote ref absent. If it unexpectedly lands,
   delete only that exact ref immediately.
6. Validate the canonical evidence with the shipped CLI.

## Task 4: Verify and review

1. Run focused security/docs tests and the full offline suite.
2. Run Ruff lint/format, verify, strict docs/wiki, Compose validation, and
   protected-state checks. Carry Maven evidence only if JVM/POM inputs are
   byte-identical; otherwise rerun all six Java 17 apps.
3. Freeze an exact binary diff and obtain two independent C0/I0/M0 Ready Yes
   reviews. Resolve findings under new RED tests before promotion.

## Task 5: Promote and close

1. Push the reviewed feature branch and merge a ready feature-to-develop PR only
   after every required check is green.
2. Merge a ready develop-to-main PR containing `Closes #93` only after every
   required and advisory check is green.
3. Merge a proved zero-file main-to-develop backsync and wait for final develop
   CI and security analyses.
4. Record issue evidence, set Project #7 to Done, remove exact local/remote
   feature refs and the clean worktree, and preserve user-owned files.
