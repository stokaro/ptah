---
name: ptah-validation
description: Plan and run bounded validation for Ptah changes, bug fixes, or failed CI. Use when choosing tests, proving a regression, changing a shared contract, or deciding which modules, build tags, engines, and platforms need evidence. Keep local diagnostic runs separate from the complete CI checks required for merge.
---

# Ptah validation

Choose checks from the behavior at risk and the current diff. Read
[validation policy](../../references/validation.md) when selecting full checks
or changing CI. Read [test standards](../../references/testing.md) before
adding or changing tests. Those references own the detailed rules.

## Define the evidence

1. Inspect the diff against the actual base and the affected callers. Name
   the failure being prevented and the result a test must observe.
2. Locate the owning module with `scripts/list-go-modules.sh`. Read the
   applicable workflow and Makefile recipe before choosing its command.
3. Select the smallest test that exercises the real entry point. Include
   adjacent consumers when a shared type, parser, renderer, planner, or
   protocol changes. A passing helper test does not prove its caller uses it.
4. Identify required evidence that needs another environment or repository.
   Preserve that requirement when local execution is unavailable.

| Change | First evidence | Expand when needed |
| --- | --- | --- |
| Internal Go logic | Named regression and owning package | Direct consumers of changed behavior |
| Parser, schema, renderer, or planner | Boundary inputs and deterministic output | Affected source formats, dialects, and downstream stages |
| SQL sent to an engine, transaction, catalog, or permission | Unit regression plus engine-level test | Read back the effect on each engine whose semantics differ |
| CLI, configuration, or compatibility | Real command path, accepted input, refusal, and output | Native, default compatibility, and strict mode where shared |
| Provider host or SDK | Owning module and provider contract fixture | Released dependency acceptance; live engine evidence in the provider repository |
| Files, subprocesses, locks, or concurrency | Failure and cleanup paths; targeted race run where relevant | Windows and other affected execution targets |
| Docs, agent instructions, or CI scripts | Changed links, command contracts, and applicable script checks | Site build for site effects; full checks when their inputs or selection change |

Read public API and architecture declarations when changing a shared boundary.
Include the corresponding guards. Do not lower lint thresholds, add skips,
or broaden exclusions to make a change pass.

## Keep the feedback loop bounded

Use these as planning checkpoints, not permission to omit required evidence:

- Aim for a focused run within two minutes on a warm cache. Set an explicit
  package timeout appropriate to the test. Go's `-timeout` does not bound
  dependency downloads or compilation; track elapsed wall time too.
- Plan roughly fifteen minutes for local validation of an ordinary change.
  If setup or a known slow suite exceeds that, identify the remaining risk
  and use the existing CI job for the complete run. Do not launch the same
  expensive suite locally and remotely without a diagnostic reason.
- Check the Go executable, the version directives in `go.mod`, dependencies,
  and required services before a costly run. Report an unavailable toolchain
  or blocked download once. Do not downgrade a module or fake a service.
- Retry a failure after changing its cause or establishing that it was
  transient. Do not rerun an unchanged timeout or deterministic failure.
- Recheck the final relevant diff. Reuse results only while the code, test
  inputs, tags, dependency graph, and environment they cover remain valid.
  Expand testing for a concrete remaining risk, then stop optional reruns.

Start with a named test, then run the owning package and relevant consumers.
Avoid `-v` for a broad passing run. Keep full failure logs outside tracked
files and report the failed test, cause, and useful excerpt.

Choose the test name from the code. Confirm that a `-run` filter executes
the intended test; a successful command with no matching tests proves nothing.
Use a brief verbose run when the execution cannot otherwise be confirmed.

## Prove the regression

For a bug fix, run the new test against the defective behavior and the fix.
The failing run must fail for the defect, not compilation, missing fixtures,
or an unavailable service. Disable only the fix in your own worktree, or use
an isolated worktree containing the regression test. Restore the fix before
the final run and inspect the diff for accidental reversions.

Use `-count=1` for this proof and when tests read state the Go test cache does
not reliably track, such as a Git file listing or a live database. In
particular, run `./internal/doccommands` uncached after documentation changes.
Keep the normal Go cache during ordinary package iteration; clearing the
build cache or adding `-count=1` to every repeated broad run wastes work.

A documentation correction needs an appropriate content check, not an
invented runtime regression. A changed validation script needs a fixture
that exercises its failure as well as its success.

## Respect execution boundaries

- A root `go test ./...` does not test nested modules. Use `go -C` for the
  affected module and discover the full set before claiming repository-wide
  coverage. Read `.github/workflows/go-module-tests.yml` for its independent
  module builds, tests, and integration runner.
- The host can depend on released SDK or SQL syntax modules rather than the
  sibling checkout. Inspect `go.mod` and `provider-sdk/README.md` before
  interpreting a passing host test. Keep temporary workspaces uncommitted;
  released acceptance uses `GOWORK=off` and the declared dependencies.
- Discover build tags with `scripts/list-build-tags.sh`. Default and tagged
  builds can select different implementations. Cover both when affected;
  adding the integration tag to a broad local run starts a different suite.
- Match evidence to the target. A Linux run does not establish Windows
  behavior, and a `js/wasm` build does not establish runtime behavior.
  Use the existing jobs for targets unavailable locally.
- Keep engine tests under `integration/`, with the integration build tag,
  `dbtarget` connections, and cleanup of everything they create. Do not run
  tests concurrently against shared databases when their cleanup conflicts.

## Separate diagnostics from complete checks

A focused tagged `go test` is local diagnostic evidence only. It must not
replace the integration contour or be described as a complete integration
pass. `internal/cmd/testcontour` owns the full listing, shard selection,
execution, and verification. Keep its no-skip and coverage checks intact.

Use the existing Makefile recipes and workflow commands. Do not create
another test discovery or reporting wrapper. Let `scripts/ci-diff-scope.sh`
control the compiled CI checks and retain their visible aggregate gates.
An elapsed local budget does not authorize changing CI scope or timeouts.

Author the required live regression even when it cannot run locally. Name
the pending engine, platform, or provider check in the PR and wait for its
result before claiming that behavior is verified. Treat a failed required
check as unresolved; a bounded local run is not a merge waiver.

## Report what ran

Record the command, scope, result, and relevant environment for each distinct
claim. Separate passes, failures, and checks not run. For a blocked check,
give the concrete blocker and the existing CI job or companion repository
that must supply the evidence. Name any unverified success condition.

Inspect CI results for the current PR head. Use existing results and check
again when the head changes or a pending job finishes; avoid repeated log
downloads and tight polling. Do not describe an older head's pass, a skipped
job, compilation, or lint as proof of behavior it did not exercise.
