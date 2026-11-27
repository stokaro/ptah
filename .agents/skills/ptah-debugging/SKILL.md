---
name: ptah-debugging
description: Investigate a Ptah failure whose cause remains unclear after initial inspection, an intermittent defect, or a bug that persists after a fix. Use for tracing failures across schema, execution, provider, module, or process boundaries. Skip mechanical edits and fixes whose cause is already established.
---

# Diagnose a Ptah failure

## Establish a reproducible observation

Record expected and observed behavior, the smallest input, and the real command
or API that exposes the failure. Capture the failing revision and relevant
module resolution, build tags, engine/version, and environment. Read the error
and its cause before changing code. Keep secrets out of diagnostic output.

Separate a product failure from a missing toolchain, unavailable service, stale
generated file, or dependency that resolves to a published module. Use
[validation](../ptah-validation/SKILL.md) to select the first executable check.
A test that never reaches the affected behavior cannot diagnose that behavior.

Reduce the reproduction while retaining the condition that makes it fail. For
an intermittent failure, bound repeated runs by time or count and retain the
failing input, seed, and event order when available. A passing repetition does
not disprove the recorded failure.

## Locate the first incorrect boundary

Trace the failing input through its production caller. Compare observations on
both sides of the suspected boundary before expanding the search:

| Path | Evidence to compare |
| --- | --- |
| Parse, model, diff, plan, readback | Object identity, coverage, owner data, dependencies, and the first lost or changed fact |
| Host, SDK, provider | Actual module version, negotiated feature, encoded value, validation result, and selected service |
| Execution, revision state, recovery | Statement outcome, durable progress, connection ownership, and the returned error class |
| Concurrent or process work | Context propagation, ownership, event ordering, termination, and resource release |

Read the nearest working case and identify the input or environment difference
that explains the failing case. Add narrowly scoped instrumentation only when
existing logs, tests, or semantic tools cannot answer the question. Follow the
schema, provider, or Go skill for the affected contract.

For uncertain database execution, preserve the unknown-outcome classification
and inspect durable state through the existing recovery contract. Repeat a
mutation only in an isolated fixture or disposable test database with a known
starting state. A connection failure does not establish permission to replay.

## Test a specific cause

State one hypothesis, its supporting observation, and the result that would
disprove it. Change the smallest input or implementation detail that separates
that hypothesis from alternatives. Keep diagnostic changes separate from the
proposed fix.

Inspect the result before trying another hypothesis. Discard a rejected change
instead of stacking speculative patches. If the experiment cannot distinguish
causes, choose the missing observation or reassess the owning boundary. Record
a concrete blocker and continue any independent work that remains authorized.

## Repair and verify the production path

Turn the reproduction into the smallest regression that exercises the affected
public path. Follow the existing Quicktest conventions and
[regression policy](../../references/testing.md#every-fix-carries-a-regression-test).
Prove failure for the original defect and success with the repair. An assertion
that merely repeats a mock's configured result is not a regression.

Fix the cause at its owner. Remove temporary instrumentation and inspect the
complete diff for unrelated changes. Use the validation skill to expand from
the regression to affected consumers, modules, and engine evidence. Reuse
results while their inputs remain valid; do not repeat a full suite per edit.

Report the cause, evidence, repair, and any unverified condition. For a task
spanning several work sessions or independent changes, use
[agent workflows](../../references/agent-workflows.md) for a compact evidence
record and review handoff.
