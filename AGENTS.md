# AGENTS.md

Ptah generates, compares, plans, and applies database schemas and migrations.
These are the required rules for every coding task. Load the matching skills
below for the detailed policy; do not read every skill or reference up front.
Paths in code spans are relative to the repository root.

## Where work happens

**Work only in a linked git worktree, never in the main one.** Start from
`origin/master`, with a task-named branch and worktree:

```bash
git fetch origin
git worktree add .codex/worktrees/<task> -b <branch> origin/master
```

`.claude/worktrees/<task>` is the equivalent location for Claude. Inspect
`git status --short` before editing. Never remove another agent's worktree or
overwrite its changes. After a merge, confirm the content reached master
before removing your worktree and deleting its branch, including squash merges.

## Start with the task

1. Establish the observable outcome and the affected boundary. Inspect the
   nearest implementation, its real caller, and its tests before designing.
2. Use `rg --files`, scoped `rg`, and bounded reads. Follow symbols outward
   when evidence requires it. Do not load whole ledgers, all ADRs, generated
   code, or all documentation for a local fix.
3. Identify the owning Go module, entry point, engine or provider, native or
   compatibility surface, and relevant generated artifacts. For cross-cutting
   work, note the dependencies and test evidence in a short plan.
4. Implement the smallest complete change at the existing ownership boundary.
   Do not add speculative frameworks, parallel implementations, or unrelated
   cleanup. Fix the cause rather than weakening a refusal or assertion.
5. Give parallel agents independent questions or disjoint file ownership.
   Share paths, findings, and test results instead of repeated repository
   surveys. Run shared-database checks serially.

Keep an evidence record: command, result, relevant diff or commit, environment,
and remaining uncertainty. Reuse a valid result until its inputs change.
Inspect failed output once; retry only after identifying a changed cause.

## Load task-specific guidance

Read the skill for each affected boundary. Start with Go development for Go
edits, add a domain skill when applicable, and use validation to select checks.
Load documentation guidance when documented behavior or documentation changes;
load review guidance when preparing a PR or issue. Each skill links only the
references its task needs.

| Task | Skill |
| --- | --- |
| Go code, errors, resources, APIs, or portability | [ptah-go-development](.agents/skills/ptah-go-development/SKILL.md) |
| Unexplained failure, intermittent defect, or a fix that did not work | [ptah-debugging](.agents/skills/ptah-debugging/SKILL.md) |
| SQL, inspection, schema identity, diff, planning, or execution | [ptah-schema-migrations](.agents/skills/ptah-schema-migrations/SKILL.md) |
| Provider protocol, host/SDK, or independent module changes | [ptah-provider-contracts](.agents/skills/ptah-provider-contracts/SKILL.md) |
| CLI, configuration, Atlas parity, or environment bindings | [ptah-compatibility](.agents/skills/ptah-compatibility/SKILL.md) |
| Tests, CI, regression evidence, or validation scope | [ptah-validation](.agents/skills/ptah-validation/SKILL.md) |
| Documentation, examples, or generated references | [ptah-documentation-maintenance](.agents/skills/ptah-documentation-maintenance/SKILL.md) |
| Review, PR description, or issue | [ptah-review](.agents/skills/ptah-review/SKILL.md) |

`.agents/skills` holds the instructions. `.claude/skills` holds small discovery
adapters with matching metadata. `CLAUDE.md` imports only this file. Keep the
root under 12 KiB and each skill under 8 KiB; move task details into conditional
references. Check changes with
`node docs/site/scripts/check-agent-guidance.mjs`.

For work spanning several contracts, use the
[workflow guidance](.agents/references/agent-workflows.md) to choose planning
and review depth. External skills and tool instructions supplement these
rules; check their examples against the repository's owners and test policy.

## Architecture and data safety

- Read declarations rather than copying module, dialect, version, or tag
  lists. Start with the [repository map](.agents/references/architecture.md)
  when the owner is unclear.
- Keep neutral models, orchestration, and shared parsers independent of
  concrete owners. Dispatch engine behavior through existing descriptors,
  capabilities, and runtime services. Follow
  [architecture boundaries](docs/architecture_boundaries.md).
- Incomplete inspection is not deletion intent. Preserve coverage, structured
  identity, quoting, scope, and dependency order through the schema pipeline.
  Unsupported semantics must not be silently discarded.
- A failed execution can have committed effects. Preserve durable progress
  and unknown-outcome errors; never authorize replay from an uncertain result.
- DuckDB, Spanner, and YDB engine knowledge and live tests belong to their
  provider repositories. Host fixture tests prove the provider contract only.

## Compatibility policy

Preserve the [complete compatibility policy](.agents/references/compatibility.md):
**(a)** never accept and ignore input the Atlas community binary enforces;
**(b)** match observable contracts without copying defects;
**(c)** keep implemented capabilities reachable in default compatibility mode.
`PTAH_ATLAS_STRICT_COMPAT=1` selects the CE-only conformance policy.

Pre-v1 cleanup does not require aliases for older Ptah shapes. It does not
permit losing user data, breaking Atlas parity, or bypassing the reviewed
Stable Embedder API approval in [the public API ledger](docs/public_api.md).

### Native and compatibility capability ownership

General capabilities live below both CLIs and are reachable natively.
Compatibility spelling, codecs, and policy belong in the adapter. Native and
shared code never import `internal/cli/atlas`; this boundary needs review
because the architecture gate does not enforce it. Declare compat additions
as `GENERAL CAPABILITY` or `COMPATIBILITY ADAPTER` in the PR.

### The native CLI surface

`ptah` groups verbs into namespaces; built `--help` is the flag authority.
There is no `ptah generate`, `ptah compare`, `ptah read-db`, `ptah drop-all`
or `ptah migrate`, and none may be added. Atlas spellings live in `ptah-compat`.
`--dry-run` belongs to commands that write. A help line's `[env: PTAH_...]`
marker declares an environment binding. Resolve owned boolean variables
strictly before early returns, and disable a forwarded native binding when
its adapter has already resolved the value. Rendering stays deterministic.

## Code style and linting

Use plain American English in code, comments, docs, issues, and PRs. Follow
relevant sections of [the style guide](docs/STYLE_GUIDE.md) and
[Go policy](.agents/references/go-style.md). Prefer existing Go idioms and
helpers over new abstractions. Preserve error identity and resource ownership.

`.golangci.yml`, architecture budgets, generated-file checks, API approvals,
and test-style baselines are contracts. Fix code instead of weakening them.
Discover modules with `scripts/list-go-modules.sh` and build tags with
`scripts/list-build-tags.sh`. Root-module tests do not cover nested modules
or prove that a published dependency pin includes an in-tree SDK edit.

## Building and testing

Use [validation guidance](.agents/skills/ptah-validation/SKILL.md) to separate
fast local feedback from complete merge evidence. Start with the regression
and affected packages. Broaden when a shared contract, tags, platforms,
engines, or observed failures require it. Budget elapsed time; report blocked
or deferred checks and use applicable CI results at the tested revision.
A local budget never waives a required test or merge gate.

Keep the existing entry points in [TESTING.md](TESTING.md). CI scope belongs
to `scripts/ci-diff-scope.sh` and the workflows, not an agent's guess. Unknown
scope runs checks. Do not add workflow-level `paths:` filters or a second
test orchestrator. Never use daemon-wide Docker cleanup for a local task.

### Testing standards

Behavioral fixes carry a regression that fails without the fix and passes
with it, through the affected public path. Record the missing input shape.
Changed SQL execution, engine-dependent constraints, or database effects
also need engine-level evidence; SQL-string assertions alone do not prove it.
Provider contract and live-provider evidence are separate claims.

Go tests use Quicktest as `qt`, black-box packages by default, declarative data
rows, and separate success/failure cases. Do not branch inside test functions
or hide assertion choices in helpers. Integration tests are tagged under
`integration/`, use `dbtarget`, and clean up all created state. Follow the
[test design policy](.agents/references/testing.md) for exact conventions,
white-box exceptions, regression controls, and contour completeness.

## Documentation obligations

Update documentation with changes to CLI/config behavior, SQL, migrations,
public APIs, diagnostics, safety checks, or compatibility claims. Search both
old and new terms across affected surfaces. Edit generated sources, then
regenerate; never repair their output by hand. The
[documentation policy](.agents/references/documentation.md) names owners.
Read style-guide sections relevant to the change, then choose local and CI
evidence through the documentation skill. A prose edit does not require
repeating unrelated engine or browser suites locally.

## PR and issue writing

Write a plain-English title and opening that explain the problem and why it
matters. Follow [review policy](.agents/references/review.md), including issue
labels and frozen/hold-off-merge handling. Separate observed results from
expected benefits. Do not invent performance gains or user impact.

## Pull request descriptions

Use [.github/pull_request_template.md](.github/pull_request_template.md), keep
its headings, and begin with `Summary`. Inspect the complete base-to-head
diff before publishing. Update the whole description when scope changes;
state passed, failed, blocked, deferred, and inapplicable checks accurately.

### Before and after diagram

Every PR includes a simple fenced Mermaid diagram in `Expected outcome`,
with `Before` and `After (with this PR)` parts showing the same scenario.
For internal or documentation changes, show the contributor workflow or
structure that changes. Verify the diagram and keep it aligned with the PR.
