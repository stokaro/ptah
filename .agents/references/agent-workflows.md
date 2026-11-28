# Choose an agent workflow

Use this reference when work spans independent changes or sessions, when
choosing an external workflow, or when evaluating instruction changes. A small
fix uses the relevant Ptah skill and existing validation entry points directly.

## Keep larger tasks recoverable

Write a short plan naming the observable outcome, owning boundaries, dependent
changes, and required evidence. Record decisions and discoveries that would be
expensive to reconstruct. Keep the record in the task's existing notes or an
untracked file; do not create a second repository planning system.

Retain the request, base revision, completed and remaining work, changed paths,
commands and results, environment, and unresolved risks. On resumption, compare
the record with the current diff and inputs before reusing a result. A command
need not be repeated merely because its output appeared in an earlier message.

Use parallel agents for independent questions or disjoint file ownership.
Give each agent the request, relevant paths, expected boundary, and necessary
raw evidence. Avoid sending an entire conversation or a presumed diagnosis.
Use one implementer for tightly coupled steps. Reuse an agent for fixes to its
own work instead of repeatedly starting repository discovery.

For a large or data-sensitive change, give an independent reviewer the request,
exact base and head revisions, full diff, test evidence, and unresolved risks.
State whether the diff includes uncommitted work. Verify findings against real
callers and requirements before changing code. After a fix, repeat only the
affected checks and review needed to resolve the finding. Do not run another
full review of unchanged inputs. Follow
[review guidance](../skills/ptah-review/SKILL.md) for publication.

Proceed with implementation choices already covered by the user's request.
An optional workflow does not add a design approval gate to an authorized fix.
Raise a specific scope, access, or irreversible-action decision only when it
actually prevents progress. Continue independent authorized work while a
separate dependency is blocked.

## Keep tool output small

Every line a command prints stays in the session's context and is processed
again on each later turn, so a large output keeps costing after the command
ends.

- Find before reading: `rg -l` or `git grep -l` names the files, then
  `rg -n -m 5 PATTERN FILE` or `sed -n 'START,ENDp' FILE` shows the lines
  around a hit. Do not print a whole large file or log to find one line.
- Find a Go declaration and its references with the semantic tools in
  [Go semantic navigation](go-semantic-tools.md) rather than a chain of text
  searches. Keep text search for serialized names, SQL, and non-Go consumers.
- Read a diff with `git diff --stat` first, then one path at a time.
- Send a full test, lint, or CI log to a file. Print the failed test, its error,
  and a short excerpt; read more of the file only when that is not enough.
- A file or result already in the conversation does not need to be read again
  unless it changed.

## Apply external skills selectively

Superpowers offers reusable investigation, planning, and review techniques.
The Ptah debugging skill uses its hypothesis-driven approach in original,
repository-specific instructions. This reference also uses its ideas of a
durable task record and an independent final review. The reviewed upstream
source is [Superpowers 7.0.2 at commit 22f0b463](https://github.com/obra/superpowers/tree/22f0b463e7a5e579904dd6211a520321035526f7),
under the [MIT license](https://github.com/obra/superpowers/blob/22f0b463e7a5e579904dd6211a520321035526f7/LICENSE).
No upstream skill text, scripts, hooks, or plugin configuration are vendored.

If an agent already has Superpowers and a task benefits from a fuller plan,
select the relevant workflow explicitly. The
[inline execution workflow](https://github.com/obra/superpowers/blob/22f0b463e7a5e579904dd6211a520321035526f7/skills/executing-plans/SKILL.md)
can suit dependent tasks with one implementer and a final reviewer. Independent
work may justify delegation. Neither choice requires loading the whole pack.

Apply external guidance within the user's instructions and Ptah's contracts.
Use Ptah's Quicktest conventions, ownership rules, and validation policy when
an upstream example differs. Select tests by the changed behavior and required
gates. Keep valid evidence across messages. Do not install global hooks,
introduce a second test runner, or require several reviewers for every edit.
Installation and global agent settings are separate from using this guide.

## Evaluate instruction changes

Before claiming better quality or lower cost, compare the candidate guidance
with Ptah's existing guidance on representative tasks. Include a local fix, a
schema or execution defect, and a change across modules. Include a simple task
that should not activate a complex workflow.

1. Define success from the task and real consumers before reading agent output.
   Include correctness, regression strength, idiomatic code, and required
   evidence. Keep expected answers out of the agent's prompt.
2. Use equivalent clean checkouts, task starting revisions, model settings,
   tools, service state, and time budgets. Change only the guidance. Use fresh
   sessions and alternate run order to reduce carryover and cache bias.
3. Record completed, failed, blocked, and timed-out tasks. Inspect which skills
   activated, unnecessary reads or reruns, and missing domain guidance. Count
   failure to activate a needed skill as part of the result.
4. Record elapsed time and reported token usage, including cached tokens,
   subagents, and tool cost where available. Label cold and warm runs. File
   size is an instruction budget, not a measured token or cost reduction.
5. Review outputs against the same acceptance criteria without revealing which
   instruction set produced them. Repeat enough tasks to identify inconsistent
   outcomes, then report sample counts and limitations with the results.

A small forward exercise can expose a bad command or missing rule. It does not
establish a general defect-rate, latency, or cost improvement. Keep measurement
artifacts with the evaluation task; do not add an agent benchmark framework to
Ptah merely to evaluate a prose change.
