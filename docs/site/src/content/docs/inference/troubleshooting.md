---
title: Troubleshooting
description: Symptoms, what causes them, and what to do - for the failures a generation change actually produces.
type: troubleshooting
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "Which inference migration symptom am I seeing, and how do I recover?"
goal: "Identify an inference migration symptom and follow its recovery path."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
  - "integration/inference_outbox_prune_e2e_test.go"
generated: false
searchAliases:
  - "outbox not shrinking"
  - "release outbox floor"
overlaps: []
disposition: keep
---

Each entry is a message you might see, what it means, and the fix.

## `ptah inference works against PostgreSQL with pgvector`

**Cause.** The `--db-url` names another engine. This feature is a PostgreSQL
vertical: the run state and the vectors have no dialect-agnostic form.

**Fix.** Point it at a PostgreSQL database. There is no workaround.

## `preprocessing.null_policy "" is not one this build acts on`

**Cause.** The field is absent. Thirteen fields are required, and each one is
refused by name with the values it accepts -- the same message appears for
`empty_policy`, `unicode_normalization`, `truncate`, `model.endpoint_class`,
`model.normalization` and `target.metric`. The refusal happens before any verb
does work, `describe` included, so it is not about your database.

**Fix.** Write the field. The [specification
reference](../reference/specification/) marks which fields a specification is
refused without; `truncate` is the one most often missed, because it is required
even where `max_input_bytes` names no cap for it to act at.

## `the target database has no pgvector`

**Cause.** The `vector` extension is not installed in that database.

**Fix.** `CREATE EXTENSION vector`, which needs a privilege Ptah does not assume
on your behalf. The refusal names the statement.

## `column "embedding" on articles holds generation ... and this run is generation ...`

**Cause.** You changed the model but not `target.column`, so a new generation
would overwrite the one your queries read.

**Fix.** Give the new generation its own column. Keeping the previous one is what
makes a rollback possible.

## `live generations sharing one source must use the same ordered key fields ...`

**Cause.** The new outbox specification changes how a shared event identifies
or versions a row while another run over that source is still live. One source
has one outbox event format, even when generations embed different input fields
or apply different filters.

**Fix.** Keep `source.key_fields`, `version_strategy`, and `version_field`
identical until the earlier run is abandoned or its generation is retired.
Input and filter columns may differ; Ptah makes the shared trigger watch their
union. The refusal occurs before target DDL and leaves the existing trigger in
place.

## `catch-up needs a consistency mode that records changes`

**Cause.** The specification selects `immutable` or nothing, and you ran
`catchup`.

**Fix.** There is nothing to catch up on under those modes, and the run does not
need one: a completed `backfill` is what carries it to `caught_up`, so `index`,
`verify` and `cutover` follow directly. If the source is changing after all,
switch to `outbox` and run `prepare` again to install the triggers and record a
fresh boundary.

## `catch-up needs a backfill that reached the end of its snapshot`

**Cause.** `catchup` was run on a generation whose `backfill` has not finished.
The message names the phase the run is actually at — `boundary_captured` for a
run that was prepared and never backfilled, `backfilling` for one whose walk was
interrupted.

**Fix.** Run `backfill` until it reports the walk complete, then `catchup`.
Catch-up covers what changed *after* the snapshot the backfill walked, so before
that walk finishes there is no such range: the changes it would read are ones
the backfill still owes.

Nothing is spent to reach this refusal. It is raised before the first provider
request, so no vector is written and the catch-up watermark does not move.

Running `catchup` again once the run is past `backfilled` — after an index, a
verification, or a cutover — is unaffected. That is ordinary, because the source
keeps moving, and [the phase is a high-water mark](../concepts/lifecycle/).

## `the outbox keeps events this run has processed: floor ... is held by run ...`

**Cause.** Another usable live feeder over the same source has not reached those
events. One source table has one outbox, so `catchup` may delete only what every
positioned, source-matched feeder has passed. The message names each run at the
minimum position and the generation it belongs to.

**Fix.** Inspect the named run:

```bash
ptah inference status --spec old-spec.yaml --db-url "$DB" --run-id old-run
```

If the migration is still wanted, catch it up. If that attempt is permanently
over but its vectors should remain available for inspection, release only its
outbox position:

```bash
ptah inference abandon --db-url "$DB" --run-id old-run \
  --reason "superseded by run 2026-09-articles-v2"
```

The next catch-up prunes events the remaining runs have all passed. The
abandoned run cannot resume. Ptah refuses this action when it would leave an
active or maintained generation without another usable live feeder: for outbox
mode, that means a nonterminal, source-matched run with a readable durable
resume position. Start a positioned replacement, move queries elsewhere, or
finish the maintenance window first. Use `retire` instead only when the
generation and its vectors should be destroyed.

## `provider: embedding endpoint unreachable`

**Cause.** The endpoint is down, the address is wrong, or the network is not
there.

**Fix.** Everything committed stays committed. Fix the endpoint and run the same
command again; it resumes from its checkpoint.

## The run stops repeatedly with provider errors

**Cause.** Usually rate limiting. Ptah does not implement backoff against a
provider's limiter.

**Fix.** Lower `--batch-inputs`, raise `--provider-timeout`, and run the backfill
in sessions. See
[Plan provider capacity](../strategies/plan-provider-capacity/).

## `the state changed underneath this write: run ... is fenced at token N`

**Cause.** Another worker took over the run. The lease says who should work; the
token says who may still commit.

**Fix.** If the other worker is real, let it finish. If it is a process that
died, run the command again — the new invocation becomes the holder.

## `verification found N blocking findings`

**Cause.** Depends on the layer named in the finding.

**Fix.** See the table in
[Resume and recover](../guides/resume-and-recover/#verification-found-something).
`freshness` and `coverage` almost always mean a phase has more to do rather than
something being wrong.

## `this policy requires an approval and none was given`

**Cause.** `policy.require_exact_approval` is on, which is the sensible default.

**Fix.** Run `cutover` once to see the plan digest, then approve that exact
digest with `--approve` and `--approver`.

## `the approval is bound to plan X and this plan is Y`

**Cause.** Something the plan rests on changed between you reading the digest and
approving it — the pointer moved, the findings changed, the source moved.

**Fix.** Read the new plan and approve that. The refusal is the mechanism
working: it stopped an approval being applied to a different plan.

## `N rows are stale and this policy allows 0`

**Cause.** The previous generation stopped being caught up. It drifted from the
source, and going back to it would answer queries from a corpus that no longer
matches your data.

**Fix.** There is no way to roll back to a drifted generation, and that is the
honest answer. Catch it up first if it is close enough to be worth it; otherwise
fix forward.

**Prevention.** Run `catchup --maintain-for` on a schedule for the length of the
window. See [Rollback and retire](../guides/rollback-and-retire/).

## `no stabilization window was asked for`

**Cause.** `cutover` ran without `--stabilize-for`, so nothing is keeping the
previous generation current and there is no rollback to it.

**Fix.** Nothing, after the fact. Decide the window before the cutover.

## Search results got worse after a cutover

**Cause.** The new model is not better for your data, the index parameters
changed, or your query is using the wrong operator for the metric.

**Fix.** Check the operator first — `<=>` for cosine, `<->` for l2, `<#>` for
inner product — because a mismatch returns plausible-looking wrong rows without
erroring. Then measure with `evaluate --baseline` rather than by impression.

**Prevention.** Run the evaluation with a regression gate before the cutover, not
after.

## `not found: run <id>`

**Cause.** The run identifier is wrong, or you are pointed at a different
database. Run identifiers are yours to choose and are not derived from anything.

The running verbs say the same thing behind the operation they were doing:
`claim run <id>: not found: run <id>`.

**Fix.** Check both. A typo looks exactly like a run that was never prepared.

## The plan says `unknown` for something

**Cause.** Ptah could not establish that fact. A source it could not count, a
capability it could not ask about.

**Fix.** It is not necessarily an error — a migration can run with an uncounted
source. It is telling you that a decision rests on something nobody checked, and
whether that matters is yours to judge.
