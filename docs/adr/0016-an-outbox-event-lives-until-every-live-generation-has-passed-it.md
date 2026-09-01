# ADR 0016: An outbox event lives until every live generation has passed it

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#2690](https://github.com/stokaro/ptah/issues/2690)
- Takes the pruning-policy decision [ADR 0014](0014-the-outbox-boundary-is-a-transaction-and-the-order-is-a-sequence.md) deferred

## 1. Context

[ADR 0014](0014-the-outbox-boundary-is-a-transaction-and-the-order-is-a-sequence.md)
listed, under what it did not decide, "Pruning policy: how long a processed
event is kept for audit, and who decides."

The consequence of leaving it open was not that pruning was configurable. It was
that pruning did not happen. `Outbox.Prune` existed, held the only deleting
statement in the vertical, and was called by nothing, so an outbox grew for the
whole life of a migration and was removed with its table at retirement.
Meanwhile the concepts page described "a table that grows until catch-up drains
it" — a behavior the product did not have, and the sentence an operator sizing a
source table would have planned against.

Two measurements shaped the decision that follows.

**An outbox belongs to a source table, not to a run.** `Outbox.TableName`
digests the source's schema and table, nothing else. Two generations over one
source therefore share one companion table, which is why retirement already has
to ask whether the retiring generation was the last reader before it removes it.
This is not a rare configuration: it is the shape the troubleshooting guide
recommends when a model changes, and the integration suite builds it.

**Nothing reads an outbox event's timestamp.** Every production read of that
table is inside `internal/embedpg/outbox.go`, reached from four constructors,
and none of them touches the `at` column. The audit trail is a different table
by declaration — `ptah_embedding_event`, "what happened to a run, in order, and
never what it embedded".

## 2. Decision

An outbox event is kept until **every live generation reading that source
table** has passed it. `catchup` deletes the rest, at the end of a pass, from
positions already on disk.

The bound is the minimum resume position across readers, not the position of
whichever run is catching up. A run's own watermark is the wrong bound because
the table is shared: pruning there deletes what another generation still owes.

There is no retention setting. "How long is a processed event kept for audit" is
answered with "it is not, and the outbox was never the audit surface". "Who
decides" is answered with "the generation registry": an event survives while any
unretired generation over that source has yet to pass it, and `retire` is the
operator's lever.

Membership of the reader set is `retired_at`, the same authority retirement
already consults. A run recording no position at all — which is what `prepare`
writes for a mode that captures nothing — is not a reader at the zero position;
it is not a reader.

## 3. Alternatives

**Prune at the invoking run's watermark.** Simplest, and wrong in a way nothing
would report. The deleted events already fail the pending predicate, so the
other generation's `Unprocessed` count answers zero, its barrier reports caught
up, and it cuts over onto stale vectors with a green readiness. Rejected because
the failure is silent, not because it is unlikely.

**A configurable retention window, deleting below the floor and older than the
window.** Rejected for this record, and it is the alternative with a real case.
It would answer the audit half directly. Three things weigh against it. Nothing
consumes the data it would retain, so it is configuration governing a reader
that does not exist. It would key on the `at` column, whose own declaration says
it follows the system clock, can go backwards, and is never an ordering key.
And it cannot be honored where it would be declared: retention is
per-specification and an outbox is per-source-table, so two specifications
sharing one outbox could declare two windows and whichever catch-up ran last
would win, silently. Deferring costs one conjunct in one statement the day a
consumer appears, and retention zero collapses to exactly the predicate chosen
here.

**Keep deferring — leave `Prune` unwired and correct the page instead.**
Rejected: it leaves a declaration carrying a rule nothing applies, which
`AGENTS.md` names as a rule that is not in effect, and it leaves operators
sizing storage for the whole history of a migration.

**Prune on a timer, or at retirement only.** Rejected. Deleting by time drops a
tombstone a paused run still owes. Deleting only at retirement is what already
happens and is the state this record ends.

## 4. Consequences

The companion table tracks the backlog rather than the history, so its size
follows how far behind the slowest live generation is rather than how long the
migration has run. An operator watching it not shrink is looking at a generation
that is behind or idle, and `retire` releases it.

A generation that is live and forgotten pins the floor for as long as it exists.
That is the cost of the safety property and it is deliberate: the alternative is
deleting what a reader still owes. There is no verb today for "this run is
abandoned", which is worth having and is tracked in
[#2723](https://github.com/stokaro/ptah/issues/2723).

An operator can no longer read the accumulated change log of a finished
migration out of that table. That capability was an accident of nothing calling
`Prune`, and retirement destroyed it anyway, so nothing durable is lost.

Runs are matched to an outbox by their unqualified source name while the table
is keyed on the qualified pair, so two same-named tables in different schemas
share a floor. That over-includes readers and therefore keeps more events than
necessary; it can never delete one that is owed. It is tracked in
[#2724](https://github.com/stokaro/ptah/issues/2724) rather than fixed here, so
that the conservative direction is a written decision and not an accident.

A failed prune is reported and the command still succeeds. The catch-up it
follows is already committed, and the only cost of the failure is a larger
table.
