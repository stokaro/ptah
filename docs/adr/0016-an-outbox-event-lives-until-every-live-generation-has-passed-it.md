# ADR 0016: An outbox event lives until every usable live feeder has passed it

- Status: proposed
- Deciders: Ptah maintainers
- Issues: [#2690](https://github.com/stokaro/ptah/issues/2690),
  [#2723](https://github.com/stokaro/ptah/issues/2723)
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

An outbox event is kept until **every usable live feeder reading that source
table** has passed it. `catchup` deletes the rest, at the end of a pass, from
positions already on disk.

The bound is the minimum resume position across readers, not the position of
whichever run is catching up. A run's own watermark is the wrong bound because
the table is shared: pruning there deletes what another run still owes.

There is no retention setting. "How long is a processed event kept for audit" is
answered with "it is not, and the outbox was never the audit surface". "Who
decides" is answered by durable lifecycle state. An event survives while any
positioned, nonterminal run over that source has yet to pass it. `abandon`
releases one run without deleting its generation or vectors; `retire` remains
the destructive lever for a whole generation.

Membership of the pruning reader set is the run status, with runs whose
generation is retired excluded as well. `abandoned` and `complete` are terminal;
`running`, `paused`, and `failed` remain recoverable readers. A run whose
generation row is missing is included conservatively. A run recording no
position at all — which is what `prepare` writes for a mode that captures
nothing — is not a reader at the zero position; it is not a reader.

The generation-level `retired_at` check remains the authority for whether the
shared change-capture objects may be removed. A live generation can own those
objects before any run has a position, so trigger ownership and the pruning
floor deliberately answer different questions.

The shared trigger preserves every live reader's capture contract. Generations
over one source must use the same ordered key fields, version strategy, and
version field because one event stores one key and one version. Their input and
filter columns may differ; prepare rebuilds the update trigger from the union of
those columns so a change relevant only to an older generation still produces
an event. An incompatible contract is refused while the existing trigger stays
installed. The rebuild's function and trigger DDL commits atomically, so a late
failure cannot leave a live source between dropped and recreated triggers.

The floor query and the deleting statement are one transaction under a
source-scoped lifecycle lock. Creating a positioned run can add a reader behind
the current floor, so the normal `PrepareRun` path holds the same lock from
target creation through run creation. The lower-level positioned `CreateRun`
path takes that lock too, while ordinary whole-run persistence cannot add the
first resume position. Retirement holds the source lock while removing
membership and shared source objects. Checkpointing and abandonment can only
advance or remove an existing floor, so their run-row fencing remains
sufficient. This prevents a new reader from appearing between the floor query
and the delete.

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

**Keep deferring — leave pruning unwired and correct the page instead.**
Rejected: it leaves a declaration carrying a rule nothing applies, which
`AGENTS.md` names as a rule that is not in effect, and it leaves operators
sizing storage for the whole history of a migration.

**Prune on a timer, or at retirement only.** Rejected. Deleting by time drops a
tombstone a paused run still owes. Deleting only at retirement is what already
happens and is the state this record ends.

## 4. Consequences

The companion table tracks the backlog rather than the history, so its size
follows how far behind the slowest usable live feeder is rather than how long
the migration has run. For outbox mode, a feeder is usable only when it is
nonterminal, source-matched, and carries a readable durable resume position.
When another run holds the floor, `catchup` names every run and generation tied
at that position.

A forgotten run pins the floor until it advances, its generation is retired,
or an operator records that attempt as abandoned. Abandonment is terminal,
preserves its checkpoint and vectors, advances the fencing token, and is
refused when it would leave an active or maintained generation without another
usable live feeder. This keeps the safety property while adding the
non-destructive lever tracked in
[#2723](https://github.com/stokaro/ptah/issues/2723).

Preparing another generation over the same source may widen the update
trigger's watched-column set. It cannot change the event key or version
contract while an earlier run is live. Retiring or abandoning the incompatible
reader first makes the narrower contract eligible again.

An operator can no longer read the accumulated change log of a finished
migration out of that table. That capability was an accident of nothing calling
`Prune`, and retirement destroyed it anyway, so nothing durable is lost.

A failed prune is reported and the command still succeeds. The catch-up it
follows is already committed, and the only cost of the failure is a larger
table.
