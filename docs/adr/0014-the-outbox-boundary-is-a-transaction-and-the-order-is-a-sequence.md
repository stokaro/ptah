# ADR 0014: The outbox boundary is a transaction, the order is a sequence, and the trigger does not watch itself

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#2068](https://github.com/stokaro/ptah/issues/2068)
- Answers the epic's "the exact PostgreSQL transaction sequence requires ADR-level design and testing"

## 1. Context

A backfill over a table nobody writes to is a loop. A backfill over a live table
is a race, and the epic's cutover rule is that a migration with no valid
consistency mode must not report itself ready over a mutable source.

The epic names four modes and picks one for the first vertical: a PostgreSQL
transactional outbox. It sketches the sequence — install, record, snapshot,
backfill, process, barrier, verify — and then says the exact transaction
sequence needs design and testing rather than a paragraph. This is that record.

Three of its four decisions were made by a live server refusing something that
had read as obviously correct.

## 2. Decision 1 — the event is written by a trigger

Alternatives: the application writes both the change and its event; a trigger
writes the event; logical decoding reads the WAL.

A trigger, because the guarantee is what distinguishes this mode from
dual-write. The event row and the source change are the same transaction, so a
change that committed has an event and a change that rolled back has none, and
nothing outside the database has to be trusted for it. An application writing
both has two writes and a hope — which is precisely the dual-write mode, where
the epic requires the result to be reported as partial.

Logical decoding is out of scope for the first vertical (the epic puts CDC
adapters in later work) and would introduce a replication slot, which is
operational surface Ptah does not otherwise need.

The property is measured rather than argued: the test opens a transaction,
writes to the source, rolls back, and requires the outbox to be unmoved — then
writes the same statement again and commits, and requires exactly one event.

## 3. Decision 2 — reads are bounded by the snapshot's transaction horizon

The obvious cursor is the sequence. It is wrong, and it is wrong in a way that
loses data silently.

A sequence value is allocated when the row is inserted. A transaction becomes
visible when it commits. Two transactions therefore routinely commit in the
opposite order to their sequences: transaction A takes sequence 5, transaction
B takes sequence 6, B commits first. A reader that saw 6 and advanced its cursor
past it has stepped over 5 — which is still in flight, and which will commit a
moment later into a range the reader has already passed. That change is then
processed by nothing, ever, and the only symptom is a stale vector.

So the cursor is a transaction identity, and the upper bound of a read is
`pg_snapshot_xmin(pg_current_snapshot())`. Every identity below it has either
committed and become visible or aborted and left nothing behind, so no row can
appear below it afterwards. That is the property that makes advancing safe, and
it is the property a sequence does not have.

Measured: the test opens a second connection, writes without committing, and
requires the event to be absent and the horizon held back — then commits, and
requires both to move.

## 4. Decision 3 — the ORDER is the sequence, and only the sequence

Having established that the transaction identity is the cursor, the tempting
next step is to order by it too. That is also wrong, and it was a surviving
mutant that said so.

PostgreSQL assigns a transaction its identity at its first write **anywhere**.
A transaction that touched an unrelated table, waited, and then updated a source
row holds an identity *earlier* than a transaction that reached that row and
committed in the meantime — while its update happened *second*. Ordering by
identity resolves that pair backwards, and for a delete-then-insert pair it
keeps the insert as the last word, leaving the target holding a vector for text
the row no longer has.

The sequence is allocated at the write, and writes to a single row are
serialized by that row's lock. For the events that can contradict each other —
the events about one key — the sequence is the order they actually happened in.

So the two values answer two questions and neither substitutes for the other:
the transaction identity says whether an event is **settled**, the sequence says
**when it was written**. The fixture that separates them has to arrange the
disagreement on purpose, because in every ordinary case the two agree.

## 5. Decision 4 — the update trigger does not watch the generation's own columns

A generation's vector column lives on the source table. Ptah's own writes are
therefore updates to the table the outbox watches.

Without a `WHEN` clause the outbox records them, catch-up reads them as source
changes, rereads the rows, re-embeds them, and writes the vectors again — each
of which produces another event. The loop does not terminate. This was found by
running the whole lifecycle against a live server; nothing smaller showed it.

The update trigger's `WHEN` clause compares the columns that decide a vector:
the key fields, the input fields, and the version field. Ptah's writes touch
none of them. Neither does an application update to an unrelated column, for the
same reason — the vector it would recompute is the vector already there. An
update to the version alone **does** produce an event even with the input
untouched, because freshness compares versions.

This costs a second trigger: a `WHEN` clause referring to `OLD` cannot sit on a
trigger that also fires for `INSERT`, and inserts and deletes are always worth
recording. The installation check counts both, because half an installation
captures half the changes and the half it misses is whichever one somebody
dropped.

## 6. Decision 5 — catch-up rereads rather than acting on the event

The event carries a key and a version and no row content. The epic asks for
that, and it is also what makes the rest work: rereading is what collapses a row
updated five times during a backfill into one provider request rather than five,
and it is what stops a stale delete writing a tombstone over a row that was
re-inserted after the page was read.

The tombstone is therefore derived from what the reread **found**, not from the
event's operation. A delete the reread confirms is gone is a tombstone whatever
the last event said; a delete the reread contradicts is not.

The vector is bound to the version the reread returns. The event's version says
when the row changed, and during a catch-up over a live source the row may have
changed again — a write carrying the event's version would read as fresh against
a source that has moved.

## 7. What "caught up" means

Not that the processed watermark equals the horizon. That never happens on a
live server: the horizon moves with every transaction anywhere in the database,
including the ones started by the check that is asking. A rule requiring the two
to meet refused a run that had processed everything, over two transactions the
reads in the check itself had started.

Caught up means nothing between the watermark and the horizon is unprocessed.
Recording how far catch-up got is a separate concern and belongs to the loop,
which moves its watermark even on a page with nothing in it — otherwise it reads
the same range again, and on a busy source that range only grows.

## 8. Consequences

- The outbox is bounded by policy rather than by age. Pruning by time drops the
  delete a paused run has not seen, and the target then holds a vector for a row
  the source no longer has — which verification reports as an unexpected row and
  no amount of re-running fixes.
- A source table gets two triggers and a companion table. That is visible in the
  operator's schema, and it is meant to be: an outbox is a thing they have,
  not an implementation detail Ptah hides.
- The version column, if there is one, becomes part of what the trigger watches.
  An application that bumps it without changing anything else produces an event
  and a re-embed. That is the correct trade — the alternative is a target whose
  recorded version disagrees with the source's forever.
- Nothing here handles a source table that is repartitioned or recreated. The
  triggers go with it, and `Installed` answers false, which is the run's problem
  to report rather than to work around.

## 9. What this record does not decide

- Whether an outbox can span more than one source table for one generation.
- Pruning policy: how long a processed event is kept for audit, and who decides.
- Anything about MySQL, whose trigger and transaction-identity semantics are
  different enough that assuming they carry over would be exactly the mistake
  this record was written to avoid.
