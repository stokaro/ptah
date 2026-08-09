---
title: Maintain migration history
description: Edit, reorder, and delete unapplied migrations with the integrity file kept true, and repair a dirty revision state after a partial failure.
---

Review feedback, a merge race, or a failed deploy means a migration has to
change after it was written. This page shows the four maintenance verbs —
`edit`, `rebase`, `rm`, and `repair` — that modify history while keeping
`ptah.sum` true and applied history protected.

The rule all four enforce: **applied history is immutable**. With `--db-url`
set, each verb checks the revision table and refuses to touch a migration
the database has already run, unless you override with `--force`.

Prerequisites: a migration directory sealed with `ptah migrations hash`. The
examples use a directory where version `1` is applied and versions `2` and
`3` are not:

```text
0000000001_init.up.sql        (applied)
0000000001_init.down.sql
0000000002_add_posts.up.sql   (pending)
0000000002_add_posts.down.sql
0000000003_add_tags.up.sql    (pending)
0000000003_add_tags.down.sql
ptah.sum
```

## Edit a migration

`ptah migrations edit` opens a version's up/down pair and rewrites `ptah.sum`
afterward, so the edit and the re-hash cannot drift apart. Interactively it
uses `$VISUAL`, then `$EDITOR` (or `--editor`); in scripts, `--up-file` and
`--down-file` replace a half non-interactively:

```bash
ptah migrations edit \
  --version 2 \
  --up-file ./reviewed_up.sql \
  --migrations-dir ./migrations \
  --db-url "$DATABASE_URL"
```

Expected output includes:

```text
Edited migration 2
Wrote ./migrations/ptah.sum
```

Editing an applied version is refused (exit `2`):

```text
error: migration version 1 is already applied; refusing to modify applied history (use --force to override)
```

:::caution
When `--db-url` is omitted, the applied-state check is skipped entirely.
Pass the database URL whenever one exists, and treat `--force` as a
deliberate decision to make the directory disagree with deployed databases.
:::

## Move a migration to the end (rebase)

Two branches each added a migration; the merge landed yours below a version
that is already applied elsewhere. Instead of hand-renaming files,
`ptah migrations rebase` re-timestamps a pending migration to the end of
history and rewrites `ptah.sum`:

```bash
ptah migrations rebase \
  --version 2 \
  --migrations-dir ./migrations \
  --db-url "$DATABASE_URL"
```

Expected output includes:

```text
Rebased migration 2 to 1785255952
  ./migrations/1785255952_add_posts.down.sql
  ./migrations/1785255952_add_posts.up.sql
Wrote ./migrations/ptah.sum
```

The pair keeps its description and content; only the version changes, so the
migration now applies after everything else. A migration that is already
last is refused (exit `2`):

```text
error: migration version 1785255952 is already last; rebase would not move it
```

Rebase complements the `--exec-order` policies on
[Apply migrations](../apply/): rebase fixes the directory once, execution
policy decides how an unfixed out-of-order migration is treated at run time.

## Delete a migration (rm)

`ptah migrations rm` deletes a version's pair and rewrites `ptah.sum`:

```bash
ptah migrations rm \
  --version 3 \
  --migrations-dir ./migrations \
  --db-url "$DATABASE_URL"
```

Expected output includes:

```text
Removed ./migrations/0000000003_add_tags.down.sql
Removed ./migrations/0000000003_add_tags.up.sql
Wrote ./migrations/ptah.sum
```

Like the other verbs, it refuses an applied version without `--force`.
Deleting applied history strands the databases that ran it — roll the
migration back first (see [Roll back migrations](../rollback/)) if the
change itself must be undone.

## Repair a dirty revision state

A migration that fails partway — typically under `--tx-mode none`, where
earlier statements are already committed — leaves a **dirty** revision row.
Every later `up` refuses until the state is resolved. The failure looks like
this (exit `2`):

```text
error: error running migrations: failed to apply migration 5: failed to execute migration SQL: sqlite: SQL execution failed: SQL logic error: no such table: missing_table (1)
SQL: INSERT INTO missing_table (id) VALUES (1)
```

`ptah migrations status` names the dirty version, how far it got, and the
failing statement:

```text
Status: ❌ Dirty migration state detected
Dirty Migration: version=5 state=failed direction=up applied=1/2
Error Statement: INSERT INTO missing_table (id) VALUES (1)

Run 'ptah migrations repair --version <version>' after fixing the database state.
```

`direction` says which body left the row dirty, and repair follows it. Every
example on this page is `direction=up`; for `direction=down` see
[Finishing an interrupted rollback](../rollback/#finishing-an-interrupted-rollback),
where `--resume-from` runs the remaining *down* statements and a finished
rollback removes the revision instead of marking it applied.

An interruption during a `no_transaction` statement is more ambiguous. Before
each SQL-backed statement, Ptah durably records the last known completed
statement and marks the next statement's outcome as unknown. After success, it
advances the progress count and clears the marker. A process exit, context
cancellation, or deadline while `ExecContext` is in flight preserves that
marker instead of replacing it with an ordinary failure.

If the process exits between those writes, the statement may or may not have
committed. Inspect the database before changing the revision row. Ptah rejects
`--resume-from` while the unknown-outcome marker is present because automatic
replay could duplicate committed SQL. Repair without `--resume-from` only after
you have reconciled the schema by hand.

Fix the migration file (it is unapplied, so `edit` applies), re-hash, and
repair. On a `direction=up` row, `--resume-from` executes the remaining up
statements — here starting at the second — before marking the migration
applied. Before any resume skips committed statements, Ptah verifies their
source prefix. Editing the unapplied suffix is allowed; changing or losing the
recorded prefix metadata fails closed. Resumed SQL uses the same in-flight
marker and per-statement durable checkpoint protocol as the original
`no_transaction` run, so a second failure records its absolute progress instead
of replaying statements that already committed.

The resumed SQL runs on one fresh pinned database session. Ptah replays
recognized session-control statements such as `SET search_path` from the
verified committed prefix before it runs the unapplied suffix; recognized
durable DDL and DML remain skipped. It refuses resume when the prefix created a
temporary object, or when a statement may have session-local effects Ptah
cannot classify safely. In those cases, inspect and reconcile the database,
then choose explicit metadata reconciliation rather than guessing how to
reconstruct the old session or replaying migration SQL.

Repair also refuses a migration whose selected recovery body contains
top-level transaction-control statements. Recovery executes resumed SQL as
independent autocommit statements, so accepting a nested `BEGIN`, `COMMIT`,
`ROLLBACK`, savepoint, autocommit toggle, or implicit-transaction toggle would
make its durable progress counters untrustworthy.

`RepairMigration` acquires the same session advisory lock as migration up and
down. It holds the lock across revision inspection, resumed SQL, safety checks,
and the final metadata write. The native repair command currently uses the
default lock name and waits indefinitely.

```bash
ptah migrations repair \
  --version 5 \
  --resume-from 2 \
  --migrations-dir ./migrations \
  --db-url "$DATABASE_URL"
```

Expected output includes:

```text
Repaired migration 5
```

Status then reports the version as applied and the directory continues
normally. Without `--resume-from`, `repair` resolves the revision row for
state you have already fixed by hand in the database.

Repairing a version that is not dirty is refused (exit `2`):

```text
error: migration 1 is not dirty; rerun with --force to rewrite it
```

`--force` rewrites (or creates) the revision row anyway — a last resort for
reconciling revision metadata that no longer matches reality.

### Repair over a half-built concurrent index

On PostgreSQL, a `CREATE INDEX CONCURRENTLY` that fails partway leaves an
**invalid** index behind. The leftover keeps the name, so the generated
`IF NOT EXISTS` form of the same statement is skipped rather than retried and
reports no error. Repair refuses to record such a migration (exit `2`):

```text
error: migration 5 cannot be repaired: PostgreSQL reports index "public"."idx_members_email" (indisvalid=false, indisready=false) unusable, so recording the migration applied would report a constraint that is not enforced; run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", or drop the index and rerun the migration, then repair again
```

Refusing leaves a dirty state you can still see, rather than a green one that
is wrong: an invalid unique index enforces nothing, so duplicate rows keep
being accepted while `status` reports the database up to date. Rebuild the
index with the `REINDEX INDEX CONCURRENTLY` the message names — or drop it and
rerun the migration — and repair again.

`--force` does not bypass this. It relaxes a precondition about the revision
row; the index being unusable is a fact about the database, and the fix for it
is `REINDEX`. Only indexes named by conditional creates in the selected
direction are checked, so an unrelated invalid index elsewhere never blocks a
repair and an unconditional create retains PostgreSQL's normal error semantics.
Other dialects have no concurrent index build to leave half-finished and are
unaffected.

`ptah migrations up` refuses on the same grounds, so `--allow-dirty` cannot be
used to walk past it either. Ordinary rollback also checks conditional creates
in its down body before deleting the revision. A failed transactional check
rolls the body back; a failed `none` check keeps a dirty down-direction row with
its completed progress. See [Apply migrations](../apply/#failure-modes).

## Set the revision boundary (set)

`repair` fixes one dirty row and `baseline` only records existing history as
applied. `ptah migrations set` moves the whole revision boundary to an
arbitrary version, in both directions, without executing any SQL: every
migration through `--version` is recorded as applied (dirty rows are marked
applied, missing rows are inserted), and revision rows above `--version` are
removed.

```bash
ptah migrations set \
  --version 5 \
  --migrations-dir ./migrations \
  --db-url "$DATABASE_URL"
```

Expected output includes:

```text
Current version is 5 (2 set, 1 removed):

  + 4 (add_orders)
  + 5 (add_index)
  - 6 (drop_legacy)
```

This is a metadata-only operation for databases whose schema was changed
outside the migration flow. It never runs or reverts migration SQL — the
database schema itself is untouched. `--revision-format atlas` targets Atlas
revision bookkeeping (`atlas_schema_revisions`) instead of Ptah's native
table, and `--dry-run` validates the inputs without changing anything.

Rows that Ptah writes in Atlas revision mode preserve Atlas's filename
description, empty successful error fields, checksum hash, and operator
metadata. Missing rows created by this metadata-only operation store the write
timestamp with zero duration; existing rows keep their timing metadata.

When Ptah executes migration SQL, it instead stores the migration lifecycle
start and full elapsed duration in nanoseconds. Atlas CE can read both forms,
but exact dynamic timing equality is not claimed: Atlas CE v1.2.0 can persist
a near-final timestamp and write-order-dependent duration. PostgreSQL-family
revision tables use `TIMESTAMPTZ` for `executed_at`, matching Atlas-created
tables.

## Atlas-compatible surface

In the `ptah-compat` drop-in binary, `migrate edit`, `migrate rebase`, and
`migrate rm` forward to these native commands for drop-in Atlas familiarity,
with `--dir` mapping to the migrations directory. Its `migrate set` is the
Atlas spelling of `ptah migrations set` with Atlas revision bookkeeping
preselected. Squashing history is its own verb pair — see
[Checkpoints](../checkpoints/).

## Next steps

- History too long rather than wrong? [Checkpoints](../checkpoints/).
- Undoing an applied migration instead of editing a pending one?
  [Roll back migrations](../rollback/).
- Keeping edits honest in CI? [Integrity and safety](../integrity-and-safety/).
