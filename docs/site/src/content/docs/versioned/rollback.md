---
title: Roll back migrations
description: Roll a database back to an explicit target version, verify the plan on a shadow database first, and understand the down-file contract.
---

Something shipped that has to come back out, or a deploy is being unwound.
This page shows how to roll back to an explicit target version, how to prove
the rollback plan works before the target database is touched, and what the
down-file contract guarantees.

Prerequisites: a migration directory and a database with applied migrations.
The examples use a local SQLite file with two applied migrations
(`1785255952_init` and `1785255953_add_posts`).

## Roll back to a target version

Rollback always takes an explicit `--target`: the version the database should
end up at. Everything applied above it is rolled back, newest first.

```bash
ptah migrations down \
  --db-url "sqlite://app.db" \
  --migrations-dir ./migrations \
  --target 1785255952
```

An interactive run states the consequences and asks for confirmation:

```text
Current version: 1785255953
Migrations to roll back: 1

⚠️  WARNING: Rolling back migrations can result in data loss!
This will roll back the database from version 1785255953 to version 1785255952.
The following 1 migration(s) will be rolled back: [1785255953]
Are you sure you want to continue? Type 'YES' to confirm:
```

Type `YES` to proceed. Expected output includes:

```text
✅ Migration rollback completed successfully!
Database is now at version: 1785255952
```

In automation there is no terminal to answer the prompt; a non-interactive
run without confirmation aborts with
`error: read rollback confirmation: EOF`. Pass `--confirm` to skip the prompt
— only in scripts where the target and blast radius are already reviewed.

Verify with status: the rolled-back version moves from applied back to
pending.

```text
Current Version: 1785255952
Applied Migrations: 1
Pending Migrations: 1
```

Before rollback starts, Ptah verifies that every selected migration has a down
body. A missing down body aborts before Ptah changes either the schema or the
revision table.

Dry run follows the same dirty-state, checksum, checkpoint, and down-body
validation path as execution. It reports no migration as reverted when a
preflight check fails before rollback starts.

## Verify the plan on a shadow database first

Add `--shadow-db` to replay and verify the rollback plan on a disposable
**shadow database** before the target is touched: the shadow database is
dropped clean, migrated up to the target's current version, and migrated down
to the requested target. A failing or missing down migration aborts with the
target untouched.

```bash
ptah migrations down \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --target 1785255952 \
  --confirm \
  --shadow-db "sqlite://shadow.db"
```

Expected output includes the verification pass before the real rollback:

```text
✅ Rollback plan verified on shadow database

✅ Migration rollback completed successfully!
Database is now at version: 1785255952
```

The shadow database must match the target dialect and select a different live
database or catalog from `--db-url` — use an empty scratch database of the same
engine, never a real environment. Ptah first rejects equivalent URL aliases,
then connects to the shadow and checks both live dialects and selected
database/catalog names before resetting it. Equal live names fail closed even
when the URLs use different hosts, driver overrides, or scheme spellings:

```text
rollback verification failed: shadow database must be distinct from target database
```

## Choosing a target

- `--target <version>` rolls back every applied migration above that version.
  Only versions recorded as applied are rolled back; a pending migration
  between applied ones is left alone.
- `--target 0` rolls back everything, returning the database to an empty
  application schema.
- Rolling back "one step" means targeting the version below the current one —
  read it from `ptah migrations status`.

## The down-file contract

Every version must have both an up and a down file. Ptah enforces the pair at
registration, so a directory with a missing half fails every command that
reads it (exit `2`):

```text
error: error registering migrations: incomplete migrations found (missing up or down files): [1]
```

Two consequences:

- **Write real rollbacks.** A generated down file reverses the generated up
  file, but hand-written migrations need the same care in both directions —
  the down half is what this page executes.
- **Imported history may carry placeholder downs.** When a source tool had no
  rollback for a migration, [Import from another tool](../import/) writes a
  placeholder down file. Rolling back through such a version executes a no-op
  comment, not a real reversal — review imported downs before relying on them.

A [checkpoint](../checkpoints/) adds one more boundary: history squashed
below a checkpoint no longer has individually applied migrations to reverse,
so a rollback target between the checkpoint and the squashed history fails
with a clear error. Roll back to the checkpoint boundary or to `0`, or
restore from a backup.

## What a failed rollback leaves behind

The two surfaces answer "my down failed, what state am I in?" differently, and
the difference is deliberate.

**Native `ptah migrations down` records the failure.** The revision row for the
failing version is marked failed with `error=<message>`, and `applied` becomes
the number of down statements that completed — `0` when the first one fails, or
when the dialect rolled the whole body back. So:

- `ptah migrations status` reports `Status: ❌ Dirty migration state detected`
  and names the version.
- `ptah migrations repair --version <version>` has a row to act on and clears
  it once you have fixed the database by hand.
- A later `ptah migrations up` refuses to run over the unfinished rollback
  instead of stacking new work on unknown state.

:::caution
`repair --resume-from` is an **up**-direction tool: it executes the remaining
statements of the migration's *up* SQL. Do not reach for it after a failed
down — on a down-failure row it replays the up body and typically fails again
(`table ... already exists`). Fix the database state yourself, then clear the
row with `ptah migrations repair --version <version>`.
:::

**`ptah-compat migrate down` reproduces Atlas's bookkeeping.** The revision row
is left byte-identical — no error recorded, `applied` and `total` unchanged.
Both `atlas migrate status` and `ptah-compat migrate status` then report the
version as applied, and a retry after fixing the down file needs no flags and
no repair step. Measured against Atlas CLI `v1.2.4-e282f76-canary` (licensed
build, 2026-08-01): after a down whose second statement fails, its revision row
still reads `applied=2, total=2, error=''`.

On the default per-file transaction mode the down body is rolled back with it,
so the schema is untouched. A down marked `-- atlas:txmode none` runs outside a
transaction, so statements that already completed stay applied: the schema is
left half-reverted behind a revision row that still reads as fully applied,
with no dirty state and no repair hook. Ptah deliberately does not write
statement checkpoints on this path. That is Atlas's behavior too.

That fidelity is the point of the compat surface: a database it touched must
read the same way to Atlas. It also means the failure is not visible in the
revision table, so recovery relies on the operator noticing the non-zero exit.

:::tip
If your team is not bound to the Atlas surface, prefer native
`ptah migrations down`: the recorded failure is what makes an interrupted
rollback visible later and gives `ptah migrations repair` something to work
from.
:::

:::note
The split follows the **revision table format**, not the binary. Native
`ptah migrations down --revision-format atlas` writes Atlas-format revisions
and therefore also follows Atlas's bookkeeping: a failed down leaves no dirty
state. If you moved a native project onto the Atlas revision table for
interoperability, you gave up failed-down recording along with it.
:::

## Failure modes

- **The down SQL itself fails.** The command exits `2` and, on the native
  surface, the failing version's revision row is marked dirty:

  ```text
  error: error running down migrations: failed to revert migration 2: failed to execute migration SQL: sqlite: SQL execution failed: SQL logic error: no such table: not_there (1)
  ```

  `ptah migrations status` then reports
  `Status: ❌ Dirty migration state detected`. Fix the down file and repair
  the revision state — see
  [Maintain migration history](../maintain-history/).
- **Shadow verification fails.** The target is untouched; the error names the
  migration whose down could not be replayed.
- **Data does not come back.** Rolling back a `DROP`-ing down file deletes
  rows by design. `--pg-dump-to` / `--mysqldump-to` on the down command write
  a backup first, and the confirmation prompt exists precisely because
  rollback is not a data-recovery tool.

## Next steps

- Re-applying after a fix? [Apply migrations](../apply/).
- Rewriting the migration that had to come back out?
  [Maintain migration history](../maintain-history/).
- Making rollback safety part of CI? [Integrity and safety](../integrity-and-safety/).
