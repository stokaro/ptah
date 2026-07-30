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

The shadow database must match the target dialect and identify a different
database from `--db-url` — use an empty scratch database of the same engine,
never a real environment. Ptah rejects equivalent URL aliases before connecting
to or resetting the shadow database:

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

## Failure modes

- **The down SQL itself fails.** The command exits `2` and the failing
  version's revision row is marked dirty:

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
