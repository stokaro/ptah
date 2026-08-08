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

This confirmation belongs only to the native command. The Atlas-compatible
`ptah-compat migrate down` matches Atlas by starting a real rollback without
reading stdin; it does not accept `--confirm`.

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

Both `ptah migrations down` and `ptah-compat migrate down` record the failure
and record that it was a rollback. The revision row for the failing version is
marked failed with
`error=<message>`, `applied` becomes the number of down statements that
completed — `0` when the first one fails, or when the dialect rolled the whole
body back — and the row carries `direction=down`. So:

- `ptah migrations status` reports `Status: ❌ Dirty migration state detected`,
  names the version, and prints `direction=down` on the
  `Dirty Migration:` line.
- `ptah migrations repair` has a row to act on, and follows its direction.
- A later `ptah migrations up` refuses to run over the unfinished rollback
  instead of stacking new work on unknown state.

### Finishing an interrupted rollback

`repair --resume-from <n>` runs the remaining statements of **the body that
failed**, and finishes the way that body's success finishes:

| Row               | `--resume-from` runs | On success                     |
| ----------------- | -------------------- | ------------------------------ |
| `direction=up`    | the up SQL           | the revision is marked applied |
| `direction=down`  | the down SQL         | the revision is **removed**    |

A finished rollback means the migration is no longer applied, and Ptah records
that by deleting its row — the same thing a rollback that never failed does.
The statement numbers are the ones the row reports, so `--resume-from` accepts
any statement of the down body, and a resume that fails again leaves the row
pointing at the new failure so you can fix it and resume once more.

A `no_transaction` down runs on one pinned physical database session. When a
resume skips a verified committed prefix, Ptah restores recognized
session-control statements such as `SET search_path` on the new session before
running the remaining down SQL. It refuses to guess after a prefix that created
temporary objects or has unclassified session-local effects.

The complete down body is validated before the applied revision is changed to
pending down. Top-level transaction-control statements are invalid in a
`no_transaction` down because they can leave the body uncommitted while Ptah's
per-statement revision checkpoints commit independently. A validation or pinned
session acquisition failure preserves the applied revision and does not run
down SQL.

On PostgreSQL, Ptah also verifies every `CREATE INDEX ... IF NOT EXISTS` in the
down body before it deletes the revision. The relation must be a usable index
on the intended target table. Each create is bound to the schema selected by
`search_path` when that statement ran, so equal raw names in different schemas
remain separate checks. A failed transactional check rolls the down body
back; a failed non-transactional check keeps the revision dirty with all
completed down statements recorded, so repair can finalize it after the
database state is corrected.

Without `--resume-from`, a rollback that already committed a statement is
**refused** rather than recorded applied: recording it applied would sign off
the migration over a schema whose objects the rollback already dropped. The
error names every way out.

- `--resume-from <n>` — finish the rollback and remove the revision.
- `--force` — record it applied, for when you restored by hand the schema the
  rollback had started to undo.
- `ptah migrations set --version <previous>` — move the revision boundary when
  you finished the rollback by hand.

A rollback that committed nothing needs none of this: a transactional down whose
body was rolled back, or a non-transactional one whose very first statement
failed, still has the migration fully applied, and plain
`ptah migrations repair --version <version>` clears it as it always has.

:::note
Rows written by a Ptah that did not yet record the direction read as up rows.
When the up and down bodies differ in statement count, `--resume-from` still
refuses them rather than replay the wrong body, naming both counts; when the two
bodies happen to be the same length such a row cannot be told apart, and
`--force` is the override in either case.
:::

**`ptah-compat migrate down` keeps the Atlas revision-table schema, but it does
not copy Atlas's failed-down defect.** Measured against Atlas (build,
2026-08-01), a down whose second statement fails leaves its row reading
`applied=2, total=2, error=''`, even when `-- atlas:txmode none` already
committed the first statement and left the schema half-reverted.

Ptah records that failure instead. For Atlas-format rows it stores the rollback
marker in `operator_version`, keeps `applied` and `total` as down-statement
progress, and writes the error fields already present in the Atlas schema. A
failed statement is therefore visible in `ptah-compat migrate status`, later
apply runs refuse to cross any unfinished rollback marker, and
`ptah migrations repair --revision-format atlas` resumes the down body. A
successful rollback still deletes the row.

`ptah-compat` intentionally has no repair verb. Resume its failed rollback
through the native command, using the same database, migration directory, and
revision schema as the compat command:

```bash
ptah migrations repair \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --dir-format atlas \
  --revision-format atlas \
  --version 2 \
  --resume-from 2
```

If the compat command used `--revisions-schema`, pass the same value as
`--migrations-schema` to repair. A repair that has already run every down
statement but stopped on a database-state safety check needs no
`--resume-from`; after the database is reconciled, rerun repair to finalize the
rollback without replaying SQL.

This is an intentional safety divergence: table interoperability is retained,
but an Atlas reader may report a Ptah-recorded failed rollback differently from
Atlas's own hidden failure. The same rule follows the **revision table format**,
not the binary, so native `ptah migrations down --revision-format atlas` gets
the same recoverable bookkeeping.

## Failure modes

- **The down SQL itself fails.** The command exits `2` and the failing version's
  revision row is marked dirty in both Ptah and Atlas table formats:

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
