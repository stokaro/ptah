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
Dirty Migration: version=5 state=failed applied=1/2
Error Statement: INSERT INTO missing_table (id) VALUES (1)

Run 'ptah migrations repair --version <version>' after fixing the database state.
```

Fix the migration file (it is unapplied, so `edit` applies), re-hash, and
repair. `--resume-from` executes the remaining up statements — here starting
at the second — before marking the migration applied:

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

## Atlas-compatible surface

`ptah atlas migrate edit`, `ptah atlas migrate rebase`, and
`ptah atlas migrate rm` forward to these native commands for drop-in Atlas
familiarity, with `--dir` mapping to the migrations directory. Squashing
history is its own verb pair — see [Checkpoints](../checkpoints/).

## Next steps

- History too long rather than wrong? [Checkpoints](../checkpoints/).
- Undoing an applied migration instead of editing a pending one?
  [Roll back migrations](../rollback/).
- Keeping edits honest in CI? [Integrity and safety](../integrity-and-safety/).
