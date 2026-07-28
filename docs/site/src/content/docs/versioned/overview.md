---
title: Versioned migrations
description: The migration lifecycle in Ptah - what a migration directory is, the core loop, and where each task lives.
---

Versioned migrations are the operational boundary between your desired schema
and live database state: every change becomes a reviewed, ordered pair of SQL
files that is hashed, linted, applied, and reversible. This page gives you the
mental model and the core loop; each lifecycle step has its own page with
runnable examples and failure guidance.

Treat migration files as code. They live in your repository, they are covered
by an integrity file, and they go through review before they reach a shared
database.

## The core loop

```bash
ptah migrations plan \
  --root-dir ./models \
  --db-url "$DATABASE_URL"

ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations

ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations

ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --verify-sum
```

Preview the SQL, write the files, seal them, and apply them. Each step is
covered in depth on its own page:

| Task | Page |
| --- | --- |
| Plan and write migration files from schema differences | [Generate migrations](../generate/) |
| Apply pending migrations and inspect state | [Apply migrations](../apply/) |
| Roll back to an earlier version | [Roll back migrations](../rollback/) |
| Hash, validate, lint, and gate destructive changes | [Integrity and safety](../integrity-and-safety/) |
| Edit, reorder, delete, or repair migrations | [Maintain migration history](../maintain-history/) |
| Adopt an existing golang-migrate, Goose, Flyway, or Liquibase directory | [Import from another tool](../import/) |
| Squash long history into a bootstrap snapshot | [Checkpoints](../checkpoints/) |
| Reconcile reference/lookup rows declaratively | [Reference data](../reference-data/) |

## What a migration directory contains

A Ptah migration directory holds one pair of files per version, plus the
integrity file:

```text
1785255952_init.up.sql
1785255952_init.down.sql
1785255953_add_posts.up.sql
1785255953_add_posts.down.sql
ptah.sum
```

- **Versions** order execution. Generated and manually created migrations
  use a timestamp; imported migrations keep their source tool's versions.
- **Every version has both directions.** Ptah refuses to register a directory
  with a missing up or down half — rollback support is part of the migration
  contract, not an optional extra.
- **`ptah.sum`** is the integrity file: a hash of every migration file,
  committed alongside them, so out-of-band edits are detected before they are
  applied. See [Integrity and safety](../integrity-and-safety/).

Applied versions are recorded in a revision table in the target database
(`schema_migrations` by default). Pending work is the set of directory
versions not present in that table, so a migration merged below the current
version is still detected; how it is treated is an execution-order policy on
[Apply migrations](../apply/). A [checkpoint](../checkpoints/) is a special
pair carrying a `.checkpoint` marker that fresh databases bootstrap from.

## Directory formats

Ptah reads its native split-file layout and supported Atlas-style migration
directories. Format detection is automatic; pass `--dir-format atlas` (or
`ptah`) when auto-detection should not guess:

```bash
ptah migrations validate --dir ./migrations --dir-format atlas
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --dir-format atlas
```

Atlas-format directories use `atlas.sum` as their integrity file and can be
tracked with Atlas revision-table metadata (`--revision-format atlas`).

Atlas-compatible command paths for the same lifecycle live under
`ptah atlas migrate ...`:

```bash
ptah atlas migrate hash --dir ./migrations
ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations
```

## Next steps

- Producing your first migration files? [Generate migrations](../generate/).
- Starting from a database or migration history built outside Ptah?
  [Adopt an existing database](../../start/adopt-an-existing-database/).
- Still deciding between versioned files and direct applies?
  [Choose a workflow](../../start/choose-a-workflow/).
