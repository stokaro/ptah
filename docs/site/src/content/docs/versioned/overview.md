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

## The migration directory

A migration directory holds one `*.up.sql`/`*.down.sql` pair per version plus
the `ptah.sum` integrity file, and each target database records its applied
versions in a revision table (`schema_migrations` by default). Pending work
is the set of directory versions not yet in that table. The full model —
file layout, version ordering, integrity files, checkpoint markers, and the
native and Atlas directory formats — is on
[The migration directory](../../concepts/migration-directory/).

Atlas-compatible command paths for the same lifecycle live in the separate
`ptah-compat` drop-in binary:

```bash
ptah-compat migrate hash --dir ./migrations
ptah-compat migrate apply --url "$DATABASE_URL" --dir ./migrations
```

## Next steps

- Producing your first migration files? [Generate migrations](../generate/).
- Starting from a database or migration history built outside Ptah?
  [Adopt an existing database](../../start/adopt-an-existing-database/).
- Still deciding between versioned files and direct applies?
  [Choose a workflow](../../start/choose-a-workflow/).
