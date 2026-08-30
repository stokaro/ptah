---
title: Versioned migrations
description: The migration lifecycle in Ptah - what a migration directory is, the core loop, and where each task lives.
type: landing
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "When should I use versioned migrations, and which task comes next?"
goal: "Choose the versioned workflow when appropriate and open the correct next task."
sourceOfTruth:
  - "cmd/migrations"
  - "migration"
generated: false
overlaps:
  - "/start/choose-a-workflow/"
  - "/concepts/migration-directory/"
disposition: rewrite
---

Versioned migrations are the operational boundary between a change and live
database state: every change becomes a reviewed, ordered pair of SQL files that
is hashed, linted, applied, and reversible. Whether you write those files or
Ptah derives them from a desired schema is a separate decision, and this page
covers both; each lifecycle step then has its own page with runnable examples
and failure guidance.

Treat migration files as code. They live in your repository, they are covered
by an integrity file, and they go through review before they reach a shared
database.

## Where the files come from

Two ways, and the lifecycle after them is the same. Only the first step differs,
so a project that never describes a desired schema uses every verb below except
`plan` and `generate`.

### You write them

```bash
ptah migrations create add_orders --migrations-dir ./migrations
# write the SQL in the generated *.up.sql and *.down.sql
```

`create` scaffolds the pair and leaves their contents to you. Nothing here reads
a schema source, and nothing later asks for one.

### Ptah derives them from a desired schema

```bash
ptah migrations plan \
  --root-dir ./models \
  --db-url "$DATABASE_URL"

ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations
```

`plan` previews the SQL for the difference between the desired schema and the
database; `generate` writes that difference as the same pair of files.

## The lifecycle both share

```bash
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations

ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --verify-sum
```

Seal the directory, check it against its own integrity file, and apply what is
pending. Each step is covered in depth on its own page:

| Task | Page |
| --- | --- |
| Write a migration by hand, or plan and write one from schema differences | [Generate migrations](../generate/) |
| Apply pending migrations and inspect state | [Apply migrations](../apply/) |
| Roll back to an earlier version | [Roll back migrations](../rollback/) |
| Hash the directory, validate it, and assert preconditions | [Integrity and safety](../integrity-and-safety/) |
| Lint the SQL and gate destructive changes | [Lint and gate unsafe SQL](../lint/) |
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
