---
title: The migration directory
description: The file layout, integrity file, revision table, and directory formats behind Ptah's versioned workflow.
---

A migration directory is versioned schema history as files: every change is an
ordered pair of SQL files that lives in your repository, sealed by an
integrity file, while each target database records which versions it has
applied. The directory says what *can* run; the database says what *has* run;
everything the versioned workflow does is derived from the difference.

## How Ptah models it

A native migration directory holds one pair per version plus the integrity
file:

```text
1785255952_init.up.sql
1785255952_init.down.sql
1785255953_add_posts.up.sql
1785255953_add_posts.down.sql
ptah.sum
```

- **Versions order execution.** Generated and manually created migrations use
  a timestamp; imported migrations keep their source tool's versions.
- **Every version has both directions.** Ptah refuses to register a directory
  with a missing up or down half — rollback support is part of the migration
  contract, not an optional extra.
- **`ptah.sum` is the integrity file**: a hash of the directory and every
  migration file, committed alongside them, so out-of-band edits are detected
  before they are applied.
- **A checkpoint** is a special pair carrying a `.checkpoint` marker that
  fresh databases bootstrap from instead of replaying all of history — see
  [Checkpoints](../../versioned/checkpoints/).

Applied versions are recorded in a **revision table** in the target database
(`schema_migrations` by default). Pending work is the set of directory
versions not present in that table, so a migration merged below the current
version is still detected; how it is treated is an execution-order policy on
[Apply migrations](../../versioned/apply/).

## Directory formats

Ptah reads its native split-file layout and supported Atlas-format
directories. Format detection is automatic; pass `--dir-format atlas` (or
`ptah`) when auto-detection should not guess:

```bash
ptah migrations validate --dir ./migrations --dir-format atlas
```

Atlas-format directories use `atlas.sum` as their integrity file and can be
tracked with Atlas revision-table metadata (`--revision-format atlas`). The
Atlas-compatible command surface operates on the same directories through
`ptah atlas migrate ...` — see the
[Atlas compatibility overview](../../atlas/overview/).

## Consequences

- **The directory is portable.** Every environment replays the same files in
  the same order, and the directory can be published to and consumed from an
  OCI registry (`--migrations-dir oci://...`) without changing its meaning.
- **Edits are a maintenance operation.** Because files are hashed and ordered,
  changing history has dedicated commands rather than ad-hoc file edits — see
  [Maintain migration history](../../versioned/maintain-history/).
- **State questions have two answers.** "What does the schema look like?" is a
  question for the database; "what is pending?" is a question for the
  directory minus the revision table (`ptah migrations status`).

## Where it appears

- The lifecycle that produces and consumes the directory: [Versioned migrations](../../versioned/overview/).
- Sealing and verifying it: [Integrity and safety](../../versioned/integrity-and-safety/).
- Bringing history from another tool into it: [Import from another tool](../../versioned/import/).
