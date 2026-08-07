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
  [Checkpoints](../../versioned/checkpoints/). Atlas-format directories mark
  checkpoints with a first-line `-- atlas:checkpoint` file directive instead,
  which Ptah honors with the same bootstrap-or-skip semantics.

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
Atlas-compatible command surface — the `ptah-compat` drop-in binary — operates
on the same directories through its `migrate ...` commands; see the
[Atlas compatibility overview](../../atlas/overview/).

## Stable local snapshots

Native `migrations up`, `migrations down`, `migrations status`,
`migrations set`, and `lint`, plus Atlas-compatible `migrate apply`,
`migrate down`, `migrate status`, `migrate set`, and `migrate lint`, open a
local migration directory through a rooted handle and capture an immutable
in-memory snapshot before connecting to a database. Their reports and rollback
verification reuse that snapshot. It contains migration SQL,
`.ptah-lint.yaml`, `ptah.sum`, and `atlas.sum`; unrelated files are excluded.

The snapshot is also what the integrity gate reads. On `migrate apply`,
`migrate status`, and `migrate set` the captured `atlas.sum` is verified
immediately after capture and before the database connection, so all three
refuse a directory that carries no integrity file or whose integrity file is
stale ([#974](https://github.com/stokaro/ptah/issues/974)). Verifying the
snapshot rather than the live directory is what makes the check and the work it
guards read the same bytes.

Relative CLI paths are rooted at the process working directory. Traversal and
symlink escapes outside that root are rejected. Explicit absolute paths remain
supported. A relative `migration.dir` in `atlas.hcl` resolves from the project
file's directory. Ptah keeps that project directory handle open from
`atlas.hcl` evaluation, including `file()` and `fileset()`, through migration
capture, so replacing the project pathname cannot redirect the command.
Parent-relative project paths such as `../shared-migrations` are intentionally
external and use unrooted compatibility semantics. An explicit CLI `--dir`
keeps CLI path semantics.

Ptah reads the directory twice and accepts it only when both observed captures
match. This best-effort check rejects observed differences, but cannot defeat
coordinated writers or an ABA change that restores the original bytes before
the next observation. Hostile writers require trusted immutable input, manifest
or process controls, or a filesystem-level snapshot. After acceptance, checksum
verification, migration registration, destructive linting, shadow rollback
verification, execution, and template reports all consume the same captured
bytes.

## Writing back to the directory

The verbs that write a migration directory — `migrate diff`, `migrate new`, and
native `ptah migrations generate` and `ptah migrations create` — bind it the
same way and keep the binding. They open the directory and its parent once,
before staging, and every staged file, published migration, `atlas.sum`,
journal, commit marker, rollback quarantine and cleanup entry is named as a
direct child of one of those two handles. A migration directory that does not
exist yet is created through the bound parent, so it is materialized where the
run looked rather than where the pathname points by then. Recovery of an
interrupted batch runs through the same handles.

Replacing the directory after the run validated it therefore cannot redirect
what it writes, and a directory configured through `atlas.hcl` stays inside the
opened project root. See
[the publication boundary](../../atlas/migrate-commands/#the-publication-boundary)
for what remains keyed to the pathname and why.

`ptah migrations generate` plans and publishes in two steps, so the directory
can be replaced between them by something outside the run's control. The plan
records the planned directory's filesystem identity as well as its contents, and
publication refuses a directory that is no longer the same object — including a
substitute that holds exactly the files the plan verified. The refusal is
`migration directory changed after migration planning`, and nothing is written
([#1118](https://github.com/stokaro/ptah/issues/1118)). This changes behavior:
a run that previously published into a recreated directory now fails instead.

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
