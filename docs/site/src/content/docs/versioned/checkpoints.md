---
title: Checkpoints
description: Squash migration history into a cumulative-schema checkpoint that fresh databases bootstrap from.
---

As a migration directory grows, every fresh database — a CI job, a new
developer machine, a throwaway shadow database — replays the whole history,
including long-dead intermediate DDL for columns and tables that were later
dropped or renamed. A **checkpoint** captures the cumulative schema at a version
so a fresh database can bootstrap from that snapshot and skip the squashed
history, while an already-migrated database ignores the checkpoint and keeps
applying only genuinely pending migrations.

Atlas keeps `migrate checkpoint` in its proprietary Pro build (an Atlas account
and the closed-source binary). Ptah provides checkpoints as an MIT, local,
no-account, embeddable capability.

## Create a checkpoint

`ptah migrations checkpoint` replays the whole directory on an ephemeral
**shadow database**, introspects the resulting schema, and writes a checkpoint
migration whose up body is the full cumulative schema:

The shadow connection's live capabilities and server-resolved identifier
equivalence snapshot are retained through checkpoint planning, including SQL
Server locale, accent, case, kana, and width semantics.

```bash
ptah migrations checkpoint \
  --migrations-dir ./migrations \
  --shadow-db postgres://user:pass@localhost/shadow
```

The shadow database is dropped and replayed from scratch, so it must be an
ephemeral, disposable database — never a real environment. The dialect is
inferred from the shadow database URL; pass `--dialect` only to assert it
explicitly. Use `--dry-run` to print the checkpoint SQL without writing files:

```bash
ptah migrations checkpoint --migrations-dir ./migrations \
  --shadow-db "sqlite://$(mktemp -u).db" --dry-run
```

| Flag | Purpose |
| --- | --- |
| `--shadow-db` | Ephemeral database the directory is replayed into (required). |
| `--migrations-dir` | Directory to checkpoint (default `./migrations`). |
| `--version` | Checkpoint version; defaults to one above the newest migration (`ptah` format) or a UTC timestamp bumped past it (`atlas` format). Must be positive and above every existing version; at most ten digits under `ptah`, and never exactly ten digits under `atlas`. |
| `--description` | Description used in the file name (default `checkpoint`). |
| `--dialect` | Asserted dialect; inferred from the shadow database when omitted. |
| `--schemas` | Comma-separated schemas to introspect. |
| `--dir-format` | Checkpoint convention: `ptah` (default) or `atlas`. `auto` is refused. |
| `--dry-run` | Print the checkpoint SQL instead of writing files. |

## File format

A checkpoint is an ordinary Ptah migration pair carrying a `.checkpoint` marker
between the description and the direction:

```text
0000000042_squash.checkpoint.up.sql
0000000042_squash.checkpoint.down.sql
```

The description (`squash` here) comes from `--description` and defaults to
`checkpoint`.

The up body is the full cumulative `CREATE` schema in dependency order; the down
body drops it in reverse. The marker is recognized by discovery and parsing
without changing how ordinary `up`/`down` files are read, so a checkpoint and
the historical migrations it squashes coexist in one directory.

`--dir-format atlas` writes the Atlas convention instead: a single up-only file
named `<version>_<description>.sql` whose **first line** is the
`-- atlas:checkpoint` directive, with `atlas.sum` refreshed rather than
`ptah.sum`. The version is a UTC timestamp (`20060102150405`), as Atlas writes
it. There is no down body — the Atlas format is up-only, so an Atlas-format
checkpoint is not reversible. See
[Atlas-compatible surface](#atlas-compatible-surface) below.

## How a checkpoint applies

The behavior depends entirely on whether the target database has any applied
migrations, so the same directory does the right thing everywhere.

- **Fresh database** (empty revision table): `ptah migrations up` runs the
  newest checkpoint's up body, then applies only migrations at or after the
  checkpoint version. The squashed pre-checkpoint migrations are never run
  individually — the checkpoint's own revision row records that history as
  satisfied, so no per-migration rows are written for them.
- **Already-migrated database** (non-empty revision table): the checkpoint is
  ignored entirely and history is applied unchanged. A checkpoint never runs on
  a database that already has migrations applied.

`ptah migrations status` reflects the same decision, so a fresh database lists
the checkpoint plus post-checkpoint migrations as pending, and an already-migrated
database does not list the checkpoint as pending.

Because selection is driven by the checkpoint's own applied state rather than a
separately written baseline, the model is crash-safe: an interrupted bootstrap
resumes to the same end state on the next `up`.

## Integrity

Checkpoint files are covered by the directory's integrity file exactly like
ordinary migrations, so `ptah migrations checkpoint` rewrites it after writing
the checkpoint and a tampered checkpoint fails `ptah migrations validate`. That
is `ptah.sum` for the Ptah convention and `atlas.sum` for `--dir-format atlas`.
Commit the checkpoint files and the updated sum together.

## Rollback boundary

A checkpoint's down body is meaningful only for a database that bootstrapped
from it. History below the checkpoint boundary no longer has individually
applied migrations to reverse, so `ptah migrations down` to a version between
the checkpoint and the squashed history fails with a clear error rather than
silently doing nothing. You can roll back to the checkpoint boundary, or all the
way to `0` to drop everything (which runs the checkpoint's down body); to land on
an intermediate pre-checkpoint version, restore from a backup or rebuild.

## Checkpoint versus baseline

Both let a database skip running historical migrations, but they solve opposite
problems:

- `ptah migrations baseline` records existing migrations as already applied
  **without executing their SQL** — for adopting Ptah on a database whose schema
  already exists.
- `ptah migrations checkpoint` **executes** a cumulative snapshot on a fresh
  database and then continues with post-checkpoint migrations — for shrinking
  replay time as history grows.

They compose: baseline onto an existing database, checkpoint to keep fresh-setup
fast.

## Atlas-compatible surface

The `ptah-compat` binary's `migrate checkpoint [name]` forwards to the native
command for drop-in Atlas familiarity: `--dir` maps to the migrations directory, `--dev-url`
to the shadow database, and the optional positional name to the checkpoint
description.

On the compat surface `--dir-format` defaults to `atlas`, matching the default
Atlas registers, so an unflagged Atlas pipeline gets an Atlas-format checkpoint
back. The native command keeps `ptah` as its default. Pass `--dir-format ptah`
on compat, or `--dir-format atlas` natively, to select the other convention.

Reading Atlas-format checkpoints works today: a migration whose first line is
the `-- atlas:checkpoint` file directive (as written by Atlas's own
`migrate checkpoint`) gets the same semantics from both
`ptah-compat migrate apply` and `ptah migrations up` — a fresh database
bootstraps from the latest checkpoint, and a database that already applied
pre-checkpoint history skips the checkpoint silently, matching measured Atlas
behavior.
