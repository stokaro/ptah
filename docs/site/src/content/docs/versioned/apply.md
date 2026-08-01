---
title: Apply migrations
description: Run pending migrations with integrity verification, inspect status, wire operational hooks, and apply from OCI artifacts.
---

You have a hashed migration directory and a database that needs to catch up.
This page shows how to preview an apply, run it with integrity verification,
read the resulting state in scripts and CI, and wire the operational controls
a production run needs.

Prerequisites: a migration directory sealed with `ptah migrations hash` (see
[Generate migrations](../generate/)). The examples use a local SQLite file;
substitute your own `--db-url`.

## Preview with a dry run

On a fresh target, `--dry-run` prints every statement that would execute,
without touching the database:

```bash
ptah migrations up \
  --db-url "sqlite://app.db" \
  --migrations-dir ./migrations \
  --dry-run
```

Expected output includes:

```text
=== DRY RUN MODE ===
No actual changes will be made to the database

[DRY RUN] Would execute SQL ... sql="CREATE TABLE \"users\" ..."

✅ Dry run completed successfully!
Would have applied 1 migrations
```

:::note
The dry run simulates an uninitialized revision table: it walks every
migration in the directory, not only the ones pending on this target. Use
`ptah migrations status` to see what a real run would apply.
:::

## Apply with integrity verification

```bash
ptah migrations up \
  --db-url "sqlite://app.db" \
  --migrations-dir ./migrations \
  --verify-sum
```

Expected output includes:

```text
=== MIGRATE UP ===
Dialect: sqlite
Transaction mode: file

Current version: 0
Total migrations: 1
Pending migrations: 1

✅ Migrations completed successfully!
Database is now at version: 1785255952
```

A hashed directory (one that carries `ptah.sum` or `atlas.sum`) is always
verified before anything runs, and the apply aborts on drift — the same check
`ptah migrations validate` performs. A directory without a sum file is not
gated. `--verify-sum` additionally makes a missing sum file itself an error,
so set it for shared environments where the directory must be hashed; see
[Integrity and safety](../integrity-and-safety/).

Applied versions land in the revision table. Rerunning the same command is a
safe no-op:

```text
Pending migrations: 0
✅ Database is already up to date!
```

## Check status

```bash
ptah migrations status \
  --db-url "sqlite://app.db" \
  --migrations-dir ./migrations
```

Expected output includes:

```text
=== MIGRATION STATUS ===
Current Version: 1785255952
Total Migrations: 2
Applied Migrations: 1
Pending Migrations: 1
Out-of-order Migrations: 0
Status: ⚠️  Pending migrations available
```

For scripts, `--json` prints the same state as one object:

```bash
ptah migrations status \
  --db-url "sqlite://app.db" \
  --migrations-dir ./migrations \
  --json
```

```json
{
  "current_version": 1785255953,
  "applied_migrations": [1785255952, 1785255953],
  "pending_migrations": [],
  "out_of_order_migrations": [],
  "total_migrations": 2,
  "has_pending_changes": false
}
```

Set `--exit-code` in CI when pending migrations should fail the job: the
command then exits `1` while pending work exists and `0` once the database is
up to date.

## Execution controls

The defaults are safe for most runs; three controls matter once several
people and pipelines share a directory:

- **Transaction mode** (`--tx-mode`): `file` (default) wraps each migration
  in its own transaction; `all` runs the whole batch in one; `none` disables
  wrapping for statements that cannot run inside a transaction.
- **Batch limit** (`--limit`): apply only the first N pending migrations —
  useful for staged rollouts and verifying one step at a time. `--allow-dirty`
  is the explicit recovery escape hatch that proceeds past a dirty revision
  row (for example one left by a crashed migration whose file was later
  rebased); prefer `ptah migrations repair` for the dirty row itself.
- **Execution order** (`--exec-order`): `linear` (default) fails when a merge
  landed a pending migration below the current version; `linear-skip` warns
  and leaves it pending; `non-linear` applies it. Status reports such
  versions as out-of-order.
- **Timeouts and locks**: `--statement-timeout`, `--lock-timeout`, and
  `--migration-lock-timeout` bound long DDL and the session-level advisory
  lock that keeps two migrators from racing.

See [Configuration](../../reference/configuration/) for the `ptah.yaml`
equivalents of every control.

## Operational hooks

Production-like runs should be configured, not wrapped in ad hoc shell
scripts. `ptah migrations up` supports:

- `--pre-up-hook` — a shell command that must exit `0` before anything is
  applied (rollback runs have `--pre-down-hook`).
- `--webhook` — a URL that receives migration metadata and must return
  HTTP 200 before the run proceeds.
- `--pg-dump-to` / `--mysqldump-to` — a directory where a backup is written
  before migrations are applied.
- Revision-table placement (`--migrations-table`, `--migrations-schema`) and
  Prometheus metrics (`--metrics-addr`).

All of these can live in `ptah.yaml` instead of the command line; see
[Configuration](../../reference/configuration/).

## Apply from an OCI registry

`up`, `status`, and `down` accept an `oci://` reference as the migrations
directory, so the artifact your CI published is exactly what production runs
— pin it by immutable digest:

```bash
ptah migrations push \
  oci://ghcr.io/acme/app-migrations \
  --migrations-dir ./migrations \
  --verify-sum

ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir oci://ghcr.io/acme/app-migrations@sha256:<digest> \
  --verify-sum
```

See [OCI registry artifacts](../../operate/oci-registry/) for
authentication, tag and digest semantics, referrer reports, and CI wiring.

## Failure modes

**Integrity drift aborts before anything runs** (exit `2`):

```text
error: migration sum verification failed:
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

**Destructive pending migrations are refused by default** (exit `2`); rerun
with `--allow-destructive` after review:

```text
error: error running migrations: pending migrations contain destructive statements; rerun with --allow-destructive after review:
- 0000000003_drop_users.up.sql:1 DS101 error: DROP TABLE permanently deletes the table and every row in it; ...
```

Both gates are covered in depth in
[Integrity and safety](../integrity-and-safety/).

**A migration failed partway.** The revision table records a dirty state and
every later run refuses to continue until it is repaired — see
[Maintain migration history](../maintain-history/).

## Next steps

- Need to undo an applied migration? [Roll back migrations](../rollback/).
- Hardening the pipeline that runs this command?
  [Integrity and safety](../integrity-and-safety/).
- Fresh databases replaying years of history? [Checkpoints](../checkpoints/).
