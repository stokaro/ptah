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

Expected output on stdout:

```text
=== DRY RUN MODE ===
No actual changes will be made to the database

✅ Dry run completed successfully!
Would have applied 1 migrations
```

The statement-by-statement narration is a log record, not report output, so it
goes to the log stream — stderr by default:

```text
level=INFO msg="[DRY RUN] Would begin transaction"
level=INFO msg="[DRY RUN] Would execute SQL" sql="CREATE TABLE \"users\" ..."
level=INFO msg="[DRY RUN] Would commit transaction"
```

Both `--log-level` and `--log-format` apply to it: `--log-level warn` drops the
narration and leaves only the stdout report, and `--log-format json` moves every
record — report lines included — onto stdout as one JSON object per line, which
keeps `ptah migrations up ... | jq` parseable. See
[Execution controls](#execution-controls).

:::note
The dry run reads an existing revision table and walks only migrations pending
on this target. On a fresh target, it treats the absent revision table as empty
without creating it. A legacy three-column Ptah revision table is read without
upgrading its layout. A partially upgraded table is rejected with a missing
column diagnostic and is not modified.
:::

:::note
A dry run predicts, it does not evaluate everything. Pre-migration checks are
reads, so a preview evaluates a migration's assertions only where the state it
observes is the state a real apply would give it — the first migration executed
in the run. Later migrations' assertions are still parsed and statically
validated, but their database evaluation is deferred and the run says so on
stderr. See
[Checks in a dry run](../integrity-and-safety/#checks-in-a-dry-run).
:::

:::caution
The per-statement narration is native-only: `ptah-compat migrate apply
--dry-run` does not emit it, matching Atlas CE. Compat still writes
safety-relevant notes to stderr — an active `PTAH_SKIP_CHECKS` bypass, and any
deferred pre-migration checks — so keep the streams apart rather than folding
them together: `ptah-compat ... --format '{{ json . }}' | jq` sees exactly one
JSON document, while `2>&1` would mix those notes into it. See
[Atlas migrate commands](../../atlas/migrate-commands/).
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
gated on this native surface. `--verify-sum` additionally makes a missing sum
file itself an error, so set it for shared environments where the directory
must be hashed. `ptah-compat migrate apply` refuses a never-hashed Atlas
directory outright, because Atlas does; see
[Integrity and safety](../integrity-and-safety/).

Either check compares the directory against the sum stored beside it, so what
it establishes depends on where the directory came from. See
[Apply from an OCI registry](#apply-from-an-oci-registry) for what that means
when the directory is a registry artifact.

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
  in its own transaction; `all` runs the selected batch in one; `none`
  disables wrapping. A migration may select `file` or `none` with a leading
  `-- atlas:txmode <mode>` header followed by a blank line. Explicit file modes
  conflict with global `all`.
- **Batch limit** (`--limit`): apply only the first N pending migrations —
  useful for staged rollouts and verifying one step at a time. `--allow-dirty`
  is the explicit recovery escape hatch that proceeds past a dirty revision
  row. When the dirty row belongs to a migration that is still pending — the
  usual case, a body that failed part-way — the retry reuses that row rather
  than recording a second one, and skips the statements the earlier attempt
  committed. Before it skips anything, Ptah verifies that the committed source
  prefix is unchanged. Native rows carry a `partial:h1:` prefix checksum;
  Atlas-format rows carry cumulative `partial_hashes`. Editing only the
  unapplied suffix is allowed. A failed retry cannot reduce `applied` below the
  previously committed prefix, even when the transaction mode changed.
  Pre-migration checks are not rerun after committed progress because they
  describe the original pre-migration state. Automatic continuation is
  up-direction only: a row left dirty by an interrupted rollback is refused so
  up SQL cannot be resumed from a down-statement offset. Use
  `ptah migrations repair` when the row cannot be resumed automatically: an
  interrupted rollback, a process whose last statement has an unknown outcome,
  changed or unverifiable committed-prefix metadata, an edit that changed the
  file's statement count, or a dirty row for a migration whose file was rebased
  away. Legacy dirty rows without prefix metadata may resume only while their
  full-file checksum still matches.
- **Execution order** (`--exec-order`): `linear` (default) fails when a merge
  landed a pending migration below the current version; `linear-skip` warns
  and leaves it pending; `non-linear` applies it. Status reports such
  versions as out-of-order.
- **Timeouts and locks**: `--statement-timeout`, `--lock-timeout`, and
  `--migration-lock-timeout` bound long DDL and the session-level advisory
  lock that keeps two migrators from racing. A migration that resolves to
  `none` cannot use statement or lock timeouts. Ptah rejects the combination
  before executing SQL or changing the revision row. A file-level `file`
  override under global `none` restores the transaction and may use timeouts.
  SQL-backed non-transactional migrations record a durable progress marker
  before and after each autocommit statement. A custom Go `MigrationFunc`
  remains opaque and is recorded only when it returns.
- **Run logging** (`--log-level`, `--log-format`): `--log-level`
  debug\|info\|warn\|error selects how much of the run is narrated —
  `warn` silences the per-statement dry-run narration — and `--log-format`
  text\|json selects the encoding. `json` moves every record, the stdout
  report included, onto stdout as one JSON object per line.

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

Pin the digest deliberately: `--verify-sum` checks the pulled directory against
the sum shipped inside the same artifact, so over a movable tag it proves the
files are internally consistent, not that they are the reviewed ones. `up`
prints that qualification, along with the digest the tag resolved to and the
reference that pins it, whenever a sum verifies over a tag-resolved artifact.
The run still succeeds; a digest reference gets no such line.

To keep the readable name and the pin together, write both:

```bash
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir oci://ghcr.io/acme/app-migrations:release@sha256:<digest> \
  --verify-sum
```

The digest selects the bytes and is verified against what the registry returns;
the tag is a label. Repointing `:release` afterwards changes nothing about what
this command runs, and this reference counts as a digest pin, so it gets no
movable-tag qualification.

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
- 0000000003_drop_users.up.sql:1 DS101 error: DROP TABLE permanently deletes table users and every row in it; ...
```

Both gates are covered in depth in
[Integrity and safety](../integrity-and-safety/).

**A migration failed partway.** The revision table records a dirty state and
every later run refuses to continue until it is repaired — see
[Maintain migration history](../maintain-history/). A failed *rollback* records
the same recoverable state in both revision-table formats;
`ptah-compat migrate down` keeps the Atlas table layout but does not copy
Atlas's hidden failed-down state. [Roll back migrations](../rollback/) shows
how to resume it through the native repair command.

**A non-transactional statement was interrupted.** If the process exits, the
context is canceled, or its deadline expires while an autocommit statement is
in flight, the revision row preserves the last known completed statement and
marks the interrupted statement's outcome as unknown. Inspect the database
before repair. Ptah rejects `repair --resume-from` while this marker is present
because the SQL may already have committed.

**A concurrent index build failed on PostgreSQL** (exit `2`). The invalid index
left behind keeps the name, so re-issuing the generated `IF NOT EXISTS`
statement is skipped rather than retried and reports no error. Ptah refuses to
run the migration while an index it creates is unusable, and names the `REINDEX
INDEX CONCURRENTLY` that rebuilds it:

```text
error: error running migrations: migration 1785756328 cannot be applied: PostgreSQL reports index "public"."idx_members_email" (indisvalid=false, indisready=false) unusable, and CREATE INDEX ... IF NOT EXISTS finds the name taken and skips it rather than rebuilding it, so this run would record the migration applied over a constraint that is not enforced; run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", or drop the index, then run the migration again
```

`--allow-dirty` does not bypass this — retrying the body is what the refusal is
about. An invalid *unique* index enforces nothing, so without the refusal the
run would exit `0`, report the database up to date, and keep accepting duplicate
rows. Rebuild the index with the `REINDEX` the message names, or drop it so the
name is free and the statement builds it, then run again. Only indexes the
migration itself creates are checked, and only on PostgreSQL; other dialects
have no concurrent index build to leave half-finished. A dry run is exempt,
because it records nothing. `ptah migrations repair` refuses on the same
grounds — see [Maintain migration history](../maintain-history/).

An intentional `DROP INDEX` followed by a matching create is allowed when the
drop will execute in the current attempt. A statement skipped by dirty-resume
cannot serve as that cleanup. Ptah resolves both unqualified drops and target
tables through `search_path`, then checks the schema-level relation name. An
index on another table or a non-index relation with that name is a conflict.
After the body, Ptah positively verifies on the active transaction or connection
that the named index exists, belongs to the intended table, and has an
acceptable catalog state before recording the revision clean. A partitioned
parent index created with `CREATE INDEX ... ON ONLY` is accepted in its expected
ready-but-incomplete catalog state.

**A pre-migration check blocked the migration.** Nothing is applied and no
revision row is written, so the run is recorded as never started. Fix the data
the check guarded and re-run; no repair step and no bypass flag is involved.
`ptah migrations up --skip-checks` exists as an emergency override, and the
Atlas-compatible `ptah-compat migrate apply` has no such flag, matching Atlas.

## Next steps

- Need to undo an applied migration? [Roll back migrations](../rollback/).
- Hardening the pipeline that runs this command?
  [Integrity and safety](../integrity-and-safety/).
- Fresh databases replaying years of history? [Checkpoints](../checkpoints/).
