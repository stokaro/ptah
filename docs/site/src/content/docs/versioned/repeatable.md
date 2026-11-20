---
title: Repeatable migrations
description: "Keep a view, a routine or another re-runnable script in one file that runs again after the versioned migrations whenever its content changes."
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I keep a view or a routine in one file that runs again whenever I change it?"
goal: "Write, apply and recover repeatable migrations, and know what they refuse."
sourceOfTruth:
  - "migration/migrator"
  - "migration/migrationfile"
generated: false
overlaps:
  - /versioned/import/
disposition: keep
---

A versioned migration runs once, at its version, and its file must not change
after it applied. Some objects are easier to keep as one current definition: a
view, a function, a grant script. A repeatable migration is that definition. It
has no version, `ptah migrations up` runs it after the versioned migrations, and
the next run executes it again only when its content changed.

Repeatable migrations run on PostgreSQL, CockroachDB, YugabyteDB, MySQL,
MariaDB, SQLite, SQL Server, Oracle and ClickHouse, and on an engine reached
through a provider that keeps their record; see
[Through a provider](#through-a-provider).

## Write one

A repeatable migration is a file named `R__<identity>.sql` in a directory read
in the Ptah format:

```text
migrations/
  0000000001_create_accounts.up.sql
  0000000001_create_accounts.down.sql
  R__account_names.sql
  views/R__active_accounts.sql
```

The identity is the part between `R__` and `.sql`. It takes ASCII letters,
digits, underscores and hyphens, at most 255 of them, and no dot, so
`R__view.up.sql` is refused rather than read as the identity `view.up`. The
extension is spelled `.sql` exactly. Two identities that differ only in letter
case are refused, because a case-insensitive filesystem cannot hold both. The
file may sit in a subdirectory; its identity is still the file name alone.

The database records the migration under its identity, so renaming the file
makes a new repeatable migration, which the next up run executes. The record of
the old name stays; see [Remove or rename one](#remove-or-rename-one).

A repeatable migration has no down file and takes no `-- +ptah check`
directive. It does take `-- +ptah no_transaction` and the timeout directives a
versioned up file takes.

**Write the body so that it can run again.** The second run executes the whole
file against a database where the first run already happened, so use
`CREATE OR REPLACE VIEW`, `CREATE OR REPLACE FUNCTION`, or a `DROP ... IF
EXISTS` before the `CREATE`:

```sql
CREATE OR REPLACE VIEW account_names AS SELECT id, name FROM accounts;
```

`--dir-format auto` reads the Ptah format only when the directory also holds a
Ptah versioned file, so a directory of repeatable migrations alone needs
`--dir-format ptah`. In the Atlas format, `R__<name>.sql` is the one-time
version `R`, as Atlas reads it. `R` is not a number: the directory runs over
the Atlas revision table (`--revision-format atlas`), and lint reports the file
under `file-form/refused-file` without that flag. `ptah-compat` reads it as a repeatable
migration when asked; see [The compatibility surface](#the-compatibility-surface).

## What an up run does

`ptah migrations up` runs the selected versioned migrations first. Once all of
them have succeeded, it runs every repeatable migration that is new or whose
content changed, in byte order of the identities, under the same migration
lock. A repeatable migration whose content is unchanged sends no statement.

What a run checks before it changes anything covers its repeatable work too.
The [destructive-change gate](../lint/#the-destructive-change-gate) judges each
repeatable migration the run executes because its file is new, changed or
retried, and not one whose content already applied. `--pre-up-hook`,
`--webhook`, `--pg-dump-to` and `--mysqldump-to` run before a run whose only
work is a repeatable migration.

`ptah migrations lint` reads a repeatable migration as an up migration that has
no down file, so `file-form/missing-down` and `file-form/file-name` do not report it. `--latest N` never
selects one, because it has no version; `--git-base` selects one whose file
changed.

Byte order puts digits before upper case and upper case before lower case, and
compares character by character: `10` runs before `9`. Pad a numeric prefix,
`R__09_base.sql` and `R__10_views.sql`, when the order matters.

Each repeatable migration runs in a transaction of its own, unless its file
says `-- +ptah no_transaction` or the run uses `--tx-mode none`. Under
`--tx-mode all` the transaction around the versioned migrations commits first,
and each repeatable migration then runs in its own.

That holds on an engine that rolls schema changes back as a unit: PostgreSQL,
SQLite, SQL Server, and CockroachDB 24 and older. MySQL, MariaDB, Oracle and
CockroachDB from 25 commit the open transaction before a schema statement, and
YugabyteDB keeps schema changes through a rollback, so a transaction there
would not undo a failed body, and ClickHouse has no transactions. On those
engines every repeatable migration runs without a transaction, and its record
says `no_transaction=true`. Oracle commits each schema statement as it runs, so
a failed body there leaves every statement that ran before the failing one. A
body that changes data only gets no transaction either, so write it to converge
when it runs again from its first statement. A file that asks for a
transaction with `-- atlas:txmode file` is refused there before anything runs.

What "changed" means is the digest of the file: `h1:` followed by the base64
SHA-256 of its bytes, the same value `ptah.sum` holds for the file. The run
records the digest that applied only after the body succeeded. Inside a
transaction the record and the body commit together.

Two runners against one database take the migration lock in turn. The second
reads the record after the first released the lock, finds the digest applied,
and runs nothing. On Oracle the lock is a row lock that Ptah keeps on a session
of its own; see [the Oracle page](../../databases/oracle/#the-migration-lock).
CockroachDB has no session advisory lock, and Ptah takes none on ClickHouse, so
it runs every migration on those two engines without one, repeatable migrations
included: run one migrator at a time against them.

Without the lock, the record still decides which of two runners executes a
changed repeatable migration, on every engine but ClickHouse. Each run has an
ID of its own, and the record keeps the ID of the run that last attempted each
file. A runner claims the row before the body runs by writing its ID over the
one it read, and the write matches only while the row still holds that ID. A
new file is claimed by inserting its row, and the record's key refuses the
second insert. When a claim does not succeed cleanly, the runner reads the row
back: its own ID means the claim took effect, and another run's ID means it
lost. The runner that loses refuses with
`another run changed the repeatable migration's record after this run read it`
before any statement of the body.

### On ClickHouse

Nothing decides between two runs on ClickHouse. An `ALTER TABLE ... UPDATE`
reports no count of the rows it matched, and the record's `PRIMARY KEY` does
not make an identity unique, so two runs that read the same record both run
the body, and both insert a row for a new file. Run one migrator at a time
against ClickHouse.

The attempt of a new file inserts its row with an `INSERT`. The attempt of a
file the record already holds, a failure and a completion are mutations: each
is an `ALTER TABLE ... UPDATE` with `mutations_sync = 1`, which rewrites the
data parts that hold the row and waits until it has. The wait covers only the
replica that ran the statement. With a replicated engine named through
`--migrations-engine`, a run against another replica can read the record
before the mutation reaches it and execute the body again, so point every run
at the same replica. A repair removes a row with a lightweight `DELETE`, which
marks the row deleted rather than rewriting its parts, and needs a
`MergeTree`-family engine that supports it.

The statement that records a body carries the body in its text, because the
driver writes bound values into the statement, and the server parses no
statement longer than `max_query_size`, 262144 bytes by default. A file whose
record would be longer is refused before anything runs, with an error that
gives the limit and the setting. A backslash, a single quote or a NUL byte in
the file counts twice, because it is escaped. To run a larger file, raise
`max_query_size` for the migration user, in its settings profile.

The run reads `max_query_size` from `system.settings` before anything runs,
so the migration user must be able to read that table. On a server whose
access control requires a grant for system tables, give the migration user
`SELECT` on `system.settings`; without it, every run with a pending repeatable
migration fails before it starts.

### Through a provider

An engine reached through an executable provider runs repeatable migrations
when the provider lists `sessions.repeatable-record` for that engine. The
provider stores the record, and Ptah decides what each row means, as on a
built-in engine: which file runs, the claim, a retry and a repair. A claim
with an unknown outcome, such as after a dropped connection, is settled by
reading the row back.

The body runs in a transaction when the provider establishes transactional DDL
for the session, and the row that records the content commits with it. Under
`--tx-mode none`, under `-- +ptah no_transaction`, or without transactional
DDL, the body runs without one and its record says `no_transaction=true`.

The record keeps at most 2 MiB (2097152 bytes) of a file; a longer file is
refused before anything runs, with an error that names the file, its length
and the limit.

Where the provider also lists `schema.reserved-repeatable-record`, the record
is its own storage: no plan drops it, and a declaration of its name is
refused.

Without `sessions.repeatable-record` for the engine, a run refuses a directory
that holds a repeatable migration before anything changes:

```text
this directory holds repeatable migrations, which this run cannot execute on ydb: unsupported feature: provider github.com/stokaro/ptah-ydb-plugin keeps no record of repeatable migrations for engine ydb, because it does not list sessions.repeatable-record (contract 25.44); install a provider release that lists it
```

## Seal the directory

`ptah migrations hash` covers repeatable migrations, so `ptah.sum` holds an
entry for each. Editing a repeatable migration is the normal way to change it,
and like any other edit it needs a new `ptah.sum`:

```bash
ptah migrations hash --dir ./migrations
```

Without it, the integrity check of `ptah migrations up` refuses the directory
before any migration runs. Regenerating `ptah.sum` does not relax anything for the
versioned files: an applied versioned migration that was edited is still
refused against the checksum its revision row recorded, and no repeatable
migration runs in that run.

## Preview and status

`ptah migrations status` lists each repeatable migration with its state and
digest, and counts a new, changed or unfinished one as a pending change, so
`--exit-code` answers `1` for it:

```text
Repeatable Migrations: 1
  account_names changed h1:Td8a3lsNQ6sxg5EVO97tgeSSgPbOG9E1KnfN3tI0tc0=
```

The states are `pending` (no record), `changed` (the file no longer hashes to
the applied digest), `applied`, `failed`, `running`, and `orphaned` for a
record whose file is no longer in the directory. With `--json`, the status
document carries them under `repeatables`, each with `identity`, `path`,
`checksum`, `applied_checksum`, `state`, `orphaned`, and what the last attempt
recorded. The text output counts the files on the first line and the orphaned
records apart, after them, as the up run reports them; a clean orphaned record
is not a pending change:

```text
Repeatable Migrations: 1
  account_names_v2 applied h1:Td8a3lsNQ6sxg5EVO97tgeSSgPbOG9E1KnfN3tI0tc0=
Repeatable migration records with no file in the directory: 1
  account_names orphaned h1:Td8a3lsNQ6sxg5EVO97tgeSSgPbOG9E1KnfN3tI0tc0=
```

`ptah migrations up --dry-run` selects under the migration lock as a real run
does, prints the repeatable work it would run, and runs and records nothing:

```text
Would have run 1 repeatable migrations:
  account_names changed h1:Td8a3lsNQ6sxg5EVO97tgeSSgPbOG9E1KnfN3tI0tc0=
```

The run document of `--json` carries the selection as `planned_repeatables`,
each entry with `identity`, `path`, `checksum`, `applied_checksum` and
`reason` (`new`, `changed` or `retry`), and names the ones whose digest applied
in `applied_repeatables`.

An approval passed with `--expect-sequence` names the repeatable work it
approves in a `repeatables` list, each entry an `identity` and a `checksum`.
An approval without the list approves none, so a run that would execute a
repeatable migration the caller did not see is refused, the same way a run that
selects an extra version is:

```json
{
  "migrations": [{"version": 2}],
  "repeatables": [{"identity": "account_names", "checksum": "h1:Td8a3lsNQ6sxg5EVO97tgeSSgPbOG9E1KnfN3tI0tc0="}]
}
```

An entry may also bind the record the work runs over with `applied_checksum`:
the digest the caller saw recorded as applied, or an empty string for an
identity with no record yet. The dry run's `planned_repeatables` carries that
value as `applied_checksum` on every entry, `""` included, so the approval
copies it unchanged. Under the lock the run compares it with
the record and refuses the run when another writer applied a different digest
in between, so an approval that says "changed from" a digest holds only over
that digest. Without the field the record is not compared. `null` is refused,
because it could mean either.

## The record table

The record is a table beside the revision table, named after it with
`_repeatable`: `schema_migrations_repeatable` by default, `<table>_repeatable`
under `--migrations-table <table>`, and in the schema `--migrations-schema`
names when one is set. The refusals of `ptah migrations up` and `ptah
migrations down` print the name quoted, the way the engine spells it, so copy
it from there when you write SQL against the table. It holds one row per
identity: the digest that last applied, the state of the last attempt, the
digest that attempt executed, its position in that run, whether it ran in a
transaction, the run identifier, when it started, when content last applied,
the error of a failed attempt, and the bytes of the content that last applied,
in the binary `body` column. The digest and the body are written in the
attempt's own transaction when it runs in one, so the row and the work commit
together.

The record stores each repeatable file verbatim, in the clear, and Ptah puts no
limit on its size. The row stays after the file is deleted, until `ptah
migrations repair --repeatable` removes it. `ptah migrations status` reads the
row, and `ptah migrations up --check-drift` reads the body to replay it. Anyone
who can read the record can read every repeatable migration that applied, so a
repeatable file must not hold secrets.

The first run that has repeatable work creates the table, before its first
versioned migration runs, so a database that refuses the table, for example for
a missing `CREATE` privilege, refuses it before anything changed. A status or a
dry run never creates it. Two refusals come before anything runs, on a dry run
as on an apply: a migrations table name that leaves the record over the
engine's identifier limit (63 bytes on PostgreSQL), and a record table that
another role owns, which is refused before it is read, as the revision table
is. A dry run cannot predict a missing privilege, because it creates nothing.

Schema comparisons and generation leave the table out, as they leave out the
revision table.

The record says what ran, not what the database holds now. A view dropped by
hand, outside a migration, leaves its row `applied`, and the next run does
nothing for it. To run the file again, remove its row, and the next up run
executes it as new. The file is still in the directory, so the removal takes
`--force`:

```bash
ptah migrations repair --db-url "$DATABASE_URL" --migrations-dir ./migrations --repeatable account_names --force
```

Pass the same `--migrations-table` and `--migrations-schema` as the up run, so
repair reads the same record. Any edit of the file works too, since the run
compares digests; a comment line is enough, followed by `ptah migrations
hash`.

## When one fails

A repeatable migration records its attempt before its first statement runs.
What the record says afterwards depends on how far it got:

- The body failed inside its transaction: the state is `failed`, and nothing
  of the body took effect.
- The body failed without a transaction: the state is `failed`, and part of the
  body may have taken effect. Status says `no_transaction=true`.
- The run stopped, lost its connection, or a commit answered with an error: the
  state stays `running`, because nothing can say whether the body took effect.

The run exits non-zero. Versioned migrations that applied before it stay
applied, and the run document reports `partial` when a clean failure follows
them, or `unknown` when the outcome is not known.

While a repeatable migration of the directory is `failed` or `running`, every
up run refuses before it changes anything, the way a dirty revision row stops
it. Fix the cause, then run it again:

```bash
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir ./migrations --allow-dirty
```

`--allow-dirty` runs the repeatable migration again from its first statement.
When it ran without a transaction, check what it left behind before the retry.

A failed or running record whose file is no longer in the directory cannot run
again, so it blocks every up run, `--allow-dirty` included. Check what it left
behind, then remove its row as [below](#remove-or-rename-one) shows. Status
then offers that step rather than the retry.

## Remove or rename one

Deleting a repeatable migration's file does not undo it: the view or function
it created stays, and so does its row, which status lists as `orphaned`. An up
run leaves an orphaned record alone, and every up run logs a warning naming it
until its row is removed. A rollback is refused while the record holds any row,
orphaned rows included, because rolling back under an object the record still
claims leaves a row that says `applied` over a schema without the object.

To remove one for good, drop what it created in a new versioned migration, then
remove its row. No file carries the identity any more, so no `--force` is
needed:

```bash
ptah migrations repair --db-url "$DATABASE_URL" --migrations-dir ./migrations --repeatable account_names
```

Repair holds the migration lock, runs no migration SQL, and refuses an identity
the record has no row for. Without `--force` it also refuses one whose file is
still in the directory, since the next up run would execute that file again.
When the row records an attempt that failed without a transaction, or one with
no outcome, the line that reports the removal says that attempt may have taken
effect, so check what it left behind first. `--repeatable` reads no environment
variable, and a `PTAH_VERSION` or `PTAH_RESUME_FROM` exported for a versioned
repair is set aside when it is given.
The same row can be deleted with SQL, naming the table as the refusal prints it:

```sql
DELETE FROM schema_migrations_repeatable WHERE identity = 'account_names';
```

A rename is a removal and an addition. The next up run executes the file under
its new identity, so its body must run against a database where the old one
already did, and the row of the old identity stays orphaned until it is
removed.

## What a repeatable migration refuses

A repeatable migration is written against the latest schema, so Ptah never runs
the current definition against an older schema, and it keeps no reverse
script. Each refusal below comes before anything changes:

- **A bounded run that stops below the latest version.** `--to-version` or
  `--limit` that would leave a versioned migration unapplied is refused while
  repeatable work is pending, with the work named. Without pending repeatable
  work, a bounded run behaves as before.
- **A rollback.** `ptah migrations down` refuses a database whose record holds
  any row, including the row of a file that was deleted or renamed. Drop what
  the repeatable migrations created, delete their rows from the record table,
  and then roll back, or roll forward with a new migration instead.
- **A drift check that cannot know what a repeatable migration left.**
  `--check-drift` replays the applied versioned history on the shadow
  database, and then each repeatable migration with the content in effect:
  the file when it hashes to the digest recorded as applied, and the body the
  record keeps when the file changed or is gone. A transactional attempt that
  failed or did not finish rolled back, so the content that applied before it
  is replayed. The check exits with status 2 and the run applies nothing when
  an attempt that ran outside a transaction did not finish, or when the record
  keeps no body that hashes to the digest it applied. Run without
  `--check-drift` to apply the change.
- **A runner that opens its own transactions.** Under
  `migration.runner: golang-migrate` a repeatable migration would run in a
  transaction Ptah opens, so the run is refused.
- **A provider that keeps no record.** A provider that does not list
  `sessions.repeatable-record` for the engine; see
  [Through a provider](#through-a-provider).
- **A file that asks for a transaction** with `-- atlas:txmode file` on an
  engine that does not roll schema changes back as a unit.

## Importing from Flyway

Flyway's `R__<description>.sql` is a repeatable migration. `ptah migrations
import --repeatables on-change` writes it as a Ptah repeatable migration with
the same body and no down file. On an engine that does not run repeatable
migrations, `ptah migrations up` refuses the directory, so import with the
default there. The default, `--repeatables once`, writes it as a
one-time migration ordered after the versioned ones, which does not run again
when its body changes. See [Import from another tool](../import/).

## The compatibility surface

By default `ptah-compat` keeps the Atlas reading: `R__view.sql` is the version
`R`, it runs once, and an edit of it after it applied changes nothing. Because
`R` sorts after every number, a versioned file added after it is refused as
added out of order, on `ptah-compat` and on the community binary alike.

Set `PTAH_ATLAS_REPEATABLE_MIGRATIONS=1` and `ptah-compat migrate apply` and
`ptah-compat migrate status` read a top-level `R__<identity>.sql` of a
directory in the Atlas layout as a repeatable migration, with everything this
page describes: it runs after the versioned files, again when its content
changes, and is refused where a native run refuses it. The other files keep
their Atlas reading, `1R_name.sql` included. A directory in another layout,
such as `?format=flyway`, is converted before it runs and is not affected.

```bash
PTAH_ATLAS_REPEATABLE_MIGRATIONS=1 ptah-compat migrate apply --url "$DATABASE_URL" --dir file://migrations
```

The variable does not change `atlas.sum`. The community binary hashes every
top-level `.sql` file, `R__` files included, and `ptah-compat migrate hash`
writes the same file whatever the variable says, so `migrate validate` agrees
with the community binary. Editing a repeatable migration needs `migrate hash`,
as editing any file does.

The revision table records the versioned files only. The record is
`atlas_schema_revisions_repeatable`, beside `atlas_schema_revisions` and in the
same schema (`atlas_schema_revisions` on PostgreSQL unless `--revisions-schema`
moves it). Native `ptah migrations up --revision-format atlas` keeps its record
in the same place, `<revision table>_repeatable`.

Both reports keep the Atlas-shaped part as the community binary prints it, and
add the repeatable work after it, only when there is some:

```text
No migration files to execute

Repeatable Migrations: 1
  view changed h1:mtlKrBRtAni0Nb+vm02kPCGLe/temD10FQ2BpKKS0OY= applied
```

`migrate status` lists each repeatable migration with its state, as
`ptah migrations status` does, and a new or changed one makes the status
`PENDING`. Under `--format '{{ json . }}'` both documents carry the list under
`Repeatables`, after every key the community binary writes, and leave the key
out when the list is empty.

A database where the Atlas reading already applied the file holds the revision
row `R`. Read as a repeatable migration, the file has no record there and would
run again, so `migrate apply` and `migrate status` refuse the database before
anything runs and name the revision table as the database resolves it. Delete
the row, check that the body runs again against the database, and apply. On
PostgreSQL the table is in the schema `atlas_schema_revisions` unless
`--revisions-schema` or a `search_path` in the URL moves it:

```sql
DELETE FROM atlas_schema_revisions.atlas_schema_revisions WHERE version = 'R';
```

On MySQL, MariaDB and SQLite the table is in the database the URL connects to,
and the bare name reaches it:

```sql
DELETE FROM atlas_schema_revisions WHERE version = 'R';
```

On any other target, copy the name from the refusal.

The opposite direction is refused too. With the variable unset, the Atlas
reading would run each recorded `R__` file once more as the version `R`, so
`migrate apply` and `migrate status` refuse a database whose record holds a row
and say to set the variable. A database with no record costs one catalog lookup
and reports as it does without this feature.

`migrate down` reads an `R__` file as the version `R` either way. While the
variable is set it refuses a directory that holds one, before connecting, and
with or without the variable it refuses a database whose record holds a row, as
`ptah migrations down` does. A directory with no `R__` file over a database with
no record rolls back with the variable set. `migrate diff` and `migrate lint` keep the
Atlas reading and analyze the file as the version `R`; when the variable is set
and the directory holds an `R__` file, each prints one line on stderr that says
so. `PTAH_ATLAS_STRICT_COMPAT=1` refuses the variable when it is enabled,
because the community binary has no re-run-on-change semantics.

## Limits

- Through a provider, the record keeps at most 2 MiB of a file, and a
  provider release that does not list `sessions.repeatable-record` refuses the
  directory.
- On ClickHouse nothing decides between two concurrent runs, and a file whose
  record would exceed `max_query_size` is refused.
- `ptah migrations repair --repeatable` removes a row; nothing marks a
  repeatable migration applied without running it. Removing the row of a file
  still in the directory makes the next run execute it as new.

stokaro/ptah#6153 records these.
