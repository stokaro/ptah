# SQLite Support

Ptah supports SQLite as the `sqlite` dialect, with `sqlite3` accepted as an
alias. The implementation uses the pure-Go `modernc.org/sqlite` driver, so it
does not require CGO.

## URLs

`dbschema.ConnectToDatabase` accepts these SQLite URL forms:

```bash
sqlite://relative.db
sqlite:///absolute/path/app.db
sqlite:file:C:/absolute/windows/path/app.db
sqlite:///:memory:
sqlite:file:memdb1?mode=memory&cache=shared
```

Use forward slashes in the Windows file URI. The `file:` portion keeps the
drive-letter colon out of the URL host and lets reserved filename characters
be percent-encoded without changing the selected file.

Ptah adds `_pragma=foreign_keys(1)` unless the URL already supplies a
`foreign_keys` pragma. SQLite connections are limited to one open connection so
in-memory databases and connection-local PRAGMA state behave predictably under
`database/sql`.

## Generated SQL

The SQLite renderer and planner support:

- `CREATE TABLE`, including `STRICT` and `WITHOUT ROWID` table options.
- Inline `PRIMARY KEY`, `UNIQUE`, `CHECK`, and `FOREIGN KEY` constraints.
- Enum annotations as `TEXT` columns plus `CHECK (<column> IN (...))`.
- `CREATE INDEX`, including unique and partial indexes.
- `DROP INDEX IF EXISTS` and `DROP TABLE IF EXISTS`.
- `ALTER TABLE ... ADD COLUMN` for SQLite-native column additions, plus
  `RENAME COLUMN` and `RENAME TO`.
- Simple column-drop plans through a table rebuild: create a temporary table
  from the retained schema, copy retained columns, drop the original table,
  rename the rebuilt table, and recreate retained indexes/triggers when their
  metadata can be round-tripped safely.
- Views without `WITH CHECK OPTION`.
- Row-level triggers; SQLite does not support statement-level triggers.

## Introspection

The SQLite reader uses `sqlite_schema` and SQLite PRAGMA metadata. It reads
catalog metadata in fixed batches instead of issuing one query per table or
index:

- `sqlite_schema` for table, index, view, and trigger definitions.
- `pragma_table_xinfo(...)` for table columns, primary-key membership,
  defaults, and generated-column kind.
- `pragma_index_list(...)` plus `pragma_index_xinfo(...)` for indexes, unique
  constraints, partial indexes, and expression indexes.
- `pragma_foreign_key_list(...)` for foreign keys.
- `sqlite_schema.sql` for generated-column expressions, named `CHECK`
  constraints, named foreign keys, view bodies, trigger headers, and trigger
  bodies.

System objects whose names start with `sqlite_` and Ptah's `schema_migrations`
table are ignored.

## Virtual Tables

Virtual tables are read as virtual tables. `ptah db read`, and
`ptah-compat schema inspect --format '{{ sql . }}'`, emit the statement that
created one:

```bash
ptah db read --db-url "sqlite://app.db"
ptah-compat schema inspect --url "sqlite://app.db" --format '{{ sql . }}'
```

```sql
CREATE VIRTUAL TABLE "docs" USING fts5(title, body);
```

The `--format` is not optional on the compatibility surface: `schema inspect`
defaults to HCL there, as the community CLI does, and neither HCL nor JSON has
a virtual-table construct. A virtual table renders as
`table "docs" { schema = schema.main }` with no columns — which is what the
pinned community binary emits for the same object, and which does not replay.

Ptah does not change that document, because matching the community binary is
what the surface is for. It says so instead:

- **Ordinarily**, a note on standard error names each virtual table whose
  module declaration the rendering dropped and points at
  `--format '{{ sql . }}'`. Standard output and the exit code are untouched, so
  a pipeline that captures the document keeps working and its operator learns
  what is missing.
- **Under `PTAH_ATLAS_STRICT_COMPAT=1`**, the same condition is refused. Strict
  mode owns the process output contract and will not hand a pipeline a document
  that looks complete and is not. `--format '{{ sql . }}'` is unaffected, since
  the refusal only fires when the declaration is actually absent from the
  rendered text.

The module name and everything between its parentheses are carried verbatim, so
tokenizer options, quoted values, and commas inside quoted arguments survive.
Applying that output to an empty database recreates the same object.

The reader never names a module. It asks `PRAGMA table_list`, which classifies
every table as `table`, `virtual`, or `shadow`, so `fts5`, `rtree`,
`rtree_i32`, `geopoly`, `fts5vocab`, `dbstat` and any module a build registers
are all handled the same way.

Which modules a build registers is not assumed. The reader asks
`PRAGMA module_list` on the same connection, and the SQLite build Ptah links
answers with exactly seven:

```text
dbstat  fts5  fts5vocab  geopoly  rtree  rtree_i32  sqlite_dbpage
```

`fts3` and `fts4` are not among them, and that gap has consequences the next
two sections describe. Ask this build yourself with any SQLite client that can
reach it — `PRAGMA module_list` is the same query — but note that
`SELECT name FROM pragma_module_list` answers differently: the table-valued
form registers itself as a module and reports itself, and keeps doing so for
the rest of that connection.

The shadow tables a module maintains — `docs_data`, `docs_idx`, `docs_config`
and the rest — are not reported. They are the module's bookkeeping, and an
operator who applied a `CREATE TABLE` for one would create a table SQLite
creates itself, which then collides when the virtual table is created. The
suppression asks SQLite rather than matching names: a table called `docs_data`
that the operator created is reported as the user table it is, next to an FTS5
index called `docs` whose own `docs_data` is not.

An explicit user-created index on one of those shadow tables is refused during
the read. Ptah cannot expose the module-owned table as an ordinary desired
object, and omitting the index would claim a complete schema that cannot be
replayed. SQLite's own autoindexes have no stored `CREATE INDEX` statement and
do not trigger this refusal.

### A Module This Build Does Not Register

Only the module can say which suffixes are its own, so the shadow tables of a
module the reading build does not register cannot be identified. SQLite reports
them as ordinary tables. The virtual table itself is still recognized as
virtual and still round-trips, because that classification does not need the
module — SQLite records it while parsing the schema, before anything is
resolved.

Measured on an `fts4` database written by a build that has the module, read
through the driver Ptah links:

| table           | with `fts4` registered | without it |
| --------------- | ---------------------- | ---------- |
| `docs`          | `virtual`              | `virtual`  |
| `docs_content`  | `shadow`               | `table`    |
| `docs_docsize`  | `shadow`               | `table`    |
| `docs_segdir`   | `shadow`               | `table`    |
| `docs_segments` | `shadow`               | `table`    |
| `docs_stat`     | `shadow`               | `table`    |

Those five rows are an FTS4 index. Ptah cannot tell them from user tables, so
the description it produces is wrong in a way it can name but not repair, and
the read says so on standard error:

```text
note: virtual table "docs" (module fts4) uses a module this build does not
register, so SQLite could not mark the tables that fts4 owns and this
description reports them as ordinary tables.
```

The exit code and standard output are untouched, so a pipeline that captures
the document keeps working.

The note describes the document that was rendered, so selection changes what it
says. Where the virtual table survived the projection it is named, as above.
Where a projection dropped it but the description is not empty — `--exclude
docs` leaves the module's storage behind as ordinary `CREATE TABLE` statements —
the note keeps the warning and drops the name, because naming a table the
document does not contain sends the reader looking for a statement that is not
there:

```text
note: this description was narrowed, and the database it came from uses module
fts4 this build does not register. SQLite could not mark the tables it owns, so
Ptah cannot tell whether any of the ordinary tables below are the module's
private storage.
```

An empty description says nothing at all. What a comparison does with such a
database is the subject of the next section, and it is not a note.

## Virtual Tables in a Comparison

The desired side of a comparison comes from one of two places, and the
difference decides what happens.

A desired state can name a virtual table two ways. A native `.sql` schema file
may declare `CREATE VIRTUAL TABLE … USING …`, so `ptah db read` output is
readable back by the tool that wrote it; and `schema diff` accepts a **database
URL** as its desired side, read by the same reader that produced it. Go
annotations, HCL and YAML have no syntax for one.

The module arguments are preserved verbatim — the text between the module's
parentheses, quotes and embedded commas included — because SQLite stores and
compares them as written.

Each name that either side calls a virtual table is classified, and only the
answers a plan cannot express are refused:

| desired side | database side | outcome |
| --- | --- | --- |
| does not name it | virtual | **refused** — a document could not have asked for it to be kept, so the silence is not a request to drop it |
| ordinary table | virtual | **refused** — two kinds of object under one name |
| virtual | ordinary table | **refused** — the same collision, mirrored |
| virtual, different module or arguments | virtual | **refused** — SQLite has no `ALTER VIRTUAL TABLE`, so converging means dropping and recreating, which destroys the index |
| virtual, same declaration | virtual | synced, nothing to do |
| virtual | absent | planned as `CREATE VIRTUAL TABLE` |

Declarations are compared with the module name folded the way SQLite folds an
identifier, and the module arguments compared verbatim: they are not SQL, only
the module interprets them, and normalizing whitespace would equate
`tokenize = 'a  b'` with `tokenize = 'a b'`, which are two different tokenizers.
Catalog identifier bytes are preserved too. A quoted table named `" docs "`
remains distinct from `docs`, and an authorized removal names only the former.

Every refusal names the table and its module. Every verb that compares a live
database is covered: `ptah schema apply`, `diff`, `compare`, `plan` and `drift`,
and `ptah-compat schema diff`, `schema apply` and `migrate diff`. Reading is
never affected — `ptah db read` and `ptah-compat schema inspect` compare
nothing.

To proceed, say which one you meant:

- **To keep the table**, exclude it from the comparison with `--exclude docs`.
  Both sides then ignore it and the rest of the schema converges normally. This
  is only offered when the module is registered; see the next section for why.
- **To drop it**, set `PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP=1`. The removal is
  planned exactly as before, including the `DROP TABLE` that destroys the index
  contents and the module's shadow tables. The opt-in covers only that first
  row of the table above: a kind collision and a changed declaration stay
  refused however it is set, because no value of it makes the planner able to
  convert one object into another.

An unset variable and an explicit false both keep the refusal. A value that is
not a boolean is a configuration error. An explicit SQLite URL or dialect is
validated before project configuration and command early returns. A SQLite URL
selected by project configuration is validated immediately after the effective
configuration merge and before source loading, path resolution, database, or
SQL work. The public migration generator has no project-config merge and
validates before resolving filesystem paths.
`migrations checkpoint --dialect sqlite` validates before requiring the shadow
URL, then validates the URL-derived dialect again when the URL exists.
Non-SQLite operations do not consult the variable.

### Comparing a Database Ptah Cannot Classify

Everything above assumes the description is right about which tables are
tables. Where the module is not registered it is not, and `--exclude` makes it
worse rather than better.

This was measured end to end on `fts3` and `fts4` databases. Ptah refused the
comparison and advised excluding the virtual table; that exact command then
planned and executed a `DROP TABLE` for each of the module's storage tables at
exit 0, after which `MATCH` returned `SQL logic error` instead of a row. The
`fts5` control, run identically, reported `Schema is synced, no changes to be
made.` and left the index untouched. The exclusion removed `docs` from the
comparison and left `docs_content`, `docs_segdir` and their siblings in it,
where a desired state that does not name them reads as a request to drop them.

So a comparison whose database side holds a virtual table this build cannot
load is refused outright, and the refusal survives excluding that table:

```text
unsupported feature: the database holds virtual table "docs" (module fts4)
whose module this build of Ptah does not register; ... excluding "docs" does
not protect the index, because the tables at risk are the module's own storage
rather than it, and Ptah cannot list them without the module; this build
registers dbstat, fts5, fts5vocab, geopoly, rtree, rtree_i32, sqlite_dbpage
```

There is no safe exclusion to suggest, because naming the tables to exclude
requires the module that is missing. Ptah says what it cannot determine instead
of advising something that destroys data.

The refusal fires only when the plan can actually act on such a table, and that
question is asked in three places because only one part of it is answerable
before the comparison runs, and a generated migration is two files rather than
one:

- **Before comparing**, when some live table is one the desired side does not
  name. That is exactly the comparator's removal set, so no second copy of its
  rules is needed to know it.
- **After comparing**, when the diff removes or rebuilds any table. A table both
  sides name can still differ in a column's type, nullability, default,
  generated expression, or a table constraint, and every one of those makes the
  SQLite planner rebuild it — drop, recreate, copy — which destroys a module's
  storage as surely as a drop. Whether that will happen is the comparator's
  answer, so it is read from the diff rather than guessed at beforehand.
- **When a rollback is planned beside the migration**, on the reversed diff.
  Reversal turns changes SQLite performs in place into changes it does not: an
  added column comes back as a removed one, which SQLite converges by rebuilding
  the table. The up file can therefore be a single
  `ALTER TABLE ... ADD COLUMN` — which the check above admits, correctly — while
  the down file written beside it drops and recreates the module's storage. Only
  `ptah migrations generate` and the `migration/generator` planning API produce a
  rollback; `ptah schema diff` and `ptah schema apply` have no down direction and
  are unaffected.

Two consequences worth knowing:

- `--include users` narrows the comparison to one table the desired side names.
  Nothing is droppable, so the comparison runs — measured as
  `Schemas are synced, no changes to be made.` at exit 0, with no opt-in.
- Two databases that both hold the same `fts4` index compare normally for the
  same reason, and need no opt-in either.

`--exclude docs` is the opposite case and stays refused, because it leaves the
module's storage tables in the comparison with nothing naming them.

### A project that skips destructive changes

Both refusals are claims about a statement, so a project that configures the
statement away is not refused for it. With `diff.skip: [drop_table]` in
`ptah.yaml`, or `diff { skip { drop_table = true } }` in an Atlas project file,
every table drop — and the dependent index, constraint, trigger, RLS and grant
removals a dropped table would carry — is deleted from the diff before any SQL
is rendered, so nothing the guard is warning about can happen.

`drop_column` and `drop_index` are read the same way. A removed column makes the
SQLite planner rebuild the table, and a removed index is a `DROP INDEX` against
a table Ptah cannot classify; a project that skips either one deletes that
statement again, so neither is counted. `drop_enum` is not read at all, because
SQLite has no enum type.

Measured on an `fts4` database built by a system SQLite that has the module,
with a desired state naming only the ordinary table:

| run | result |
| --- | --- |
| `ptah schema apply`, no project policy | refused, exit 2 |
| the same run with `diff.skip: [drop_table]` | `Schema is synced, no changes to be made.`, exit 0, `MATCH` still `1` |

This is narrow on purpose. `skip drop_table` filters removals, not
modifications, so a desired state that NAMES one of the module's storage tables
and describes it differently is still refused — the SQLite planner rebuilds
that table, and a rebuild is drop, recreate, copy. Measured on the same
database, with the policy set:

```text
unsupported feature: the plan changes "docs_content" in a database that holds
virtual table "docs" (module fts4) ...
```

The same applies to the plain virtual-table refusal above: with the policy set,
a live `fts5` index the desired state does not name is no longer refused,
because the `DROP TABLE` it warns about is never rendered.

The post-comparison refusal names the tables the plan would change:

```text
unsupported feature: the plan changes "docs_content" in a database that holds
virtual table "docs" (module fts4) whose module this build of Ptah does not
register; ... dropping or rebuilding one of them destroys the index it belongs
to, while dropping or replacing an index or trigger one of them carries removes
machinery the module may be the one maintaining; ...
```

It counts every table the SQLite planner drops or rebuilds, which is more than
the obvious ones: a table whose columns are unchanged and whose **constraint**
changed is recorded only at schema level, and since SQLite has no `ALTER` for a
constraint, that table is rebuilt too.

It also counts a table an **index or trigger is removed from or replaced on**,
even when the table itself is neither dropped nor rebuilt. `DROP INDEX` and
`DROP TRIGGER` reach the plan on their own, and Ptah can no more tell a module's
own index from an operator's than it can tell the module's storage from an
ordinary table. Measured on an `fts4` database this build cannot load, with both
sides naming the module's storage: `ptah schema diff` planned
`DROP INDEX IF EXISTS "docs_content_title_idx";` at exit 0 before this was
counted, and the trigger fixture planned
`DROP TRIGGER IF EXISTS "docs_content_guard";` the same way.

Additions are not counted. A table the plan CREATES cannot be one the module
already owns, so adding a table — with or without a constraint — beside an index
Ptah cannot classify stays ordinary work, and a table that only GAINS an index
or a trigger is not counted either: a `CREATE` removes nothing.

**Adding a column is not counted either.** `ALTER TABLE t ADD COLUMN c` is a
statement SQLite has, so the planner emits it in place and drops or rebuilds
nothing — and this refusal is about a drop or a rebuild. A narrowed comparison
such as `ptah schema diff --include users` against a database holding an `fts4`
index therefore runs at exit 0 and prints its one
`ALTER TABLE "users" ADD COLUMN "email" TEXT;`, with no opt-in. A table diff that
also removes or changes a column, or carries a constraint change, is a rebuild
and is still refused.

**The rollback of an added column is counted, though.** A migration is generated
in both directions, and reversing an added column produces a removed one, which
SQLite converges by rebuilding the table. So a *generated migration* whose up
file is that one `ALTER TABLE ... ADD COLUMN` is refused when its down file would
rebuild a table in a database holding a module this build cannot load:

```text
unsupported feature: the rollback generated beside this migration changes
"docs_content" in a database that holds virtual table "docs" (module fts4) whose
module this build of Ptah does not register; the forward statements are ones
SQLite performs in place, but reversing them for the down file turns an added
column into a removed one, which SQLite has no ALTER for and converges by
rebuilding the table; ...
```

The rollback check discounts what the migration itself creates: a table, index or
trigger the up file creates cannot be storage the module already owns, so the
down file dropping it again removes nothing that was there. Measured on an `fts4`
database this build cannot load, adding an ordinary `audit` table still generates
both files at exit 0 with `DROP TABLE IF EXISTS "audit";` as the rollback. An
index or trigger the up file *replaces* rather than creates stays counted.

The residue that leaves is stated rather than hidden, and it is not destruction:
a desired state that explicitly **names** one of the module's storage tables with
an extra column plans an `ALTER TABLE ... ADD COLUMN` against it. Measured on a
live `fts4` index, that leaves every row in place and `MATCH` still answering,
and refuses only further writes (`INSERT` reports `SQL logic error`) until the
added column is dropped again, after which they resume. A rebuild is
drop-recreate-copy and takes the index with it for good, which is why the two are
treated differently.

Two ways forward:

- **Read the database with a build that registers the module.** Every
  classification then comes from SQLite and the ordinary rules above apply.
- **Set `PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE=1`** to compare the
  module's storage as the ordinary tables it appears to be, accepting the drops
  that follow. This restores what Ptah did before the refusal existed. It is a
  separate variable from `PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP` because the two
  answer different questions — that one permits dropping a virtual table Ptah
  can see, this one permits planning against tables Ptah has said it cannot
  vouch for — and neither implies the other.

**Adding** such a table is refused with no opt-in at all. A plan carrying
`CREATE VIRTUAL TABLE ... USING fts4` fails on this build with
`no such module: fts4`, which previously happened after the plan had been
printed, approved, and started. No value of an environment variable makes a
module exist, so the refusal comes before the plan:

```text
unsupported feature: the desired schema adds virtual table "docs" (module fts4)
whose module this build of Ptah does not register; creating it means the
statement `CREATE VIRTUAL TABLE ... USING fts4`, which this build answers with
`no such module: fts4` ...
```

This fires only where a `CREATE VIRTUAL TABLE` would actually be planned — the
name is virtual on the desired side and absent from the database. Two databases
that both already hold the same `fts4` index plan no such statement, so with
`PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE=1` they compare normally and
report `Schemas are synced, no changes to be made.` Refusing them would have
claimed a mid-apply failure that cannot happen and left the opt-in unable to
restore the comparison it promises.

## ALTER TABLE Limits

SQLite cannot add, drop, or modify table constraints in place, and many column
shape changes require rebuilding the table. Ptah emits a rebuild plan for simple
column drops, including the down migration generated for SQLite add-column
changes. Ptah still reports explicit errors instead of emitting unsafe or
partial SQL for unsupported rebuild shapes:

- combining dropped columns with other table changes in the same diff;
- dropping columns from tables referenced by inbound foreign keys;
- dropping columns when the internal rebuild table name would collide with an
  existing table;
- dropping columns from tables whose retained triggers use SQLite syntax Ptah
  cannot round-trip yet, such as `UPDATE OF` trigger columns;
- modifying column type, nullability, default, primary key, unique, or generated
  column shape;
- adding or removing table constraints on existing tables;
- changing enum-backed `CHECK` constraints;
- PostgreSQL-only objects such as extensions, materialized views, row-level
  security, roles, grants, and `EXCLUDE` constraints.

Broader table rebuild planning remains intentionally conservative. SQLite
migrations should still model complex rebuild-only changes manually.

`ALTER TABLE ... ADD COLUMN` has narrower SQLite rules than `CREATE TABLE`.
Ptah only emits native add-column migrations for shapes SQLite can apply in
place. Adding a primary key, unique column, `AUTOINCREMENT` column, `NOT NULL`
column without a non-NULL literal default, foreign-key column with a non-NULL
default, expression default, parenthesized default, `CURRENT_TIME`,
`CURRENT_DATE`, `CURRENT_TIMESTAMP`, or a `STORED` generated column is reported
as rebuild-required.
