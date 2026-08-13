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

Virtual tables are read as virtual tables. `ptah db read` and
`ptah-compat schema inspect` emit the statement that created one:

```sql
CREATE VIRTUAL TABLE "docs" USING fts5(title, body);
```

The module name and everything between its parentheses are carried verbatim, so
tokenizer options, quoted values, and commas inside quoted arguments survive.
Applying that output to an empty database recreates the same object.

The reader never names a module. It asks `PRAGMA table_list`, which classifies
every table as `table`, `virtual`, or `shadow`, so `fts3`, `fts4`, `fts5`,
`rtree`, `rtree_i32`, `geopoly`, `fts5vocab`, `dbstat` and any module a build
registers are all handled the same way. The SQLite build Ptah links reports its
own registered modules through `PRAGMA module_list`.

The shadow tables a module maintains — `docs_data`, `docs_idx`, `docs_config`
and the rest — are not reported. They are the module's bookkeeping, and an
operator who applied a `CREATE TABLE` for one would create a table SQLite
creates itself, which then collides when the virtual table is created. The
suppression asks SQLite rather than matching names: a table called `docs_data`
that the operator created is reported as the user table it is, next to an FTS5
index called `docs` whose own `docs_data` is not.

One limit is worth knowing about the read: only the module can say which
suffixes are its own, so shadow tables belonging to a module the reading build
does not register — an `fts4` index in a database written elsewhere, for
example — cannot be identified, and SQLite reports them as ordinary tables.
The virtual table itself is still recognized as virtual and still round-trips,
because that classification does not need the module.

## Virtual Tables in a Comparison

No desired-state source declares a virtual table. Go annotations, HCL, YAML and
`.sql` schema files have no syntax for one, and the native SQL schema parser
says so: feeding it `ptah db read` output for a database holding a virtual
table fails with `unsupported CREATE target: VIRTUAL`.

That leaves a comparison two ways to be wrong, and Ptah refuses both rather
than planning them:

- **The desired state does not name it.** Read as intent, that plans
  `DROP TABLE "docs"`, which deletes the index and everything in it. The desired
  state could not have asked for the table to be kept, so the removal is refused
  and named instead.
- **The desired state names it**, which it can only do as an ordinary table.
  The two are different kinds of object, the planner cannot convert one into the
  other, and `ALTER TABLE ... ADD COLUMN` is not something SQLite accepts on a
  virtual table. The collision is refused rather than reported as no difference.

Both refusals name the table and its module. Every verb that compares a live
database is covered: `ptah schema apply`, `diff`, `compare`, `plan` and `drift`,
and `ptah-compat schema diff`, `schema apply` and `migrate diff`. Reading is
never affected — `ptah db read` and `ptah-compat schema inspect` compare
nothing.

To proceed, say which one you meant:

- **To keep the table**, exclude it from the comparison with `--exclude docs`.
  Both sides then ignore it and the rest of the schema converges normally.
- **To drop it**, set `PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP=1`. The removal is
  planned exactly as before, including the `DROP TABLE` that destroys the index
  contents and the module's shadow tables. The opt-in covers only the removal:
  a desired ordinary table colliding with a live virtual one stays refused
  however it is set, because no value of it makes the planner able to convert
  one kind into the other.

An unset variable and an explicit false both keep the refusal; a value that is
not a boolean is a configuration error rather than a silent refusal.

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
