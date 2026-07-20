# SQLite Support

Ptah supports SQLite as the `sqlite` dialect, with `sqlite3` accepted as an
alias. The implementation uses the pure-Go `modernc.org/sqlite` driver, so it
does not require CGO.

## URLs

`dbschema.ConnectToDatabase` accepts these SQLite URL forms:

```bash
sqlite://relative.db
sqlite:///absolute/path/app.db
sqlite:///:memory:
sqlite:file:memdb1?mode=memory&cache=shared
```

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

## ALTER TABLE Limits

SQLite cannot add, drop, or modify table constraints in place, and many column
shape changes require rebuilding the table. Ptah currently reports explicit
errors for these cases instead of emitting unsafe or partial SQL:

- dropping columns;
- modifying column type, nullability, default, primary key, unique, or generated
  column shape;
- adding or removing table constraints on existing tables;
- changing enum-backed `CHECK` constraints;
- PostgreSQL-only objects such as extensions, materialized views, row-level
  security, roles, grants, and `EXCLUDE` constraints.

Table rebuild planning is intentionally left as a separate feature. Until that
exists, SQLite migrations should model rebuild-only changes manually.

`ALTER TABLE ... ADD COLUMN` has narrower SQLite rules than `CREATE TABLE`.
Ptah only emits native add-column migrations for shapes SQLite can apply in
place. Adding a primary key, unique column, `AUTOINCREMENT` column, `NOT NULL`
column without a non-NULL literal default, foreign-key column with a non-NULL
default, expression default, parenthesized default, `CURRENT_TIME`,
`CURRENT_DATE`, `CURRENT_TIMESTAMP`, or a `STORED` generated column is reported
as rebuild-required.
