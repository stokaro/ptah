# PostgreSQL

Start with `internal/builtindialects/postgres.go`. Catalog code lives in
`internal/dbschema/postgres`, planning in `internal/planner/dialects/postgres`,
and migration services in `internal/migratorext/postgres`. Selected schema
extensions live under `dialect/postgres`.

- Preserve schema-qualified, quoted, and overloaded identities. Check the
  connection's `search_path` and role when catalog lookup differs from direct
  SQL. Do not assume that an unqualified object belongs to `public`.
- Trace sequence ownership, defaults, extension members, and dependencies
  before planning replacement or removal. A constraint and its backing index
  are related objects, not interchangeable declarations.
- Keep catalog visibility and inspection coverage explicit. An object hidden
  by privileges or an unsupported catalog shape is not a deletion request.
- Check transaction eligibility per statement. `CREATE INDEX CONCURRENTLY`
  cannot run inside a transaction block. A failed concurrent build can leave
  an invalid index; preserve the existing migration index guard and recovery
  contract instead of accepting its name as proof of success.
- Preserve key order, expressions, predicates, included columns, operator
  classes, and null-distinctness where the selected version supports them.
  Do not propagate a PostgreSQL capability to CockroachDB or YugabyteDB solely
  because they use the same driver.

Use the affected owner's unit tests and `integration/dbschema/postgres` for
catalog round trips. For migration changes, inspect the concurrent-index and
transaction tests under `integration/` and `migration/migrator`. Assert the
readback state and repeat-plan result, not only the emitted SQL.

Sources: PostgreSQL 18 [schema lookup](https://www.postgresql.org/docs/18/ddl-schemas.html),
[index creation](https://www.postgresql.org/docs/18/sql-createindex.html), and
[dependency tracking](https://www.postgresql.org/docs/18/ddl-depend.html).
Use the corresponding manual for the tested server version.
