# SQL Server Support

Ptah supports an initial portable SQL Server and Azure SQL subset under the
canonical dialect name `sqlserver`. The aliases `mssql`, `sql-server`,
`sql_server`, and `tsql` normalize to the same dialect.

## Connection URLs

Ptah uses Microsoft's `github.com/microsoft/go-mssqldb` database/sql driver and
passes canonical `sqlserver://` URLs to it:

```bash
ptah db read --db-url 'sqlserver://sa:pass@localhost:1433?database=app&encrypt=disable'
```

`mssql://...` input URLs are accepted and normalized to `sqlserver://...`.

The optional Ptah-only `schema` query parameter selects the default schema for
introspection, migration metadata, and write helpers:

```bash
ptah db read --db-url 'sqlserver://sa:pass@localhost:1433?database=app&schema=audit&encrypt=disable'
```

Ptah removes `schema` before handing the URL to the driver. If omitted, the
default schema is `dbo`. `--migrations-schema` / `WithMigrationsTable(schema,
table)` still takes precedence for migration metadata.

## Supported Surface

The current SQL Server implementation covers:

- T-SQL rendering with bracket-quoted identifiers.
- Schema-qualified object names such as `dbo.users`.
- `IDENTITY(start,increment)` for auto-increment columns.
- `NVARCHAR`/`NVARCHAR(MAX)` string mapping.
- Core table DDL, primary keys, unique constraints, foreign keys, CHECK
  constraints, and basic indexes.
- Rendering for views and triggers when definitions are supplied as raw SQL.
- Live schema introspection from `sys.tables`, `sys.columns`,
  `sys.foreign_keys`, `sys.indexes`, and related catalog views.
- Transactional migration apply for DDL supported by SQL Server.

Enums are represented as `NVARCHAR(255)` plus generated CHECK constraints.
SQL Server does not have a native enum object equivalent to PostgreSQL
`CREATE TYPE ... AS ENUM`.

## Limitations

The SQL Server support is deliberately conservative:

- No PostgreSQL-style extensions, row-level security policies, roles/grants, or
  materialized views.
- Column drift planning supports direct `ALTER COLUMN` only for type and
  nullability changes. Default, generated expression, unique, and CHECK changes
  require explicit constraint-aware planning before they can be emitted safely.
- Automatic column removal is rejected because SQL Server requires dependent
  constraints, defaults, and indexes to be dropped in the correct order first.
- SQL Server metadata repair/baseline paths use SQL Server-compatible revision
  table SQL, but Ptah does not yet expose a general DML/upsert AST. User-schema
  `MERGE` generation is tracked separately in
  [#457](https://github.com/stokaro/ptah/issues/457) and is outside the current
  DDL renderer surface.
- View and trigger introspection records SQL Server's persisted definition text,
  but Ptah does not yet normalize it into body-only drift-safe definitions.
- Index introspection covers key columns but does not yet preserve included
  columns or descending key order for drift-safe round trips.
- `DROP INDEX IF EXISTS` and `DROP CONSTRAINT IF EXISTS` are not used in the
  portable preset; Ptah relies on scoped, deterministic object ownership.
- Schema-scoped `DropAllTables` removes tables and foreign keys owned by the
  selected schema only. If another schema has a foreign key referencing a
  selected table, cleanup fails with a blocking-constraint error instead of
  mutating objects outside the selected schema.
- SQL Server-specific options such as `WITH (ONLINE = ON)` and
  `NOT FOR REPLICATION` are not planned yet.

## Live Tests

The live SQL Server introspection test is opt-in:

```bash
PTAH_SQLSERVER_TEST_URL='sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable' \
  go test ./dbschema ./migration/migrator -run 'TestSQLServerLive(ReadSchema|DropAllTablesDropsForeignKeys|ComputedColumnZeroDiff|DropAllTablesRejectsExternalForeignKeys)|TestSQLServerMigratorHonorsURLSchemaForMetadata|TestMigrationAdvisoryLock_SQLServer(DefaultTimeout|Timeout)Integration'
```

The test creates a temporary schema, tables with `IDENTITY`, a reserved-word
table name, computed columns, CHECK/UNIQUE/FK constraints, and an index, then
verifies that Ptah can introspect them through the SQL Server reader. The live
coverage also verifies schema-scoped cleanup of FK-linked tables and migration
metadata placement through the URL `schema` parameter. Cleanup fails safely when
another schema owns a foreign key into the selected schema, and computed column
and default catalog readback are compared as a zero-diff schema. SQL Server
advisory-lock coverage verifies both normal migration progress and timeout
reporting when another session holds `ptah_migrate`.

The integration runner also has an opt-in SQL Server contour:

```bash
make integration-test-sqlserver
```

That target starts the SQL Server Docker Compose profile and runs the
SQL Server-compatible migration fixture scenarios plus
`dynamic_sqlserver_identity_schema_bracket_reserved_words`, which renders a
Ptah schema to T-SQL, applies it to a live database, inserts through an
`IDENTITY` column, and verifies schema/identifier round-trip via introspection.
