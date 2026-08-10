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
  constraints, and indexes with ordered ascending or descending key columns.
- Filtered indexes: the index annotation's `condition` predicate survives
  introspection, comparison, planning, and rendering as
  `CREATE INDEX ... WHERE ...`. See [Filtered Indexes](#filtered-indexes).
- Rendering for views and triggers when definitions are supplied as raw SQL.
- Live schema introspection from `sys.tables`, `sys.columns`,
  `sys.foreign_keys`, `sys.indexes`, and related catalog views.
- Transactional migration apply for DDL supported by SQL Server.
- Transactional dev-database cleanup across supported user schemas, with a
  preflight that rejects replication-enabled and subscriber databases,
  replicated tables, and unsupported database-scoped artifacts.

Enums are represented as `NVARCHAR(255)` plus generated CHECK constraints.
SQL Server does not have a native enum object equivalent to PostgreSQL
`CREATE TYPE ... AS ENUM`.

## Identifier Collation

SQL Server compares database object identifiers using catalog collation
semantics. Ptah reads the catalog collation when opening a connection, but does
not infer behavior from `_CI_`, `_CS_`, or other collation-name substrings.

Use the live-aware comparison API when embedding Ptah:

```go
diff, err := schemadiff.CompareWithDatabase(
	ctx,
	conn,
	desired,
	current,
	compareOptions,
)
if err != nil {
	return err
}
```

Ptah sends the finite candidate set of schema, table, column, and index names as
one bound JSON parameter. SQL Server groups the names with
`COLLATE CATALOG_DEFAULT`, so the result follows the target catalog's case,
accent, locale, kana, and width rules. The resulting diff carries the immutable
equivalence snapshot through destructive-change policy, forward and reverse
planning, checkpoint generation, and shadow verification. Shadow execution is
rejected when the shadow catalog produces different equivalence classes.

`CompareWithDatabaseInfo` is safe for offline comparison or when the caller
already supplies a complete resolved `DBInfo.IdentifierSemantics` snapshot.
It returns an error when a non-zero snapshot is invalid, does not cover every
candidate identifier, or reveals a target table, column, or index collision.
Omitting the snapshot selects conservative dialect-only SQL Server rules.

`CompareWithOptions` has no error return. When its explicit snapshot is
invalid, incomplete, or collision-prone, Ptah discards that snapshot and
produces a conservative offline diff instead of treating unresolved names as
equal.

An offline `CompareWithDialect(..., "sqlserver")` call cannot know the target
collation. It confirms only exact-spelling identity. Distinct unresolved names
in the same catalog namespace are treated as potential conflicts because a
target collation may equate them by case, accent, locale, kana, or width.
Planning rejects ambiguity before emitting SQL and requires a live resolved
snapshot. This can make dialect-only SQL Server planning intentionally stricter
than live planning.

Ptah does not reproduce SQL Server collation rules locally. The live catalog is
the source of truth.

## Filtered Indexes

Declare a SQL Server filtered index with the index annotation's `condition`
(or `where`) attribute:

```go
//ptah:schema:index name="idx_active_users" fields="status" condition="status = 1"
```

Ptah renders the predicate verbatim:

```sql
CREATE INDEX [idx_active_users] ON [dbo].[users] ([status]) WHERE status = 1;
```

SQL Server cannot alter an index predicate in place, so a changed predicate is
planned as a replacement: `DROP INDEX ... ON ...` followed by
`CREATE INDEX ... WHERE ...` carrying the target predicate.

SQL Server persists `sys.indexes.filter_definition` in a canonical spelling —
identifiers come back bracket-quoted and numeric literals parenthesized, so
`status = 1` is stored as `([status]=(1))`. Predicate comparison therefore
normalizes case, whitespace, wrapping parentheses, bracket identifier quoting,
and parentheses around bare numeric literals before deciding whether an index
changed.

Rewrites SQL Server applies beyond that spelling — for example the `N'...'`
prefix it adds to Unicode string literals or implicit `CAST` insertions — are
not reconstructed: such predicates compare as changed and are re-planned as a
replacement on every run.

If a predicate keeps reporting drift after it was applied, spell the
annotation the way the catalog stores it (compare with `ptah db read`); the
rendered SQL always preserves the annotation text verbatim, so no predicate is
ever silently dropped.

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
  table SQL. User-schema upserts can be rendered through Ptah's reusable
  DML upsert AST as SQL Server `MERGE`; see
  [DML Upsert AST](dml_upsert.md).
- View and trigger introspection records SQL Server's persisted definition text,
  but Ptah does not yet normalize it into body-only drift-safe definitions.
- Index introspection and planning preserve ordered key columns, their
  direction, and filtered-index predicates, but do not yet preserve included
  columns.
- `DROP INDEX IF EXISTS` and `DROP CONSTRAINT IF EXISTS` are not used in the
  portable preset; Ptah relies on scoped, deterministic object ownership.
- Schema-scoped `DropAllTables(ctx)` removes tables and foreign keys owned by the
  selected schema only. If another schema has a foreign key referencing a
  selected table, cleanup fails with a blocking-constraint error instead of
  mutating objects outside the selected schema.
- SQL Server-specific options such as `WITH (ONLINE = ON)` and
  `NOT FOR REPLICATION` are not planned yet.
- Database-realm cleanup fails before DDL when replication is configured,
  subscription state is unavailable or unexpected, a user table is replicated,
  or another unsupported database-scoped artifact exists.

## Live Tests

SQL Server coverage is part of the repository's single tagged integration
contour. Follow the service and environment matrix in the
[integration test guide](../integration/README.md), then run the recursive
`./integration/...` contour. Do not select the old production package paths or
the removed `ptah_live_*` tags.

The test creates a temporary schema, tables with `IDENTITY`, a reserved-word
table name, computed columns, CHECK/UNIQUE/FK constraints, and an index, then
verifies that Ptah can introspect them through the SQL Server reader. The live
coverage also verifies full database-realm cleanup of cross-schema dependency
graphs and migration metadata placement through the URL `schema` parameter.

The cleanup suite executes replication-state catalog checks on a real SQL
Server and unit coverage verifies fail-closed subscriber and replicated-table
states. Schema-scoped cleanup fails safely when another schema owns a foreign
key into the selected schema, and computed column and default catalog readback
are compared as a zero-diff schema.

SQL Server advisory-lock coverage verifies both normal migration progress and
timeout reporting when another session holds `ptah_migrate`.

Identifier-collation coverage creates separate CI and CS databases, verifies
discovered semantics, executes case-only, changed-column, and ASC-to-DESC
replacements safely, proves case variants coexist only on the CS database, and
covers Turkish dotless-I, accent-insensitive Latin, Greek sigma, Japanese
kana, and width equivalence.

Filtered-index coverage creates a filtered index from a natural-spelling
`condition` annotation, replaces a changed predicate through `DROP INDEX` +
`CREATE INDEX ... WHERE`, and re-introspects to a zero diff against the
canonical `filter_definition` spelling.

The generator coverage replays the prior schema on a separate SQL Server shadow
database, then applies the generated ASC-to-DESC migration up, down, and up
again while re-introspecting the index direction after every transition. The
repository workflow runs that coverage through the same recursive tagged
contour and rejects every missing service as a skipped test.

The integration runner also has an opt-in SQL Server contour:

```bash
make integration-test-sqlserver
```

That target starts the SQL Server Docker Compose profile and runs the
SQL Server-compatible migration fixture scenarios plus
`dynamic_sqlserver_identity_schema_bracket_reserved_words`, which renders a
Ptah schema to T-SQL, applies it to a live database, inserts through an
`IDENTITY` column, and verifies schema/identifier round-trip via introspection.
