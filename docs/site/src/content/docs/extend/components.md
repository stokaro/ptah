---
title: Reusable components
description: Use Ptah as a Go schema engine, not only as a CLI.
---

Ptah can be used in three different ways:

- the native CLI, such as `ptah schema render` and `ptah migrations up`;
- the Atlas-compatible CLI surface of the separate `ptah-compat` drop-in
  binary;
- stable Go packages imported by another Go program.

The CLI is only one consumer of the engine. The same public packages can power
internal platform CLIs, CI gates, schema documentation generators, migration
automation, and database tooling that should not shell out to `ptah`.

Ptah is pre-GA. The supported embedder surface is the package list in
[Public Go API](../public-api/).
Packages under `internal/...` are not supported embedder APIs, even when a CLI
uses them internally.

## Component map

| Need | Stable package(s) | What it gives you |
| --- | --- | --- |
| Build SQL DDL programmatically | `core/ast`, `core/renderer` | Dialect-aware SQL from structured AST nodes. |
| Build parameterized SELECT queries | `core/query`, `core/renderer` | Fluent, dialect-aware SELECT/WHERE/ORDER BY/LIMIT with bound parameters. See [Query builder](../query-builder/). |
| Parse Go schema annotations | `core/goschema` | Go source comments to Ptah's schema IR. |
| Parse Atlas HCL schema files | `atlascompat` | Atlas-style HCL schema files to Ptah's schema IR through a stable compatibility wrapper. |
| Parse YAML schema files | Native CLI and schema-file workflows | An implementation detail, not a stable package. Use the CLI, or open an API proposal before embedding it. |
| Render SQL from schema IR | `core/renderer`, `atlascompat` | Ordered DDL statements for supported dialects. |
| Introspect live databases | `dbschema`, `dbschema/types` | Database schema snapshots from live connections. |
| Compare desired vs. live schemas | `migration/schemadiff`, `migration/schemadiff/types` | Structured schema diffs for planning and reporting. |
| Plan SQL migrations | `migration/planner` | Ordered AST or SQL statements for schema changes. |
| Generate migration files | `migration/generator` | Versioned migration files from desired/live differences. |
| Apply migrations | `migration/migrator` | Embedded migration runner with filesystem providers, revision metadata, dry-run planning, and transaction modes. |
| Check migration integrity | `atlascompat`, `migration/migrator` | Ptah and Atlas migration-directory hash validation. |
| Lint migration SQL | `migration/lint` | Rule-coded findings for migration files in CI. |
| Assess risk and safety | `migration/risk`, `migration/safety` | Destructive-change classification and rendered-statement safety reports. |
| Seed data | `migration/seeder` | Environment-scoped seed discovery and execution. |
| Model dialect, version, and identifier behavior | `core/platform`, `core/platform/capability`, `core/platform/identifier` | Dialect constants, capability sets, and catalog identifier semantics for comparison and planning. |

`atlascompat` is intentionally narrow. It gives external tools a stable way to
use Atlas-compatible parsing, SQL parsing, schema conversion, and migration-sum
helpers without promoting the implementation packages behind those features.

Index identity remains table-qualified in schema diffs even when the target
database uses a broader namespace. Planners apply the target rules when ordering
replacements: PostgreSQL, YugabyteDB, Spanner, and SQLite use schema-scoped
index names; CockroachDB, MySQL, MariaDB, SQL Server, and ClickHouse use
table-scoped index names.
On schema-scoped engines, an unqualified owner denotes the dialect's default
schema (`public` for the PostgreSQL family and `main` for SQLite) and remains
independent from other named schemas.

## AST deep dive

Ptah uses a structured AST so callers can describe schema intent without
manually concatenating SQL strings. A table, column, constraint, index, enum, or
schema object is represented as a typed node. Renderers then translate the same
node graph into dialect-specific SQL.

That separation matters for embedders:

- AST construction is easier to unit-test than raw SQL string assembly.
- Dialect renderers own quoting, syntax differences, and unsupported-feature
  errors.
- Planners can return AST nodes first, so callers can inspect risk before
  rendering or executing SQL.
- Capability-aware renderers can change behavior for a database version without
  rewriting the caller's schema model.

The AST is mature for DDL objects that Ptah currently renders and plans: tables,
columns, constraints, indexes, enums, extensions, views, materialized views,
triggers, row-level security policies, roles, grants, and routine placeholders
where supported. It is not a full SQL parser for every dialect-specific
sub-language.

DML query building has a bounded slice: `core/query` builds parameterized
`SELECT` statements with `INNER`/`LEFT`/`RIGHT`/`FULL OUTER` joins, table aliases
and qualified columns, a composable `WHERE` (and join `ON`) expression tree,
`ORDER BY`, and `LIMIT`/`OFFSET`, rendered through `renderer.RenderSelect`.
`GROUP BY`, `HAVING`, subqueries, and the `INSERT`/`UPDATE`/`DELETE` family are
follow-up phases of issue
[`#98`](https://github.com/stokaro/ptah/issues/98). See the
[Query builder](../query-builder/) reference for the full API.

This complete example uses only public packages. The same AST/rendering path is
validated by
[`examples/reusable_components`](https://github.com/stokaro/ptah/tree/master/examples/reusable_components):

```go
package main

import (
	"fmt"
	"log"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

func main() {
	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary()).
		AddColumn(ast.NewColumn("email", "TEXT").SetNotNull().SetUnique())

	sql, err := renderer.RenderSQL("postgres", table)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sql)
}
```

Expected output shape:

```sql
-- POSTGRES TABLE: users --
CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY NOT NULL,
  "email" TEXT UNIQUE NOT NULL
);
```

## End-to-end reuse examples

The examples below use only stable public packages unless a block is explicitly
marked as pseudo-code. Complete copy-pasteable versions are kept in
[`examples/reusable_components/reusable_components_test.go`](https://github.com/stokaro/ptah/blob/master/examples/reusable_components/reusable_components_test.go)
and are validated with:

```bash
go test ./examples/reusable_components
```

Inline blocks in this section are excerpts from those examples or from the
minimal host-tool flow described by the heading.

### Render SQL from Go annotations

Use this when a Go package owns the desired schema.

```go
fsys := fstest.MapFS{
	"models/user.go": {Data: []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int

	//ptah:schema:field name="email" type="TEXT" not_null="true" unique="true"
	Email string
}
`)},
}

db, err := goschema.ParseFS(fsys, "models")
if err != nil {
	return err
}
statements, err := renderer.GetOrderedCreateStatements(db, "sqlite")
if err != nil {
	return err
}
fmt.Println(statements[0])
```

For targets other than SQLite, schema rendering places every `CREATE TABLE`
before phase-two foreign key statements. SQLite keeps foreign keys inline.
Malformed or capability-incompatible foreign keys return a typed error and no
partial statement list.

### Render SQL from Atlas HCL

Use `atlascompat` when you need Atlas-shaped HCL input through a stable public
wrapper.

```go
db, err := atlascompat.ParseAtlasHCL([]byte(`
schema "public" {}

table "users" {
  schema = schema.public
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), "schema.hcl")
if err != nil {
	return err
}

list := atlascompat.SchemaToAST(*db, "postgres")
sql, err := renderer.RenderSQL("postgres", list.Statements...)
if err != nil {
	return err
}
fmt.Println(sql)
```

### Render SQL from YAML schema

YAML schema files are supported by Ptah workflows, but the YAML parser is not a
stable embedder package today. For embedders, prefer Go annotations or HCL
through `atlascompat`. For scripts, use the CLI:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

If you need a stable YAML-to-IR Go API, create a design issue before depending
on the current implementation package.

### Inspect a live database and diff

Use this when a tool needs to compare a desired schema against a live database.
The block below is pseudo-code because the URL must point to a database you
control.

```go
ctx := context.Background()
conn, err := dbschema.ConnectToDatabase(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
	return err
}
defer dbschema.CloseAndWarn(conn)

live, err := conn.Reader().ReadSchema()
if err != nil {
	return err
}

desired, err := goschema.ParseDir("./models")
if err != nil {
	return err
}

diff, err := schemadiff.CompareWithDatabase(ctx, conn, desired, live, nil)
if err != nil {
	return err
}
info := conn.Info()
sql, err := planner.GenerateSchemaDiffSQLWithCapabilities(
	diff,
	desired,
	info.Dialect,
	info.Capabilities,
)
if err != nil {
	return err
}
fmt.Println(sql)
```

For unit tests or offline planning, you can build a `dbschema/types.DBSchema`
value directly and pass it to `schemadiff`.

Index names are table-scoped in some dialects. Use `diff.IndexAdditions()` and
`diff.IndexRemovals()` when consuming index changes through a copied slice, or
read the canonical `IndexesAdded` and `IndexesRemoved` `[]IndexRef` fields
directly. Every reference includes its owning table.

Planning rejects missing owners, unresolved additions, and same-name target
indexes that conflict in the selected dialect's namespace. When a custom
consumer starts from `goschema.Index` values, use
`goschema.ResolveIndexTableNames` to resolve all owning tables in one indexed
pass instead of scanning the table list for each index. MySQL and SQLite index
matching applies ASCII case folding.

MariaDB matching also applies Unicode lowercase equivalence. All three retain
the declared spelling in `IndexRef` values and rendered SQL.

For a live SQL Server connection, `CompareWithDatabase` sends the finite set of
candidate schema, table, column, and index names to SQL Server as one bound JSON
parameter. SQL Server groups those names with `COLLATE CATALOG_DEFAULT`; Ptah
stores the returned equivalence classes and catalog collation in the resulting
`SchemaDiff`. Diff policy, forward and reverse planning, checkpoint generation,
and shadow verification then use that immutable snapshot. This handles
case, accent, locale, kana, and width behavior according to the target catalog
instead of approximating it in Go.

`CompareWithDatabaseInfo` remains useful for deterministic offline comparison
or for callers that already provide a complete resolved
`DBInfo.IdentifierSemantics` snapshot. It returns an error when a non-zero
snapshot is invalid, incomplete for the compared identifier set, or exposes a
target table, column, or index collision. Omitting the snapshot selects
conservative dialect rules. SQL Server embedders should normally use
`CompareWithDatabase`.

`CompareWithOptions` has no error return. When an explicit snapshot is invalid,
incomplete, or collision-prone, it falls back to conservative dialect rules
instead of allowing unresolved identifiers to collapse into a false zero diff.

Dialect-only SQL Server comparison cannot know the database collation. It keeps
exact identity for deterministic offline diffs, but treats distinct unresolved
names in one catalog namespace as potentially equivalent. Planning rejects
that ambiguity before SQL generation and requires a live resolved snapshot.
Ptah does not emulate SQL Server collation rules locally.

When applying a reusable destructive-change policy to a known database target,
use `diffpolicy.ApplyForDialect`. It preserves the drop/create pair required by
schema-scoped engines PostgreSQL, YugabyteDB, Spanner, and SQLite while keeping
same-named indexes on different CockroachDB, MySQL, MariaDB, SQL Server, and
ClickHouse tables independent. CockroachDB plans retain the owning table so the
renderer emits an unambiguous `table@index` drop target.

### Embed the migrator

Use this when an application or internal tool wants to run migrations from an
`fs.FS` without invoking the CLI. The block below is pseudo-code because it
needs a real database connection.

```go
fsys := os.DirFS("./migrations")
provider, err := migrator.NewFSMigrationProvider(fsys)
if err != nil {
	return err
}

conn, err := dbschema.ConnectToDatabase(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
	return err
}
defer dbschema.CloseAndWarn(conn)

m := migrator.NewMigrator(conn, provider)
status, err := m.Status()
if err != nil {
	return err
}
fmt.Printf("pending: %d\n", len(status.PendingMigrations))
return m.Up(ctx)
```

The migrator owns revision-table metadata. Use dry-run and explicit transaction
mode options when your host tool needs preview or dialect-specific transaction
behavior.

A runnable embedded-migrator example with migration fixtures lives in
[`examples/migrator`](https://github.com/stokaro/ptah/tree/master/examples/migrator).

### Build a CI gate

Use this when a repository wants integrity and policy checks before merging
migration files. The integrity and lint calls are compile-checked in
`examples/reusable_components`.

```go
fsys := os.DirFS("./migrations")

sum, err := atlascompat.ComputeSum(fsys, migrator.MigrationDirFormatPtah)
if err != nil {
	return err
}
fmt.Printf("directory hash: %s\n", sum.DirHash)

lintConfig, err := lint.LoadConfigFS(fsys, lint.ConfigFileName)
if err != nil {
	return err
}
dialect := lintConfig.Dialect
if dialect == "" {
	dialect = "postgres"
}

lintOptions := lint.Options{
	Dialect:     dialect,
	Disabled:    lintConfig.DisabledRules,
	RuleConfigs: lintConfig.Rules,
}
findings, err := lint.LintFS(fsys, lintOptions)
if err != nil {
	return err
}
if len(findings) > 0 {
	for _, finding := range findings {
		fmt.Println(lint.Describe(finding))
	}
	return fmt.Errorf("migration lint failed")
}
```

`LintFS` and `AnalyzeFS` validate `lint.Options` before reading migrations. A
host that can return early when no work is pending, or that offers an execution
override which skips analysis, should call `lint.ValidateOptions(lintOptions)`
before that branch. This rejects unknown selectors against the active built-in,
registered, and per-run rule set even when no migration is analyzed.

Use `lint.AnalyzeFS` when more than findings are needed. It captures every SQL
file plus migration metadata (`atlas.sum`, `ptah.sum`, and
`.ptah-lint.yaml`) once and excludes unrelated files. The immutable result
contains prepared files, exact statement spans, finding-to-statement contexts,
and a read-only filesystem snapshot. Replay, checksum, report, and
migration-provider code can consume that snapshot without reopening a changing
migration directory:

```go
analysis, err := lint.AnalyzeFS(fsys, lint.Options{
	Dialect: "postgres",
	Selection: lint.VersionSelection{
		Versions:   []int64{42, 43},
		Restricted: true,
	},
})
if err != nil {
	return err
}

selected := analysis.SelectedFiles()
findings := analysis.Findings()
snapshot := analysis.SnapshotFS()
fmt.Printf("linted %d of %d files and found %d issues\n",
	len(selected), len(analysis.Files()), len(findings))

m, err := migrator.NewFSMigrator(conn, snapshot)
if err != nil {
	return err
}
_ = m
```

`VersionSelection.Restricted` distinguishes no selector from an explicitly
empty changeset. Native Ptah callers should keep the zero-value
`CompatibilityProfileNative`; `CompatibilityProfileAtlas` exists for
Atlas-compatible command adapters. It switches the `atlas:nolint` code
namespace to the codes that profile prints and enables the file-header form,
without changing native safety behavior. Atlas analyzer-name selectors resolve
under both profiles, because they name rule families rather than printed
codes.

Each finding context identifies its zero-based statement index. Structured
subjects preserve the executable identifier spelling: table subjects use
`SubjectTable`; column subjects use `SubjectColumn` and can include `Parent`
and `DataType`. In Atlas compatibility mode, a bare file-header
`-- atlas:nolint` marks `File.Ignored`; it does not merely clear the file's
findings. Report adapters should omit ignored files while retaining them in the
captured snapshot.

A host tool can add its own analyzers to the same run without reimplementing
the dialect-aware scanner. `Options.ExtraRules` appends per-run rules — the
preferred shape, with no global state. `lint.Register` separately installs a
rule process-wide and returns an error for invalid or duplicate rules; callers
must handle that error during initialization. Either way, the rule receives statements Ptah has already
prepared: `Statement.Words` is the comment-free token-word sequence the
built-in rules scan, and `Statement.Canonical` is the uppercased display
form. This example is compile-checked in `examples/reusable_components`:

```go
findings, err = lint.LintFS(fsys, lint.Options{
	Dialect: "postgres",
	ExtraRules: []lint.Rule{{
		Code:     "ORG101",
		Title:    "TEXT column without explicit limit",
		Severity: lint.SeverityWarning,
		CheckStatement: func(stmt *lint.Statement) (bool, string) {
			return slices.Contains(stmt.Words, "TEXT"), "use VARCHAR(n) so limits stay reviewable"
		},
	}},
})
```

For plugin-style process initialization, register once and propagate the
validation error:

```go
err := lint.Register(lint.Rule{
	Code:     "ORG102",
	Title:    "organization policy",
	Severity: lint.SeverityWarning,
	CheckStatement: func(stmt *lint.Statement) (bool, string) {
		return slices.Contains(stmt.Words, "UNLOGGED"), "UNLOGGED tables require platform review"
	},
})
if err != nil {
	return err
}
```

Rule codes use uppercase ASCII letters and digits and start with a letter.
Custom codes flow through reporting, `--disable`, inline `-- ptah:nolint`
directives, and `.ptah-lint.yaml` per-rule severity and path excludes
exactly like built-in codes; the configuration surface is documented in
[Integrity and safety](../../versioned/integrity-and-safety/).

### Use capabilities

Use capabilities when syntax depends on a dialect version rather than only a
dialect family.

```go
caps := capability.ForServerVersion("postgres", "17.0")
table := ast.NewCreateTable("accounts").
	AddColumn(ast.NewColumn("id", "INTEGER").
		SetIdentity("BY_DEFAULT", "1", "1").
		SetPrimary())

sql, err := renderer.RenderSQLWithCapabilities("postgres", caps, table)
if err != nil {
	return err
}
fmt.Println(sql)
```

Dialect defaults such as `capability.ForDialect("postgres")` are useful for
offline generation. Live database connections expose resolved capabilities
through `conn.Info().Capabilities`; use those when a database server has already
been inspected.

## Use cases

Each entry below names the stable packages for a common embedding shape, the
end-to-end example above to start from, and what stays in the host tool.

**Internal platform CLI** — start from
[Inspect a live database and diff](#inspect-a-live-database-and-diff).
Stable packages: `core/goschema`, `dbschema`, `migration/schemadiff`,
`migration/planner`, `migration/migrator`, `migration/safety`.
The host tool keeps approval, locking, and production rollout policy.

**Migration CI gate** — start from [Build a CI gate](#build-a-ci-gate).
Stable packages: `atlascompat`, `migration/migrator`, `migration/lint`,
`migration/safety`, `migration/risk`.
The host tool keeps failure policy; add a dev database replay when live
compatibility matters.

**Schema documentation generator** — start from
[Render SQL from Go annotations](#render-sql-from-go-annotations).
Stable packages: `core/goschema`, `atlascompat`, `dbschema/types`,
`migration/schemadiff`, `core/platform/capability`.
The host tool keeps output formatting; generate from the stable schema IR, not
internal renderers.

**Atlas-compatible transition** — start from
[Embed the migrator](#embed-the-migrator).
Stable packages: `atlascompat`, `migration/migrator`, `core/renderer`.
The host tool keeps parity expectations; use the conformance reports for
measured compatibility.

**Dialect extension research** — start from [Use capabilities](#use-capabilities).
Stable packages: `core/platform/capability`, `core/ast`, `core/renderer`,
`migration/planner`, `migration/safety`.
The host tool keeps unsupported-feature handling; create a design issue before
relying on out-of-tree extension points.

**Application embedded migrations** — start from
[Embed the migrator](#embed-the-migrator).
Stable packages: `migration/migrator`, `dbschema`.
The host tool keeps startup locking, approvals, observability, and rollback
policy; avoid uncontrolled production startup migrations.

**Schema drift bot** — start from
[Inspect a live database and diff](#inspect-a-live-database-and-diff).
Stable packages: `core/goschema`, `dbschema`, `migration/schemadiff`,
`migration/planner`, `migration/safety`.
The host tool keeps review delivery; require human review for destructive
changes.

## Stability and boundaries

- Stable embedder packages are listed in
  [Public Go API](../public-api/).
- There is currently no provisional public package tier.
- `internal/...` packages are not supported embedder APIs.
- Ptah is pre-GA. Before a tagged release exists, pin a commit for production
  embedders; after releases exist, pin an explicit version.
- Public error handling should prefer typed or sentinel errors where the public
  API exposes them, such as `core/ptaherr`.
- Native CLI usage, Atlas-compatible CLI usage, and direct Go embedding are
  separate surfaces. Do not treat a CLI flag as proof that a matching Go API is
  stable.

## Follow-up gaps

This page intentionally avoids documenting unsupported public APIs. Follow-up
issues should be created before exposing:

- a stable YAML schema parser package;
- a stable Atlas HCL renderer package beyond `atlascompat` wrappers;
- more ergonomic AST builder helpers if current AST construction becomes too
  verbose for embedder docs;
- snippet validation that extracts docs code blocks automatically;
- out-of-tree dialect, planner, renderer, or lint-rule extension points.
