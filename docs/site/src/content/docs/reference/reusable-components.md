---
title: Reusable Components
description: Use Ptah as a Go schema engine, not only as a CLI.
---

Ptah can be used in three different ways:

- the native CLI, such as `ptah schema render` and `ptah migrations up`;
- the Atlas-compatible CLI surface under `ptah atlas <command> ...`;
- stable Go packages imported by another Go program.

The separate `ptah-compat` binary is the binary-level drop-in replacement for
scripts that need Atlas-style root commands. It uses the same Atlas-compatible
surface as `ptah atlas`, so it is not listed as a separate component category.

The CLI is only one consumer of the engine. The same public packages can power
internal platform CLIs, CI gates, schema documentation generators, migration
automation, and database tooling that should not shell out to `ptah`.

Ptah is pre-GA. The supported embedder surface is the package list in
[Public Go API](../public-api/).
Packages under `internal/...` are not supported embedder APIs, even when a CLI
uses them internally.

## Component Map

| Need | Stable package(s) | What it gives you |
| --- | --- | --- |
| Build SQL DDL programmatically | `core/ast`, `core/renderer` | Dialect-aware SQL from structured AST nodes. |
| Parse Go schema annotations | `core/goschema` | Go source comments to Ptah's schema IR. |
| Parse Atlas HCL schema files | `atlascompat` | Atlas-style HCL schema files to Ptah's schema IR through a stable compatibility wrapper. |
| Parse YAML schema files | Native CLI and schema-file workflows | YAML schema parsing is currently an implementation detail, not a stable public package. Use the CLI or create a follow-up API proposal before embedding it. |
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
| Model dialect and version behavior | `core/platform`, `core/platform/capability` | Dialect constants and capability sets for version-aware rendering and planning. |

`atlascompat` is intentionally narrow. It gives external tools a stable way to
use Atlas-compatible parsing, SQL parsing, schema conversion, and migration-sum
helpers without promoting the implementation packages behind those features.

## AST Deep Dive

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
sub-language. It is also not a DML query builder; SELECT/WHERE/JOIN style query
builder work belongs to issue
[`#98`](https://github.com/stokaro/ptah/issues/98) or follow-up design.

This complete example uses only public packages. The same AST/rendering path is
validated by
[`examples/reusable_components`](https://github.com/stokaro/ptah/tree/master/examples/reusable_components):

```go
package main

import (
	"fmt"
	"log"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer"
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

## End-To-End Reuse Examples

The examples below use only stable public packages unless a block is explicitly
marked as pseudo-code. Complete copy-pasteable versions are kept in
[`examples/reusable_components/reusable_components_test.go`](https://github.com/stokaro/ptah/blob/master/examples/reusable_components/reusable_components_test.go)
and are validated with:

```bash
go test ./examples/reusable_components
```

Inline blocks in this section are excerpts from those examples or from the
minimal host-tool flow described by the heading.

### Render SQL From Go Annotations

Use this when a Go package owns the desired schema.

```go
fsys := fstest.MapFS{
	"models/user.go": {Data: []byte(`package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int

	//migrator:schema:field name="email" type="TEXT" not_null="true" unique="true"
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

### Render SQL From Atlas HCL

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

### Render SQL From YAML Schema

YAML schema files are supported by Ptah workflows, but the YAML parser is not a
stable embedder package today. For embedders, prefer Go annotations or HCL
through `atlascompat`. For scripts, use the CLI:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

If you need a stable YAML-to-IR Go API, create a design issue before depending
on the current implementation package.

### Inspect A Live Database And Diff

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

diff := schemadiff.CompareWithDialect(desired, live, conn.Info().Dialect)
sql, err := planner.GenerateSchemaDiffSQL(diff, desired, conn.Info().Dialect)
if err != nil {
	return err
}
fmt.Println(sql)
```

For unit tests or offline planning, you can build a `dbschema/types.DBSchema`
value directly and pass it to `schemadiff`.

### Embed The Migrator

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

### Build A CI Gate

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

findings, err := lint.LintFS(fsys, lint.Options{Dialect: "postgres"})
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

### Use Capabilities

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

## Use Cases

### Internal Platform CLI

Use this when a company wants one database workflow command that also handles
environment selection, approvals, tickets, or audit logs.

Packages: `core/goschema`, `dbschema`, `migration/schemadiff`,
`migration/planner`, `migration/migrator`, `migration/safety`.

Minimal flow (pseudo-code; host code must provide context, connections, inputs,
and error handling):

```go
desired, _ := goschema.ParseDir("./models")
conn, _ := dbschema.ConnectToDatabase(ctx, url)
live, _ := conn.Reader().ReadSchema()
diff := schemadiff.CompareWithDialect(desired, live, conn.Info().Dialect)
nodes, _ := planner.GenerateSchemaDiffAST(diff, desired, conn.Info().Dialect)
assessments, _ := safety.AssessRendered(nodes, conn.Info().Dialect)
```

Caveat: keep approval, locking, and production rollout policy in the host tool;
Ptah supplies schema and migration primitives.

### Migration CI Gate

Use this when a pull request should fail on checksum drift, dangerous DDL, or
lint findings.

Packages: `atlascompat`, `migration/migrator`, `migration/lint`,
`migration/safety`, `migration/risk`.

Minimal flow (pseudo-code; host code must provide the filesystem, policy, and
process exit behavior):

```go
result, _ := atlascompat.VerifySum(fsys, migrator.MigrationDirFormatPtah)
findings, _ := lint.LintFS(fsys, lint.Options{Dialect: "postgres"})
if !result.OK() || len(findings) > 0 {
	os.Exit(1)
}
```

Caveat: a filesystem lint gate does not prove live database compatibility. Add
a dev database replay when that matters.

### Schema Documentation Generator

Use this when a tool needs Markdown, diagrams, OpenAPI or GraphQL-oriented
schema summaries.

Packages: `core/goschema`, `atlascompat`, `dbschema/types`,
`migration/schemadiff`, `core/platform/capability`.

Minimal flow (pseudo-code; host code must provide the schema input and output
formatting):

```go
db, _ := goschema.ParseDir("./models")
for _, table := range db.Tables {
	fmt.Printf("## %s\n", table.Name)
}
```

Caveat: OpenAPI and GraphQL rendering are not listed as stable public packages;
generate from the stable schema IR instead of importing internal renderers.

### Atlas-Compatible Transition

Use this when a repository wants Atlas-shaped migration directories or HCL
sources while using Ptah as the implementation engine.

Packages: `atlascompat`, `migration/migrator`, `core/renderer`.

Minimal flow (pseudo-code; host code must provide the filesystem and migration
directory format policy):

```go
provider, _ := migrator.NewFSMigrationProvider(
	fsys,
	migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
)
_ = provider.Migrations()
```

Caveat: this does not mean full Atlas parity. Use the conformance reports for
measured compatibility.

### Database Dialect Extension Research

Use this when evaluating support for a new dialect or a new server version.

Packages: `core/platform/capability`, `core/ast`, `core/renderer`,
`migration/planner`, `migration/safety`.

Minimal flow (pseudo-code; host code must provide the AST node, dialect target,
and unsupported-feature handling):

```go
caps := capability.ForServerVersion("postgres", "17.0")
sql, _ := renderer.RenderSQLWithCapabilities("postgres", caps, node)
assessment := safety.AssessSQL(sql)
_ = assessment
```

Caveat: custom dialect registration is intentionally limited. Create a design
issue before relying on out-of-tree planner or renderer extension points.

### Application Embedded Migrations

Use this when tests or controlled startup need migrations without a separate
binary.

Packages: `migration/migrator`, `dbschema`.

Minimal flow (pseudo-code; host code must provide the database connection,
filesystem, startup policy, and error handling):

```go
m, _ := migrator.NewFSMigrator(conn, os.DirFS("./migrations"))
_ = m.Up(ctx)
```

Caveat: avoid uncontrolled production startup migrations unless the host
application owns locking, approvals, observability, and rollback policy.

### Schema Drift Bot

Use this when a bot should comment planned schema changes on a pull request.

Packages: `core/goschema`, `dbschema`, `migration/schemadiff`,
`migration/planner`, `migration/safety`.

Minimal flow (pseudo-code; host code must provide desired and live schemas,
dialect selection, and review delivery):

```go
diff := schemadiff.CompareWithDialect(desired, live, dialect)
sql, _ := planner.GenerateSchemaDiffSQL(diff, desired, dialect)
fmt.Println(sql)
```

Caveat: generated SQL is a plan proposal. Require human review for destructive
changes and capability-sensitive dialect behavior.

## Stability And Boundaries

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

## Comparisons

Atlas has the more mature CLI ecosystem and upstream documentation. Ptah's
distinct value for embedders is an independent MIT-licensed implementation with
stable Go packages for the schema engine surface Ptah exposes. Ptah should not
claim full Atlas parity until the conformance gates prove it.

`golang-migrate` and `goose` are simpler versioned migration runners. Ptah also
has schema IR, diffing, planning, rendering, linting, safety classification,
capabilities, and Atlas-compatible flows.

Prisma and Ent tie schema workflows to their ecosystems. Ptah is Go-first but
is not an ORM.

Skeema is an excellent MySQL/MariaDB declarative schema tool. Ptah is
multi-source, Go-embeddable, and multi-dialect.

`sqlc` generates typed code from queries. Ptah focuses on schema and migration
tooling, not query-code generation.

## Follow-Up Gaps

This page intentionally avoids documenting unsupported public APIs. Follow-up
issues should be created before exposing:

- a stable YAML schema parser package;
- a stable Atlas HCL renderer package beyond `atlascompat` wrappers;
- more ergonomic AST builder helpers if current AST construction becomes too
  verbose for embedder docs;
- snippet validation that extracts docs code blocks automatically;
- out-of-tree dialect, planner, renderer, or lint-rule extension points.
