---
title: Reusable components
description: Use Ptah as a Go schema engine, not only as a CLI.
type: how-to
audience:
  - "go-developer"
readerQuestion: "How do I use Ptah as a Go schema engine, not only as a CLI?"
goal: "Select and call the public Go package for a schema task."
sourceOfTruth:
  - "core"
  - "migration"
  - "dbschema"
generated: false
overlaps: []
disposition: keep
sourceMode: source-neutral
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
| Build SQL DDL programmatically | `core/ast`, `core/astbuilder`, `engine/builtin` | Dialect-aware SQL from structured AST nodes, written as struct literals or as builder chains. |
| Build parameterized DML statements | `core/query` | Fluent, dialect-aware SELECT, INSERT, UPDATE, DELETE and YDB's UPSERT with bound parameters. See [Query builder](../query-builder/). |
| Parse Go schema annotations | `core/goschema` | Go source comments to Ptah's schema IR. |
| Parse Atlas HCL schema files | `atlascompat` | Atlas-style HCL schema files to Ptah's schema IR through a stable compatibility wrapper. |
| Parse YAML schema files | `core/yamlschema` | Ptah's YAML authoring format to the schema IR, from bytes or from a path. |
| Render SQL from schema IR | `engine/builtin`, `atlascompat` | Ordered DDL statements for supported dialects. |
| Introspect live databases | `dbschema`, `catalog` | Database schema snapshots from live connections. |
| Compare desired vs. live schemas | `migration/schemadiff`, `migration/schemadiff/difftypes` | Structured schema diffs for planning and reporting. |
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

DML lives in `core/query`. It builds parameterized `SELECT`, `INSERT`,
`UPDATE` and `DELETE` statements, and YDB's `UPSERT INTO`, and renders them with
`query.RenderSelect`, `query.RenderInsert`, `query.RenderUpdate` and
`query.RenderDelete` for a named dialect. A statement can carry joins, `GROUP BY`
and `HAVING`, subqueries, common table expressions, window functions,
`RETURNING` and `ON CONFLICT`, where the target has them. See the
[Query builder](../query-builder/) reference for the full API and what each
dialect refuses.

This complete example uses only public packages. The same AST/rendering path is
validated by
[`examples/reusable_components`](https://github.com/stokaro/ptah/tree/master/examples/reusable_components):

```go
package main

import (
	"fmt"
	"log"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
)

func main() {
	table := ast.NewCreateTable("users").
		AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary()).
		AddColumn(ast.NewColumn("email", "TEXT").SetNotNull().SetUnique())

	sql, err := builtin.RenderSQL("postgres", table)
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

`core/astbuilder` writes the same AST as a chain rather than as nested literals.
It produces `core/ast` nodes and nothing of its own, so the two spellings mix,
and a node the builders do not model stays reachable through `core/ast`:

```go
table := astbuilder.NewTable("users").
	Column("id", "SERIAL").Primary().End().
	Column("email", "TEXT").NotNull().Unique().End().
	Build()
```

`NewSchema` builds a whole `*ast.StatementList` in one chain — enums, tables,
indexes, and comments in the order they were added — where `NewTable` and
`NewIndex` build a single statement. The builders do not validate; an unknown
type or an unresolved foreign key is reported by `engine/builtin` or by the
database.

## End-to-end reuse examples

The examples below use only stable public packages unless a block is explicitly
marked as pseudo-code. Complete copy-pasteable versions are kept in
[`examples/reusable_components/reusable_components_test.go` in the latest development source](https://github.com/stokaro/ptah/blob/master/examples/reusable_components/reusable_components_test.go)
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

runtime, err := builtin.New()
if err != nil {
	return err
}
// The runtime's owners decode their own directives, such as a TimescaleDB
// hypertable. annotation.None() reads the frontend's own directives only.
db, err := goschema.ParseFS(runtime.Annotations(), fsys, "models")
if err != nil {
	return err
}
rendered, err := renderer.RenderSchema(ctx, runtime, renderer.SchemaRequest{
	Target: "sqlite", Schema: db, Capabilities: capability.ForDialect("sqlite"),
})
if err != nil {
	return err
}
fmt.Println(rendered.Statements[0])
```

`Target.SchemaRendering` selects a whole-schema provider separately from AST
rendering. It receives the captured declaration once, with target facts and the
caller context. `renderer.RenderSchema` rejects incomplete replies and returns
no output on refusal, service failure, or cancellation. Completed refusals expose
`SchemaRefusalError`; accepted results retain the provider's omission records.

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

runtime, err := builtin.New()
if err != nil {
	return err
}
list, err := atlascompat.SchemaToAST(ctx, runtime, *db, "postgres", capability.ForDialect("postgres"))
if err != nil {
	return err
}
sql, err := builtin.RenderSQL("postgres", list.Statements...)
if err != nil {
	return err
}
fmt.Println(sql)
```

### Render SQL from YAML schema

YAML is one of the authoring formats that produce Ptah's schema IR, alongside
Go annotations, HCL, SQL, and DBML. `core/yamlschema` is its reader: `Parse`
takes the document as bytes, `ParseFile` reads it from a path, and both return
the same `*schemamodel.Database` the other readers return. Nothing downstream
knows which one filled it.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/yamlschema"
	"ptah.run/engine/builtin"
)

func main() {
	db, err := yamlschema.ParseFile("schema.yaml")
	if err != nil {
		log.Fatal(err)
	}

	runtime, err := builtin.New()
	if err != nil {
		log.Fatal(err)
	}
	rendered, err := renderer.RenderSchema(context.Background(), runtime, renderer.SchemaRequest{
		Target: "postgres", Schema: db, Capabilities: capability.ForDialect("postgres"),
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, statement := range rendered.Statements {
		fmt.Println(statement)
	}
}
```

Parsing is strict: an unknown key is an error, and a second YAML document in the
same stream is refused. See [YAML schema](../../schema/yaml/) for the document
format. The equivalent CLI call is:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

When the YAML is written by another program rather than held in a file, use
`core/schemasource` instead: it runs that program and parses its standard output
through the same reader.

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

live, err := conn.Reader().ReadSchemaContext(ctx)
if err != nil {
	return err
}

runtime, err := builtin.New()
if err != nil {
    return err
}

desired, err := goschema.ParseDir(runtime.Annotations(), "./models")
if err != nil {
	return err
}
diff, err := schemadiff.CompareWithDatabase(ctx, conn, desired, live, nil, runtime)
if err != nil {
	return err
}
info := conn.Info()
sql, err := planner.GenerateSchemaDiffSQLWithOptions(
	ctx,
	runtime,
	diff,
	info.Dialect,
	planner.Options{Capabilities: info.Capabilities},
)
if err != nil {
	return err
}
fmt.Println(sql)
```

For unit tests or offline planning, you can build a `catalog.Database`
value directly and pass it to `schemadiff`.

Pass the same selected runtime and context through comparison and planning.
Planning uses its feature services and renderer; a service failure or cancellation
returns no usable SQL prefix. `core/featureplan` defines the contextual planning
batch for provider implementations. Owners receive typed changes, captured parent
state, identifier rules, and target capabilities without opening a database.
Completed refusals carry structured diagnostics without operations. The host
converts them to `featureplan.RefusalError` before lowering the plan. Provider
failures and cancellation return no diagnostics or operations.

For a feature that refers to several objects, register
`engine.Provider.Relations` and call `runtime.CaptureRelations` with complete
captured values and source coverage. Owners return dependency references in one
batch, without reading a database. The resulting
`RelationSnapshot.CaptureRelated(ctx, subjects)` keeps each multi-table value
intact and includes other values connected through its dependencies. Unknown
namespaces or unresolved references return `schemaext.ErrIncompleteRelations`.

The result is context for owner assessment. It does not authorize changing its
prerequisites or expanding the user's selected scope. Built-in scope selection
and comparison capture these relations themselves. A scope that selects some of
the tables an object binds, and not the others, is refused. So is a plan that
drops a table, column or function a standalone feature object of the desired
state still binds.
Independent providers can use the public batch and capture contracts without
importing built-in engines.

Pass the same `context.Context` and selected runtime to `safety.AssessRendered` or
`AssessRenderedWithCapabilities` as well. Safety renders the assessment units
in one batch and keeps each statement associated with its source operation.
An extension's unknown effects still require manual review when it produces
several SQL statements. Migration generation uses the same selected renderer
for assessment, forward SQL, and rollback SQL.

`migration/safety.ClassifySchemaDiff` also accounts for owned changes before
planning. It reads the change owner's `schemaext.EffectSource` metadata and
reports each kind under `feature_changes:<kind>`. Missing or invalid metadata
requires the strongest review. Removing a coordination node is destructive
because it also removes application semaphores and rate limiter resources.
Removing a YDB secret is destructive because nothing can read its value back.

An owner whose changes affect what roles may read or write also implements
`schemaext.AccessEffectSource` on its change and operation payloads. The verb
does not decide the direction: creating a policy can widen access and dropping
one can narrow it, so the owner assesses each change against the enforcement
state and sibling objects it captured, and stores the result in the payload.
Embed `schemaext.AccessEffectSchema()` in the codec definition. A payload that
implements the interface without a valid assessment is refused at every codec
boundary, so the assessment cannot be dropped on the way to a report. Safety
reports print it beside the statement, and diff findings count it under
`feature_access_widened:<kind>`, `feature_access_narrowed:<kind>`,
`feature_access_unchanged:<kind>` or `feature_access_unknown:<kind>`; a
widening or unknown effect is destructive, a narrowing is a warning.

Plan each owner operation as a node of its own. Planning refuses a node that
holds one beside other work with `migration/planner.ErrInvalidPlan`, because a
report could not tell which statements the operation wrote. Each statement of
a saved plan keeps the node that rendered it, so an owner's verdict reaches its
own statements and no others.

YDB inspection records unknown coordination settings as incomplete subject
coverage. Go and HCL export refuse those limits before writing output, because
these formats cannot carry the captured inspection limit. Exporting an empty
node list would otherwise turn unknown configuration into declared absence.

Offline target-aware comparison requires `schemadiff.TargetRuntime`, including
its selected validation service. Live comparison requires
`schemadiff.DatabaseRuntime`, adding selected AST rendering for normalization
probes. Completed rendering refusals leave normalization unresolved; service
failures abort comparison. Pure catalog comparison accepts
`schemapreparation.Runtime`. Comparing source documents requires
`schemadiff.DocumentRuntime`, which also predicts the current document's CREATE
result through the selected provider. This prediction preserves explicit source
knowledge limits and establishes no live inspection.
Validation sends the whole captured schema with target facts in one call.

`runtime.ResolveTarget(name)` returns an immutable `schemaext.TargetSelection`.
It resolves only explicitly registered names and aliases. Use that snapshot with
`schemamodel.ScopeToTarget` or `OmissionsForTarget`; unresolved selections are
errors. An empty declaration scope includes the selected target. A named scope
matches its registered spellings, including aliases. Whole-schema validation and
rendering apply this projection before codec checks. Comparison also suppresses
matching observed objects, so exclusion cannot become a requested drop.

This supports custom target scopes in typed declarations. Go annotation and YAML
scope parsing still recognize built-in names only. `schemavalidation.Runtime`
combines validation and target resolution for consumers that project declarations
before checking them.

`Target.Validation` selects a `schemavalidation.Service`; leaving it absent
refuses validation. A completed reply contains data diagnostics, while missing
completion, service failure, and cancellation return errors. `NoSkipped` also
asks the provider to diagnose omitted declarations. Migration generation uses
the same validator for the desired schema and captured rollback target before
publishing files.

AST rendering replies must set `Complete`, even for an empty batch. Accepted
replies account for every input node in `Fragments`. Completed refusals carry
`Diagnostics` and no output. A diagnostic can name its input node by index;
`BatchRefusalError` retains that data and exposes the caller's node through the
local error chain. Service failures remain errors rather than refusal data.

Generated-expression probes and inference-store creation also use selected AST
rendering. They require the whole batch before database execution and reject
reported omissions or empty output. Inference-store creation uses an explicit
PostgreSQL baseline profile for its internal tables and indexes.

Index names are table-scoped in some dialects. Use `diff.IndexAdditions()` and
`diff.IndexRemovals()` when consuming index changes through a copied slice, or
read the canonical `IndexesAdded` and `IndexesRemoved` `[]IndexRef` fields
directly. Every reference includes its owning table.

Planning rejects missing owners, unresolved additions, and same-name target
indexes that conflict in the selected dialect's namespace. When a custom
consumer starts from `schemamodel.Index` values, use
`schemamodel.ResolveIndexTableNames` to resolve all owning tables in one
indexed pass instead of scanning the table list for each index. MySQL and
SQLite index matching applies ASCII case folding.

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

`CompareWithOptions` also refuses an invalid, incomplete, or collision-prone
explicit snapshot. Every comparison takes a context and the selected feature
runtime, and returns an error when its inputs or a provider fail. A caller must
handle that error before using the diff.

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
[Lint and gate unsafe SQL](../../versioned/lint/).

### Use capabilities

Use capabilities when syntax depends on a dialect version rather than only a
dialect family.

```go
caps := capability.ForServerVersion("postgres", "17.0")
table := ast.NewCreateTable("accounts").
	AddColumn(ast.NewColumn("id", "INTEGER").
		SetIdentity("BY_DEFAULT", "1", "1").
		SetPrimary())

sql, err := builtin.RenderSQLWithCapabilities("postgres", caps, table)
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
Stable packages: `core/goschema`, `atlascompat`, `catalog`,
`migration/schemadiff`, `core/platform/capability`.
The host tool keeps output formatting; generate from the stable schema IR, not
internal renderers.

**Atlas-compatible transition** — start from
[Embed the migrator](#embed-the-migrator).
Stable packages: `atlascompat`, `migration/migrator`, `engine/builtin`.
The host tool keeps parity expectations; use the conformance reports for
measured compatibility.

**Dialect extension research** — start from [Use capabilities](#use-capabilities).
Stable packages: `core/platform/capability`, `core/ast`, `engine/builtin`,
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

Unsupported public APIs stay out of this reference. Create a follow-up issue
before exposing:

- a stable Atlas HCL renderer package beyond `atlascompat` wrappers;
- snippet validation that extracts docs code blocks automatically;
- out-of-tree dialect, planner, renderer, or lint-rule extension points.
