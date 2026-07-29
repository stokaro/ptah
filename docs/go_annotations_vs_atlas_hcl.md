# Go annotations vs. HCL schema

Ptah can use both Go source annotations and HCL schema files as schema sources.
It can also export Go annotations to HCL schema:

```bash
ptah schema export --from go --to hcl --root-dir ./models --out schema.hcl
```

This export path is an IR conversion. Ptah parses Go annotations into the
`goschema.Database` intermediate representation, then renders that IR as HCL
schema. It does not rewrite annotation comments directly into HCL text.

Go annotations remain a first-class Ptah workflow for Go applications. The
exporter is an escape hatch for projects that outgrow app-owned Go annotations
or intentionally move to a schema-first workflow. It is not a required final
state for every Go project. If the Go service owns the schema and the annotation
model remains expressive enough, keeping Go annotations as the source of truth
is a supported Ptah workflow.

## When to use each format

Use Go annotations when the Go model types remain the primary schema source:

- schema metadata lives next to the model fields it describes
- embedded structs are expanded into concrete columns during parsing
- platform overrides can remain attached to concrete fields and tables

Use HCL schema files when a language-neutral schema file should become the
primary source:

- schema review can happen without reading Go source
- generated SQL can be compared against Atlas-compatible schema files
- the same schema file can be shared with tools that understand the supported
  Atlas HCL schema language subset

Use an external desired-schema command when an ORM, framework, or generator
already owns the model. Ptah accepts SQL, HCL, or YAML stdout and can use the
same `ptah.yaml external_schema` block for native render, comparison, drift, and
migration planning. Config-sourced program execution requires
`--allow-external-schema`; an explicit `--schema-cmd` is already an opt-in. See
[ORM and external loaders](site/src/content/docs/schema/orm-and-external.md) and
[Composite desired schema](site/src/content/docs/schema/composite.md).

Use export as a one-time migration path when a project wants to move from
app-schema authoring to declarative schema authoring without manually rewriting
its existing Ptah metadata.

## Exported schema shape

The exporter writes deterministic HCL for every schema semantic accepted by the
Go annotation parser:

- schemas
- enums
- tables and concrete columns, including columns from embedded Go structs
- table checks, custom CREATE TABLE SQL, and table platform overrides
- column checks, inline enums, generated and identity metadata, multi-word SQL
  types, and column platform overrides
- primary keys and named `PRIMARY KEY` constraints
- indexes, including uniqueness, predicates, include columns, comments,
  ClickHouse data-skipping granularity, operator classes, and supported index
  part metadata
- named `CHECK`, `UNIQUE`, `FOREIGN KEY`, `PRIMARY KEY`, and `EXCLUDE`
  constraints with their complete Ptah IR metadata and comments
- foreign keys from both field annotations and table constraints, including
  composite referenced columns
- defaults and default SQL expressions
- PostgreSQL extensions with `if_not_exists`, version, and comments
- standalone PostgreSQL sequences and sequence grants
- PostgreSQL domains, composite types, and ranges
- PostgreSQL roles, including passwords
- PostgreSQL grants for table, schema, and sequence targets
- PostgreSQL functions with body, language, return type, security, volatility,
  comments, and either Atlas-style argument blocks or lossless raw `params`
- PostgreSQL views and materialized views with query bodies, comments, and
  materialized-view refresh strategies (`refresh_strategy`)
- PostgreSQL triggers with timing/event blocks, `for`, body, and comments
- PostgreSQL row-level security enablement as `table.row_security`, including its
  optional comment
- PostgreSQL row-level security policies
- managed data declarations, including their table, keys, and data-file path

The generated file may use Ptah HCL extensions in addition to the
Atlas-compatible subset. These extensions are deliberate: they preserve
annotation semantics that cannot be represented by the corresponding Atlas
schema block without loss. A valid Go annotation source is expected to export
without diagnostics. Diagnostics indicate incomplete or orphaned IR that was
constructed outside the validated annotation parser and must never be silently
dropped.

Role passwords are written to HCL as string values. Treat an export containing
passwords as sensitive. Ptah writes such exports with owner-only `0600`
permissions. Do not commit them unless that is appropriate for the repository.

## Cleanup mode

After a successful export, Ptah can remove Go schema annotations:

```bash
ptah schema export \
  --from go \
  --to hcl \
  --root-dir ./models \
  --out schema.hcl \
  --cleanup-go-annotations
```

Cleanup is a one-time source migration. Before Ptah writes the HCL file or
changes Go source, it verifies all of these conditions:

- the cleanup plan contains at least one real Ptah annotation
- the output path does not alias a Go source or referenced managed-data file
- the HCL renderer reports no lossy or unsupported details
- the rendered HCL parses and remains byte-stable after canonical re-rendering

Cleanup then removes only Ptah schema annotation comments:

- `//migrator:schema:*`
- `//migrator:embedded ...`

It preserves regular Go comments, leaves unrelated formatting untouched, and
keeps original file permissions. Ptah writes the validated HCL output before it
applies the prevalidated source cleanup plan. A failed output write therefore
leaves every Go source unchanged.

Do not repeat the cleanup command after the annotations are gone. Ptah rejects
that run with `no Ptah Go annotations found to export and clean` before it can
replace the previous HCL file with an empty schema.

Use dry-run or diff mode before modifying source files:

```bash
ptah schema export --root-dir ./models --out schema.hcl \
  --cleanup-go-annotations --cleanup-dry-run

ptah schema export --root-dir ./models --out schema.hcl \
  --cleanup-go-annotations --cleanup-diff
```

Both dry-run and diff mode require `--cleanup-go-annotations`. They write the
validated HCL export but do not modify Go source. Any unexpected export
diagnostic blocks all three cleanup modes with
`refuse to clean Go annotations after a lossy HCL export`; run without cleanup
to inspect the HCL and its diagnostics.

## Current limits

The exporter targets the Atlas-compatible subset and Ptah parity extensions
documented in [HCL Schema Input](atlas_hcl_schema.md). Go-only provenance such
as struct and field names is not schema intent and is intentionally omitted.
Embedded annotations are exported as their finalized concrete columns and
foreign keys rather than as Go embedding metadata.

HCL can model additional schema semantics beyond Go annotations. Parity is
one-way: every valid Go annotation semantic has a lossless HCL representation,
but not every accepted HCL construct has a Go annotation spelling.

Some PostgreSQL object blocks documented by Atlas are gated by Atlas plans at
runtime. Ptah's guarantee here is IR preservation through Atlas-compatible HCL
input and export; it does not change which Atlas CLI features are available in
Atlas OSS or Pro.
