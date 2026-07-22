# Go Annotations vs. HCL Schema

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

## When to Use Each Format

Use Go annotations when the Go model types remain the primary schema source:

- schema metadata lives next to the model fields it describes
- embedded structs are expanded into concrete columns during parsing
- platform overrides can remain attached to fields and schema objects

Use HCL schema files when a language-neutral schema file should become the
primary source:

- schema review can happen without reading Go source
- generated SQL can be compared against Atlas-compatible schema files
- the same schema file can be shared with tools that understand the supported
  Atlas HCL schema language subset

Use export as a one-time migration path when a project wants to move from
app-schema authoring to declarative schema authoring without manually rewriting
its existing Ptah metadata.

## Exported Schema Shape

The exporter writes deterministic HCL for the schema subset that maps
directly from Ptah's IR:

- schemas
- enums
- tables and concrete columns, including columns from embedded Go structs
- primary keys
- indexes, including uniqueness, predicates, include columns, and supported
  index part metadata
- unique constraints
- foreign keys from both field annotations and table constraints
- check constraints from both field annotations and table constraints
- defaults and default SQL expressions
- generated and identity columns where represented by the IR
- PostgreSQL extensions with version and comments
- PostgreSQL roles
- PostgreSQL grants as Atlas `permission` blocks for table and schema targets
- PostgreSQL functions with body, language, return type, simple argument
  blocks, security, volatility, and comments
- PostgreSQL views and materialized views with query bodies and comments
- PostgreSQL triggers with timing/event blocks, `for`, body, and comments
- PostgreSQL row-level security enablement as `table.row_security`
- PostgreSQL row-level security policies

Unsupported or lossy details are reported as export diagnostics on stderr.
Examples include platform-specific overrides, table custom SQL, extension
`if_not_exists`, role passwords, grantor metadata, non-manual
materialized-view refresh strategies, RLS enablement comments, and function
parameter strings that cannot be split into Atlas `arg` blocks without losing
meaning. These warnings are intentional; the exporter must not silently drop
schema intent.

## Cleanup Mode

After a successful export, Ptah can remove Go schema annotations:

```bash
ptah schema export \
  --from go \
  --to hcl \
  --root-dir ./models \
  --out schema.hcl \
  --cleanup-go-annotations
```

Cleanup removes only Ptah schema annotation comments:

- `//migrator:schema:*`
- `//migrator:embedded ...`

It preserves regular Go comments, leaves unrelated formatting untouched, and
keeps original file permissions. Cleanup is idempotent: running it again after
annotations were removed should report no changed files.

Use dry-run or diff mode before modifying source files:

```bash
ptah schema export --root-dir ./models --out schema.hcl \
  --cleanup-go-annotations --cleanup-dry-run

ptah schema export --root-dir ./models --out schema.hcl \
  --cleanup-go-annotations --cleanup-diff
```

Both dry-run and diff mode require `--cleanup-go-annotations`. Cleanup starts
only after the HCL file has been rendered and written successfully.

## Current Limits

The exporter targets the HCL schema subset documented in
[HCL Schema Input](atlas_hcl_schema.md). Objects that need their own
structural model, parser, or renderer are not emitted as best-effort SQL blobs.
They produce diagnostics instead so the caller can decide whether the exported
HCL is complete enough to replace the original Go annotations.

Some PostgreSQL object blocks documented by Atlas are gated by Atlas plans at
runtime. Ptah's guarantee here is IR preservation through Atlas-compatible HCL
input and export; it does not change which Atlas CLI features are available in
Atlas OSS or Pro.
