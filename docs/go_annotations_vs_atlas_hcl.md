# Go Annotations vs. Atlas HCL

Ptah can use both Go source annotations and Atlas schema HCL as schema sources.
It can also export Go annotations to Atlas schema HCL:

```bash
ptah schema export --from go --to atlas-hcl --root-dir ./models --out schema.hcl
```

This export path is an IR conversion. Ptah parses Go annotations into the
`goschema.Database` intermediate representation, then renders that IR as Atlas
schema HCL. It does not rewrite annotation comments directly into HCL text.

## When to Use Each Format

Use Go annotations when the Go model types remain the primary schema source:

- schema metadata lives next to the model fields it describes
- embedded structs are expanded into concrete columns during parsing
- platform overrides can remain attached to fields and schema objects

Use Atlas HCL when a language-neutral schema file should become the primary
source:

- schema review can happen without reading Go source
- generated SQL can be compared against Atlas-style schema files
- the same schema file can be shared with tools that understand Atlas HCL

## Exported Schema Shape

The exporter writes deterministic Atlas HCL for the schema subset that maps
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

Unsupported or lossy details are reported as export diagnostics on stderr.
Examples include platform-specific overrides and raw SQL-backed objects such as
functions, triggers, views, materialized views, RLS policies, roles, grants, and
extensions. These warnings are intentional; the exporter must not silently drop
schema intent.

## Cleanup Mode

After a successful export, Ptah can remove Go schema annotations:

```bash
ptah schema export \
  --from go \
  --to atlas-hcl \
  --root-dir ./models \
  --out schema.hcl \
  --cleanup-go-annotations
```

Cleanup removes only Ptah schema annotation comments:

- `//migrator:schema:*`
- `//migrator:embedded ...`

It preserves regular Go comments, formats cleaned Go files with `gofmt`, and
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

The exporter targets the Atlas schema HCL subset documented in
[Atlas HCL Schema Input](atlas_hcl_schema.md). Objects that need their own
structural model, parser, or renderer are not emitted as best-effort SQL blobs.
They produce diagnostics instead so the caller can decide whether the exported
HCL is complete enough to replace the original Go annotations.
