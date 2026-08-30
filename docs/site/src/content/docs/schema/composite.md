---
title: Composite desired schema
description: Merge static schema files, Go annotations, and an external loader into one desired schema.
type: how-to
audience:
  - "schema-author"
readerQuestion: "How do I merge several schema sources into one desired schema?"
goal: "Build and verify one desired schema from multiple sources."
sourceOfTruth:
  - "cmd/schema"
  - "internal/schemaload"
generated: false
overlaps: []
disposition: keep
sourceMode: source-neutral
---

Assemble one desired schema from several ownership boundaries. Repeat
`--schema-file` for static files, add `--root-dir` for Go annotations when the
project uses them, or add one external loader. Ptah merges every selected
source before rendering or connecting to a database. This page owns the merge
rules; source-specific pages link here instead of restating them.

## Combine sources

The first complete form uses three static sources and no Go toolchain. Each
file owns different objects, and references may cross file boundaries:

```bash
ptah schema render \
  --schema-file ./schema.sql \
  --schema-file ./shared.yaml \
  --schema-file ./vendor.hcl \
  --dialect postgres
```

The same repeated sources work on `ptah schema compare`, `ptah schema drift`,
`ptah migrations plan`, and `ptah migrations generate`, so the merged schema
diffs, drift-checks, and migrates like one source.

Go annotation roots join the same merge when a project uses them:

```bash
ptah schema render \
  --root-dir ./models \
  --schema-file ./vendor/thirdparty.hcl \
  --schema-file ./shared/lookups.yaml \
  --dialect postgres
```

An [external loader](../orm-and-external/) composes the same way through
`--schema-cmd` or the `external_schema` config block.

## How sources merge

Sources are merged and finalized together. At source boundaries, Ptah checks
every named object by its database identity: schema-qualified names where the
object supports schemas, table-qualified names for columns, indexes,
constraints, triggers, and row-level security (RLS) policies, and global names
for extensions, functions, enums, and roles. Identical definitions are
deduplicated even when their parser-only Go names differ.

If the same identity has different desired properties, Ptah stops before
rendering or connecting to a database. Two file sources (or a file source and
a Go root) that disagree fail with:

```text
error merging composite schema: conflicting field "id" definitions on table "users"
```

Two Go roots that disagree fail during parsing with the same identity detail:

```text
error parsing packages: conflicting field "id" definitions on table "users"
```

The conflict rules cover tables, columns, indexes, constraints, enums,
extensions, functions, sequences, user-defined types, views, triggers, RLS
objects, and roles. Ptah resolves table-scoped identities before comparing
definitions, so different Go struct names cannot hide a database-object
conflict.

## Source boundaries and type ownership

Treat each repeatable `--root-dir` and `--schema-file` value, plus the selected
`--schema-cmd` when present, as one ownership boundary. Ptah applies the same
strict database-identity conflict checks inside and across boundaries.

Go roots also keep separate type namespaces:

- Two roots may use the same Go type name for different schema-qualified
  tables without mixing their columns.
- Source-local embedded helper types are scoped per root, including nested
  helpers, so two roots may each define a `Metadata` helper without either
  table receiving the other root's fields.
- A single recursively scanned root remains one type namespace.
- Managed-data annotations retain the absolute directory of their declaring Go
  source, so equal relative `file=` paths in different roots load the correct
  row files after the merge.

## Relation to Atlas

This is Ptah's open, local, no-account equivalent of Atlas's Pro-only
`composite_schema` data source.

## Next steps

- Feeding an ORM into the merge? [ORM and external loaders](../orm-and-external/).
- Generating migrations from the merged schema? [Generate migrations](../../versioned/generate/).
- Reviewing what the merge produced? [Visualize the schema](../visualize/).
