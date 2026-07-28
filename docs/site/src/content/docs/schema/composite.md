---
title: Composite desired schema
description: Merge Go packages, YAML, HCL, and SQL files, and an external loader into one desired schema.
---

Assemble one desired schema from several sources. `--root-dir` and
`--schema-file` are repeatable, mix freely, and merge with a configured
external loader, so a Go package for application tables, a vendored HCL file
for third-party tables, and a YAML file for shared lookups become a single
schema. This page is the canonical description of how sources merge; the
per-source pages link here instead of restating the rules.

## Combine sources

Several Go packages — a shared `common` package plus per-service tables, for
example — merge into one schema. Every root is parsed, merged, and finalized
together, so a table in one root can reference a table in another:

```bash
ptah schema render \
  --root-dir ./common \
  --root-dir ./services/orders \
  --dialect postgres
```

Formats mix freely, and file sources join the same merge:

```bash
ptah schema render \
  --root-dir ./models \
  --schema-file ./vendor/thirdparty.hcl \
  --schema-file ./shared/lookups.yaml \
  --dialect postgres
```

An [external loader](../orm-and-external/) composes the same way through
`--schema-cmd` or the `external_schema` config block.

The same repeatable sources work on `ptah schema compare`,
`ptah schema drift`, `ptah migrations plan`, and `ptah migrations generate`,
so a composite schema diffs, drift-checks, and migrates exactly like a
single-source one.

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

Treat each repeatable `--root-dir` and `--schema-file` value, plus the
selected `--schema-cmd` when present, as one ownership boundary. Ptah applies
the same strict database-identity conflict checks inside a Go root and across
source boundaries, while each boundary keeps its own Go type namespace:

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
- Generating migrations from the merged schema? [Migrations](../../workflows/migrations/).
- Reviewing what the merge produced? [Visualize the schema](../visualize/).
