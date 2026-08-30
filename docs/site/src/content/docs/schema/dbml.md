---
title: DBML
description: Read a DBML document as a desired schema, and write any Ptah schema back out as canonical DBML.
type: how-to
audience:
  - "schema-author"
readerQuestion: "How do I read a DBML document as a desired schema, and write any Ptah schema back out as canonical DBML?"
goal: "Round-trip a desired schema through canonical DBML."
sourceOfTruth:
  - "cmd/schema"
  - "internal/schemaload"
generated: false
overlaps: []
disposition: keep
sourceMode: static-file-only
---

[DBML](https://dbml.dbdiagram.io/) is a compact way to write down tables,
columns, enums, indexes and relationships. Ptah reads it as a desired schema and
writes it back out, so a diagram-as-code document and a database can be the same
thing rather than two descriptions that drift.

```text
DBML  ->  Ptah  ->  reviewed migration plan  ->  database
database  ->  Ptah  ->  DBML
```

DBML is a format adapter around Ptah's own schema model. It is not a second
planner, there is no DBML-to-SQL shortcut, and nothing here runs Node.js or a
subprocess — the grammar is read in Go.

## Read a document

A `.dbml` file is a schema source like any other, so every command that takes
`--schema-file` accepts one:

```bash
ptah schema validate --schema-file ./schema.dbml --dialect postgres
ptah schema render   --schema-file ./schema.dbml --dialect postgres
ptah schema plan     --schema-file ./schema.dbml --db-url "$DATABASE_URL"
ptah migrations generate --schema-file ./schema.dbml --db-url "$DATABASE_URL"
```

## Write one

```bash
ptah schema export  --schema-file ./schema.hcl --to dbml --out ./schema.dbml
ptah schema inspect --db-url "$DATABASE_URL" --format dbml > schema.dbml
```

Both write the same canonical form: LF endings, one trailing newline, and an
order that comes from the schema rather than from a map. Two exports of one
schema are the same bytes, so a checked-in `.dbml` file diffs cleanly.

Columns keep the order they were declared in. Enums, tables, indexes and
references are sorted by identity — the order a schema states for its columns is
part of what it says, and the others carry no order to preserve.

## What a document can say

| DBML | Ptah |
| --- | --- |
| `Table`, with `schema.name` | a table, in that schema |
| a column, with its type | a column |
| `pk`, `increment`, `unique`, `not null` | the matching column property |
| `default: 'text'` | a literal default |
| ``default: `expr` `` | an expression default |
| `note:` and `Note:` | a column or table comment |
| `Indexes { … }` with `unique`, `type`, `name` | an index |
| `Ref` and `[ref: > table.column]` | a foreign key, with `delete:` / `update:` |
| `Enum` | an enum type |

A **literal default and an expression default stay apart** in both directions.
`default: 'now()'` is the six-character string; ``default: `now()` `` is the
call. They are different columns, and Ptah keeps them different.

## What it cannot

DBML describes tables, columns, enums, indexes and references. It has no syntax
for views, functions, triggers, sequences, domains, composite types, ranges,
policies, roles, extensions, synonyms, extended properties, hypertables,
continuous aggregates or virtual tables.

That has a consequence worth understanding before you apply a DBML document to
an existing database: **Ptah records those families as not described**, so a
sequence or a policy the database already holds is left alone rather than read
as something the document asked to remove. Silence in a format that cannot speak
is not intent.

Going the other way, an export names what it left behind:

```console
$ ptah schema export --schema-file ./schema.hcl --to dbml --out ./schema.dbml
warning: DBML cannot express views (2); the export leaves them out
warning: DBML cannot express triggers (1); the export leaves them out
```

The warnings go to standard error, so redirecting the document to a file keeps
the document clean.

## What is refused, and why

A **many-to-many relationship** — `Ref: a.id <> b.id` — has no foreign key
behind it. A database expresses one with a join table, and Ptah will not invent
a table the document never declared. Declare the join table and two references
to it.

**`use`, `reuse`, `include` and `import`** are refused. Ptah reads one document;
a directive that pulls in another describes a schema it has not read, and
carrying on would hand back a model missing whatever that file declared —
silently, and looking complete.

An **unsupported setting** on a column, an index or a reference is refused
rather than ignored. A property Ptah does not implement is one it would apply
differently than the document reads, and dropping the difference quietly is how
a declared property disappears from a database.

## What is read and not applied

Some DBML describes the diagram rather than the database. Ptah reads it, does
not apply it, and says so:

```text
warning: schema file /path/to/schema.dbml:1:1: project describes the diagram
rather than the database, and is not applied
```

`Project` and `TableGroup` are in that group. So are `Records`, and for a
sharper reason: they are seed rows, and Ptah has
[reference data](../../versioned/reference-data/) with its own declaration, keys
and safety gates. Turning diagram rows into rows a migration writes would apply
data nobody asked to have applied, so Ptah reports the block and leaves it —
once per block, not once per row.

## Diagnostics

A syntax or binding error names the file, the line and the column:

```console
$ ptah schema validate --schema-file ./schema.dbml --dialect postgres
postgres: source: error parsing schema file: /path/to/schema.dbml:3:1: unsupported column setting "unlock_everything"
```

## Related

- [Work with a source](../work-with-a-source/) — how `--schema-file` picks a format.
- [API schema export](../export/) — the OpenAPI and GraphQL targets.
- [Visualize a schema](../visualize/) — diagrams from the same model.
