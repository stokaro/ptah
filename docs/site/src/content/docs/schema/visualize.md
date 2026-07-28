---
title: Visualize the schema
description: Render entity-relationship diagrams from the desired schema as Mermaid, DOT, or SVG with ptah viz.
---

Render an entity-relationship diagram of the desired schema. `ptah viz` scans
Go annotations and writes Mermaid `erDiagram`, Graphviz DOT, or SVG output to
stdout, so diagrams live next to the models that define them.

Prerequisites: a built `ptah` binary, and the Graphviz `dot` binary only for
`--format svg`. Mermaid and DOT output need no extra tools.

## Render a diagram

From a Ptah checkout, the committed example models in `examples/viz/models`
give a schema with several relationship shapes:

```bash
ptah viz --root-dir examples/viz/models --format mermaid --include-columns
```

Expected output includes:

```text
erDiagram
  organizations {
    INTEGER id PK
    TEXT name
    TEXT slug
  }
  tags {
    INTEGER id PK
    INTEGER organization_id FK
    TEXT name
  }
```

Point `--root-dir` at your own annotated models the same way. `mermaid` is the
default format, so `ptah viz --root-dir ./models` is the shortest useful call.

## Choose an output format

| Format | Use when |
| --- | --- |
| Mermaid | You want Markdown-friendly diagrams. |
| DOT | You want Graphviz source for another renderer. |
| SVG | You want a committed image artifact. |

```bash
ptah viz --root-dir ./models --format dot --include-columns > schema.dot
ptah viz --root-dir ./models --format svg --include-columns --theme dark > schema.svg
```

DOT output starts with `digraph ptah_schema`. SVG output shells out to
Graphviz `dot`; `--theme` selects `light` (default) or `dark` colors.

## Shape the diagram

- `--include-columns` adds each table's columns with primary-key and
  foreign-key markers. Without it, the diagram shows only tables and
  relationships.
- `--exclude-tables` takes comma-separated table names to omit, which keeps
  join tables or audit tables from drowning the interesting edges:

```bash
ptah viz --root-dir ./models --exclude-tables task_comments,task_tags
```

## The committed example

[`examples/viz`](https://github.com/stokaro/ptah/tree/master/examples/viz)
keeps generated artifacts in the repository so diagram output stays reviewable:

| File | Purpose |
| --- | --- |
| `schema.sql` | The SQLite input schema. |
| `models/schema.go` | Annotated Go model generated from it by `ptah introspect`. |
| `schema.mmd` | Mermaid `erDiagram` output. |
| `schema.dot` | Graphviz DOT output. |
| `schema.svg` | Dark-theme SVG rendered through Graphviz. |

After regenerating, compare the committed artifacts:

```bash
git diff -- examples/viz/schema.mmd examples/viz/schema.dot examples/viz/schema.svg
```

The example should show a connected schema with readable table names,
relationships, and columns. A diagram that renders but loses relationships is
a bug in the visualization path, not an acceptable example.

## Failure modes

- `--format svg` without Graphviz installed fails with
  `Graphviz dot is required for --format svg; install graphviz or use --format dot`.
  See [Troubleshooting](../../operate/troubleshooting/).

## Limitations

- `ptah viz` reads Go annotations only (`--root-dir`); it does not accept
  `--schema-file` or a database URL. To diagram a live database or a SQL
  schema file, generate annotated models first with `ptah introspect` — the
  committed example does exactly that.

## Next steps

- Diagramming a database that exists outside Ptah? [Adopt an existing database](../../start/adopt-an-existing-database/) covers `ptah introspect`.
- Modeling the schema the diagram should follow? [Go annotations](../go-annotations/).
- Projecting the schema into API formats instead? [API schema export](../export/).
