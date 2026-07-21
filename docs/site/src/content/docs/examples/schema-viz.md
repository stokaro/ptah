---
title: Schema visualization example
description: Render schema diagrams as Mermaid, DOT, or SVG.
---

Ptah includes a checked-in visualization example at
[`examples/viz`](https://github.com/stokaro/ptah/tree/master/examples/viz). Use
it to verify that diagram output is useful, not just syntactically generated.

The example contains:

| File | Purpose |
| --- | --- |
| `models/schema.go` | Annotated Go model with several related tables. |
| `schema.sql` | SQL rendered from the model. |
| `schema.mmd` | Mermaid `erDiagram` output. |
| `schema.dot` | Graphviz DOT output. |
| `schema.svg` | SVG rendered through Graphviz. |

Rebuild the outputs from the repository root:

```bash
ptah schema render \
  --root-dir ./examples/viz/models \
  --dialect postgres \
  > ./examples/viz/schema.sql

ptah viz \
  --root-dir ./examples/viz/models \
  --format mermaid \
  --include-columns \
  > ./examples/viz/schema.mmd

ptah viz \
  --root-dir ./examples/viz/models \
  --format dot \
  --include-columns \
  > ./examples/viz/schema.dot

ptah viz \
  --root-dir ./examples/viz/models \
  --format svg \
  --include-columns \
  > ./examples/viz/schema.svg
```

SVG output requires the Graphviz `dot` binary. If Graphviz is unavailable, generate Mermaid or DOT and render it with another tool.

## Inspect the result

After rebuilding, compare the committed artifacts:

```bash
git diff -- examples/viz/schema.sql examples/viz/schema.mmd examples/viz/schema.dot examples/viz/schema.svg
```

The example should show a connected schema with readable table names,
relationships, and columns. A diagram that renders but loses relationships is a
bug in the visualization path, not an acceptable example.

## Choose an output format

| Format | Use when |
| --- | --- |
| Mermaid | You want Markdown-friendly diagrams. |
| DOT | You want Graphviz source for another renderer. |
| SVG | You want a committed image artifact. |

Use `--exclude-tables` to trim noisy tables and `--include-columns` when the
diagram is meant to document table shape rather than only relationships.
