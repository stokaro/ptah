---
title: Schema visualization example
description: Render schema diagrams as Mermaid, DOT, or SVG.
---

Ptah includes a checked-in visualization example at [`examples/viz`](https://github.com/stokaro/ptah/tree/master/examples/viz).

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
