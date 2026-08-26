# Ptah Docs Site

This directory contains the Astro + Starlight documentation site.

```bash
npm ci
ASTRO_TELEMETRY_DISABLED=1 npm run check:links:selftest
ASTRO_TELEMETRY_DISABLED=1 npm run check:links
ASTRO_TELEMETRY_DISABLED=1 npm run check:core-doc-links
ASTRO_TELEMETRY_DISABLED=1 npm run check:page-health
ASTRO_TELEMETRY_DISABLED=1 npm run check:exit-codes
ASTRO_TELEMETRY_DISABLED=1 npm run build
npm run versions:selftest
npm audit --audit-level=low
```

For local development:

```bash
ASTRO_TELEMETRY_DISABLED=1 npm run dev
```

The site is versioned by `DOCS_VERSION`; `edge` tracks `master`.
The docs workflow runs internal link validation, core-reference link checks,
page-health checks, and exit-code reference validation before building `edge`
and before building released tags that include the relevant checker scripts.
Older historical tags without those checkers are still built with a warning.

## Brand assets

Two files carry the Ptah mark, and they hold the same artwork:

- `src/assets/logo.svg` — the site header, referenced from `astro.config.mjs`.
- `public/favicon.svg` — the browser tab.

`logo.svg` additionally carries `role="img"` and a `<title>` for screen readers.
`favicon.svg` does not, because a tab icon is announced by the page title.

### The mark

Three stacked courses on a rounded slate tile, widening downward, with the top
course in amber. Ptah is the builder god, so the mark is something built; the
amber course is the one being set, which is what a schema tool does to a
database. The geometry is six rectangles on a 64×64 grid — no paths, no
gradients, no text — so it stays legible when the browser rasterizes it to 16
CSS pixels.

Palette, matching the docs theme:

| Role | Hex |
| --- | --- |
| Tile | `#0f172a` |
| Courses | `#38bdf8` |
| Top course | `#f59e0b` |

### Editing the mark

Keep the two files identical apart from the accessibility attributes, and keep
course gaps at 3 grid units or wider. Below that the courses merge at 16 px and
the mark becomes a blob — this is the failure mode to check for, not a matter of
taste.

Verify a change the way the mark was chosen: rasterize the SVG at 16 and 32 CSS
pixels at a device pixel ratio of 1, then magnify the resulting bitmap with
`image-rendering: pixelated`. Playwright is already a dependency of this site
for `check-responsive`, so no new tooling is needed. Rendering at a higher
device pixel ratio and scaling down flatters small sizes and hides exactly the
defect worth finding.

Directions that were rendered and rejected, so they are not re-derived:

- **A djed pillar**, Ptah's own emblem: a shaft with crossbars reads as a cross
  or a utility pole, and moving the bars to the top of the shaft reads as a
  capital `I`. Both are the ambiguity this mark exists to avoid.
- **Four courses instead of three**: the courses merge at 16 px and the
  silhouette becomes a cone.
- **A displaced top course**: at 16 px the offset reads as a stray dot rather
  than as movement.
- **A table grid**, header band over a 2×2 body: legible at 32 px and clearer
  about the subject matter, but its rows blur together at 16 px. This is the
  runner-up, and the one to revisit if the mark is ever reconsidered.
