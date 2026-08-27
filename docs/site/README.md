# Ptah Docs Site

This directory contains the Astro + Starlight documentation site.

```bash
npm ci
ASTRO_TELEMETRY_DISABLED=1 npm run check:links:selftest
ASTRO_TELEMETRY_DISABLED=1 npm run check:links
ASTRO_TELEMETRY_DISABLED=1 npm run check:route-retirement:selftest
ASTRO_TELEMETRY_DISABLED=1 npm run check:route-retirement
ASTRO_TELEMETRY_DISABLED=1 npm run check:core-doc-links
ASTRO_TELEMETRY_DISABLED=1 npm run check:page-health:selftest
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
page-health checks, route-retirement checks, and exit-code reference validation
before building `edge` and before building released tags that include the
relevant checker scripts. Older historical tags without those checkers are still
built with a warning.

## Published routes

`scripts/data/published-routes.json` records every route this site has
published, and `scripts/check-route-retirement.mjs` requires each of them to be
either a live page today or the source of a redirect in `astro.config.mjs`.
That is the one thing no other gate here can see: `check-redirects` validates
the redirects that exist, `check-links` reports a broken link between pages,
and a page renamed with no redirect involves neither. It 404s for every bookmark
and search result instead.

Add a page, then record its route in the same change:

```bash
node scripts/check-route-retirement.mjs --write
```

The regeneration only adds, and it adds to the ledger on disk. It never drops
an entry, because the entry for a route whose page this change deleted is
exactly what the gate is looking for -- and it refuses to run at all when the
ledger file is missing, since rebuilding it from the tree would erase the same
evidence a different way. A ledger git still tracks is reported as a deletion to
restore; `--seed` is the separate flag for a repository that has never had one.

Two further modes:

```bash
# Nothing recorded at the merge base has been dropped from the ledger.
node scripts/check-route-retirement.mjs --against origin/master

# Drop a route this branch invented and renamed before it merged. Refused for
# any route the ledger at the merge base already recorded.
node scripts/check-route-retirement.mjs --forget /schema/newthing/ --against origin/master
```

`--against` runs in the docs `build` job, which is checked out with
`fetch-depth: 0`. It is the answer to the one escape the primary invariant
cannot see: the check iterates the ledger, so a hand-deleted line is a line it
never visits.

The ledger sits under `scripts/data/` rather than in the content collection
because `docs/docs.go` embeds that collection whole, and a file placed there
would ship inside every `ptah` binary.

## Which files are routes

`scripts/lib/docroutes.mjs` is the single answer to "which routes does this site
publish", and `check-links`, `check-redirects`, `check-page-health` and
`check-route-retirement` all read the tree through it. It enumerates pages with
`git ls-files` rather than by walking the directory, so a checkout parked under
the content root contributes nothing, and it derives each route the way Astro
does:

- a frontmatter `slug:` replaces the file path;
- a basename starting with `_` is a partial and publishes no route;
- two files that resolve to one route are refused, not silently merged;
- a path segment Astro would put through github-slugger -- anything outside
  `[a-z0-9_-]` -- is refused by name. These gates carry no npm dependency, so
  the slugifier is not available to them, and an unmodeled route is reported
  loudly rather than recorded wrongly. Rename the file.

## Navigation

The sidebar is `src/sidebar.mjs`, a module `astro.config.mjs` imports. It lives
outside the config because `scripts/check-page-health.mjs` reads it, and a plain
Node script cannot import the config: Starlight's entry point is TypeScript
inside `node_modules`, which Node refuses to strip types for. Keep the module
dependency-free so the gate keeps reading a value rather than a regex over a
file.

The gate reads it in both directions. Every page needs an entry, and every entry
needs something at the other end: a `slug:` names a page, an internal `link:`
names a route the site publishes or a redirect `astro.config.mjs` declares.
Either kind counts as coverage. External `link:` values are left alone.

The tree is two levels: a top-level group holds subgroups, a subgroup holds
pages. Starlight nests groups to any depth and the gate flattens whatever it
finds, so the cap is a reading rule that review holds, as is the rule that no
list of siblings runs much past eight. A group carries a `label` and `items`
and nothing that navigates -- the schema has no `link` and no `slug`, and the
heading renders as a `<summary>` rather than an `<a>` -- so a section index is
an ordinary first item inside its own group, labeled `Overview` where the page
title would otherwise repeat the group label. `collapsed: true` hides a
subgroup's items until the reader opens it, and opens the group anyway
whenever the current page is inside it.

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
