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
ASTRO_TELEMETRY_DISABLED=1 npm run check:content-inventory:selftest
ASTRO_TELEMETRY_DISABLED=1 npm run check:content-inventory
ASTRO_TELEMETRY_DISABLED=1 npm run check:support-matrix:selftest
ASTRO_TELEMETRY_DISABLED=1 npm run check:support-matrix
ASTRO_TELEMETRY_DISABLED=1 npm run check:exit-codes
ASTRO_TELEMETRY_DISABLED=1 npm run check:navigation:selftest
ASTRO_TELEMETRY_DISABLED=1 npm run check:search-ranking:selftest
ASTRO_TELEMETRY_DISABLED=1 npm run build
ASTRO_TELEMETRY_DISABLED=1 npm run check:navigation
ASTRO_TELEMETRY_DISABLED=1 npm run check:search-ranking
npm run versions:selftest
npm run root-assets:selftest
npm run check:pages-root:selftest
npm run check:pages-root
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

## Content metadata and inventory

Every published page declares its page type, audience, reader question,
outcome, sources of truth, generated status, overlaps, and editorial
disposition. `src/content.config.ts` validates the contract. Status pages also
name a verification date and evidence; fully generated pages name their
generator and edit source.

`content-inventory.json` is generated from that metadata, the content
collection, the sidebar, and the internal link graph:

```bash
npm run inventory:write
```

Do not edit the JSON by hand. `CONTENT_INVENTORY.md` contains reader journeys
and editorial decisions only.

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

The root list names product domains directly. A root row may hold pages or one
further subgroup level. Starlight nests groups to any depth and the gate
flattens whatever it finds, so the cap is a reading rule that review holds, as
is the rule that no list inside a root row runs much past eight.

A Starlight group carries a `label` and `items`, but no `link` or `slug`.
The group heading is always a disclosure control. Put a landing page first in
the group's `items` as an ordinary child link. Use `Overview` when the page
title repeats the group label, or give the child a more specific label when it
answers a narrower question. `check-navigation.mjs` requires this first child
to have `type: landing` and renders a descendant of every top-level group to
verify that the parent breadcrumb links back to it. `collapsed: true` hides a
subgroup's items until the reader opens it, and opens the group anyway whenever
the current page is inside it.

## Page context and actions

`src/components/PageTitle.astro` replaces Starlight's page-title component. It
derives each breadcrumb from the rendered sidebar tree, including its current
page marker, so the page hierarchy has no second label map to maintain. The
home crumb uses Starlight's version-aware site URL.

`Copy page` reads a generated Markdown representation from
`page-source/<page-id>.md`. The endpoint strips frontmatter, restores the title
and description, and records the canonical page URL. Keeping that text outside
the HTML avoids sending a second copy of a long reference page to every reader.
The remaining actions link to edit, source, and issue-reporting destinations in
that order. `check-navigation.mjs` exercises the menu's keyboard open and Escape
paths. `check-search-ranking.mjs` queries the built Pagefind index and requires
each recorded canonical page in the first three results.

The source endpoints are build artifacts, not reader pages. They are omitted
from the sitemap and from the published-route ledger, and no sidebar entry
points at them.

## The Pages root

The site is served from `https://stokaro.github.io/ptah/`, and everything a
reader browses lives one directory down, under a version slug. A few files sit
at the root itself and address the site as a whole:

| File | Written by | What it is |
| --- | --- | --- |
| `versions.json` | `scripts/gen-versions.mjs` | The version list the picker reads |
| `index.html` | `scripts/gen-versions.mjs` | The redirect from the root to the default version |
| `install.sh` | `scripts/publish-root-assets.mjs` | The shell installer, from `public/install.sh` |
| `install.ps1` | `scripts/publish-root-assets.mjs` | The PowerShell installer, from `public/install.ps1` |

The installers have to answer at the root because the commands a reader is
given carry no version:

```bash
curl -fsSL https://stokaro.github.io/ptah/install.sh | sh
```

```powershell
irm https://stokaro.github.io/ptah/install.ps1 | iex
```

`.github/workflows/docs.yml` assembles `_site/` from scratch on every deploying
run and uploads the whole directory, so there is no Pages state for a file to
survive in. A root file exists after a deploy only because that deploy wrote
it. That is why the installers are published by a workflow step rather than
uploaded once, and why the step carries the same `push` condition as the
version index beside it: a tag deploy has to write them too.

`scripts/check-pages-root.mjs` is the gate. It runs the assembly and reads what
came out, rather than asking whether the files are in the tree — the difference
between those two questions is the whole failure it exists to catch, because a
tree that still carries `public/install.sh` and a workflow that no longer
publishes it look identical from the tree's side. It also requires the workflow
to still invoke both producers before the upload, and requires the
documentation and the root to agree in both directions: a published URL nothing
writes is a 404, and a file nothing documents is unreachable.

`--site <dir>` is the second half, and the build job runs it on the real `_site`
after assembling it. Everything the gate reads in the tree can be right while
the directory about to be uploaded is missing a file.

### What only a request can see

Both halves above read this repository: one reads the tree, the other reads the
directory a run of this workflow assembled. Neither asks whether
`https://stokaro.github.io/ptah/install.sh` answers, and there are ways for it
to stop answering that leave no trace here at all:

- A Pages settings change, or a repository rename that moves the whole site.
- A deploy triggered by a **tag**. A tag push runs the workflow file as it
  exists *at that tag*, and every tag cut before this mechanism landed has a
  `docs.yml` with no publish step — it still builds, uploads and deploys, and a
  Pages deployment replaces the whole site. Re-cutting or re-running such a tag
  would deploy a root with no installers in it. Tags move forward, so cut them
  from `master` and do not re-push an old one.

`--live` is the third half and the only one that asks the address. It requests
every root file, requires a 200, and requires each installer to be the bytes of
its source under `public/`. It probes the generated files beside them as a
control: a run where everything answers 404 is a site that moved, and a run
where only an installer does is the publish step having stopped running.

```bash
node scripts/check-pages-root.mjs --live
```

It runs on the schedule in `.github/workflows/install-smoke.yml`, not on a push
— a push to `master` starts that workflow and the Pages deploy at the same
moment, so a probe there would race the deploy it is verifying. The site
converges on `master` within a minute of each push, which is why a byte
difference on the schedule is the root serving something the repository does
not, rather than a deploy in flight.

### Adding a root file

Add an entry to `ROOT_ASSETS` in `scripts/publish-root-assets.mjs`, put the file
under `public/`, and give it a documented command. Nothing else needs editing:
the workflow step already copies whatever the declaration names, and the gate
already requires the rest.

`public/` rather than the repository's `scripts/` directory, because
`docs.yml` filters its jobs on `docs/**` and `docs/site/**`. A file under
`scripts/` would change the installer without running the workflow on the pull
request and without deploying on merge.

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
