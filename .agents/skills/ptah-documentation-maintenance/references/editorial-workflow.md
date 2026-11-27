# Editorial workflow

Use the sections relevant to page structure, metadata, navigation, visuals, or
examples. The style guide remains authoritative.

- [Content inventory](#maintain-the-content-inventory)
- [Navigation](#preserve-the-navigation-contract)
- [Page quality](#quality-bar)
- [Behavior and examples](#validate-against-current-behavior)
- [Tables and Markdown](#tables-and-supported-markdown)
- [Review](#self-review)

## Maintain the content inventory

`docs/site/scripts/build-content-inventory.mjs` computes the factual inventory
of every published page, and nothing stores it. Page frontmatter owns the
editorial metadata: audience, reader question, outcome, page type, sources of
truth, overlaps, generated status, search aliases, and disposition. The builder
derives routes, sidebar paths, links, word counts, and source sizes.

`check:editorial-shape`, `check:source-page-contracts`, and `check:navigation`
build the inventory from the current tree on every run. The build refuses
invalid page metadata, a feature two pages own, an overlap that names no live
route, and a page with no sidebar entry. To read the inventory, run
`node scripts/build-content-inventory.mjs --print` in `docs/site`. Keep reader
journeys and non-derivable decisions in `docs/site/CONTENT_INVENTORY.md`; never
copy mechanical counts or route lists into that file.

Run `npm run check:editorial-shape:selftest` and
`npm run check:editorial-shape` after any page, metadata, navigation, or link
change. Length, mixed-type, generic-introduction, and near-duplicate findings
are checked against the existing baseline. New unwaived findings fail. Keep a
deliberate finding quiet only through a reasoned entry in
`scripts/data/editorial-waivers.json`; the check rejects stale waivers.

## Preserve the navigation contract

A top-level sidebar group is a disclosure control. Give it no route. Its first
child must be an authored `type: landing` page, normally labeled `Overview`;
use a task-specific child label when that is clearer. Breadcrumbs link the
group ancestor to this explicit child. A subgroup has no breadcrumb link unless
it also has an explicit landing.

For navigation or discoverability changes, regenerate the inventory, build the
site, then run `npm run check:navigation` and
`npm run check:search-ranking`. Add a search alias only for reader language the
title and visible prose do not already contain. The ranking check owns the
accepted query-to-canonical-route map and requires a top-three result.

## Quality bar

The binding rules are in `docs/STYLE_GUIDE.md`. When designing or restructuring
a page, Inventario's docs offer additional examples of tone and structure. Read
only a relevant example:

- [Home](https://github.com/denisvmedia/inventario/blob/master/docs/site/src/content/docs/index.mdx)
- [Getting started](https://github.com/denisvmedia/inventario/blob/master/docs/site/src/content/docs/getting-started.md)
- [Self-hosting](https://github.com/denisvmedia/inventario/blob/master/docs/site/src/content/docs/self-hosting.md)
- [Backup and restore](https://github.com/denisvmedia/inventario/blob/master/docs/site/src/content/docs/backup-and-restore.md)
- [Groups and sharing](https://github.com/denisvmedia/inventario/blob/master/docs/site/src/content/docs/groups-and-sharing.md)
- [Reports](https://github.com/denisvmedia/inventario/blob/master/docs/site/src/content/docs/reports.md)

Match the discipline, not the product structure exactly:

- Start with what the reader is trying to do.
- Include concrete, copy-pasteable commands where a command is useful.
- Show expected output or verification commands when the result can be checked.
- Use comparison tables for command parity, feature support, config precedence,
  or dialect behavior when prose would be ambiguous.
- Keep prose inside the site's `60rem` reading measure and aligned with the
  `70rem` content shell's left edge. Tables, code, diagrams, and explicit
  `.ptah-wide-content` blocks may use the full page measure; never widen
  ordinary paragraphs for a reference artifact. On ultra-wide two-rail pages,
  additional space belongs between the content shell and the right rail after
  the left gutter reaches the navigation rail's width.
- Add troubleshooting notes for likely failure modes.
- Link to related docs, examples, issues, or tests instead of duplicating large
  blocks.
- Keep American English spelling.

## Validate against current behavior

Documentation is not done until it is checked against the repo and, when
practical, live commands:

- Run the commands shown in new or changed examples, or explain in the PR why a
  command cannot be run locally.
- For CLI docs, compare against current help and output from the built `ptah`
  binary. `go test ./internal/doccommands -count=1` resolves every `ptah` and
  `ptah-compat` command line in the tracked Markdown and in `--help` against
  the command tree; run it after any change that adds a command line or
  renames a verb or flag.
- For a workflow that accepts several desired-schema sources, keep the prose
  source-neutral and make the first complete example a static source that does
  not require Go. Use the exact selector for the chosen transport:
  `--schema-file`, `--root-dir`, `--schema-cmd`, or
  `--config ... --allow-external-schema`. A Go-only opening must be proven by
  current built help, recorded in the page's `sourceOfTruth`, and linked to a
  source-neutral alternative or tracked product gap when one exists.
- For config docs, test parse/validation behavior with representative fixtures.
- For generated SQL or migration docs, generate or apply a small fixture when
  feasible.
- For a page that demonstrates a Ptah diagram, browser UI, HTML report,
  lineage, metrics, or contract output, update both visual manifests and run
  the committed generator. Build `bin/ptah`, then run
  `PTAH_BIN=../../bin/ptah npm run assets:write` from `docs/site`. An
  explanatory lineage diagram must say that it is not product output and keep
  the real text and JSON artifacts beside it.
- For public Go API docs, run the relevant guards:

  ```bash
  scripts/check-public-api.sh
  scripts/check-exported-docs.sh
  scripts/check-public-api-released.sh
  ```

## Tables and supported Markdown

The style guide governs table density. A dense table rarely needs only
shorter sentences. A row that needs a paragraph wants the table to become
an index of short linked cells with a section per row underneath; see
`atlas/docs-coverage` and `atlas/retained-divergences` for the shape. Anchors into
those sections are checked, so a later heading rename fails the build
instead of stranding readers at the top of a long page.

Markdown that this site does not render is a recurring trap: definition
lists (`: term`) come out as literal text. Build and read the page before
trusting a syntax the docs do not already use.

A reference page describes a command's flags. Write them as a table or a
list from the start: prose that enumerates a dozen flags inline reads as a
wall no matter how accurate it is, and `check:style` rejects it.

## Self-review

Before opening or updating a PR, review coverage, truth, and style:

1. **Coverage pass**: for each change class, list which docs were checked,
   changed, or intentionally left unchanged. Confirm
   `docs/site/CONTENT_INVENTORY.md` reflects any changed journey or editorial
   decision.
2. **Truth pass**: verify examples, command paths, claims, links, American
   English spelling, and absence of legacy aliases or unsupported parity claims.
3. **Style pass**: run the review checklist in `docs/STYLE_GUIDE.md` (page
   type and template, terminology, executed examples, link and build checks,
   stale-term sweep).

The PR should summarize the documentation impact map and validation commands.
