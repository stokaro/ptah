# Documentation restructuring report

Date: August 30, 2026

## Baseline and audit delta

The implementation was revalidated against Ptah `master` at
`cfe896af7a8d289916bcb7ebd0e5fe519b7eecc5`. The supplied audit described
`5509bc64c02deb2e1c7676c4a63ace77b3eb1415`.

The only material navigation change between those commits was already correct:
linked sidebar group headings had been removed, and each major group exposed a
separate landing child. The restructuring preserves that contract. Group
headings disclose children; an explicit first child, normally `Overview`, is
the landing; a breadcrumb ancestor links to that landing.

The remaining high-priority findings were rechecked before editing. The support
matrix contained generated release facts beside contradictory authored facts;
the home and quick starts repeated several reader jobs; support policy, support
lookup, and measurement evidence competed on one route; reference and workflow
material were mixed; prose inherited the width required by the largest tables;
and general troubleshooting did not give a consistent diagnosis path.

## Delivery stack

The work is split into reviewable branches and pull requests:

1. Governance and support correctness: commit `d650b24e1`, PR
   [#2553](https://github.com/stokaro/ptah/pull/2553).
2. Onboarding: commit `054ffbfae`, PR
   [#2555](https://github.com/stokaro/ptah/pull/2555).
3. Navigation and discoverability: commit `20740300d`, PR
   [#2556](https://github.com/stokaro/ptah/pull/2556).
4. Reproducible visuals and examples: commit `3e34e903b`, PR
   [#2557](https://github.com/stokaro/ptah/pull/2557).
5. Reference and compatibility separation: commit `2fe3c57fb`, PR
   [#2558](https://github.com/stokaro/ptah/pull/2558).
6. Editorial, layout, and final validation: the final stacked branch based on
   Phase 5.

## Pages and routes

The generated inventory contains 126 live pages: 124 authored pages and two
fully generated pages. Every page carries its audience, reader question,
outcome, type, sources of truth, generated state, overlaps, and disposition.

Eighteen routes were added:

- section landings: `/start/overview/`, `/schema/overview/`,
  `/databases/overview/`, `/operate/overview/`, `/extend/overview/`, and
  `/reference/overview/`;
- focused onboarding and support pages: `/start/install-options/`,
  `/databases/support-policy/`, and `/databases/support-evidence/`;
- Atlas boundaries: `/atlas/strict-ce-mode/` and
  `/atlas/output-and-redaction/`;
- AI agent and Assist tasks: `/operate/ai-agent-connect/`,
  `/operate/ai-agent-permissions/`, `/operate/ai-agent-changes/`,
  `/operate/ai-agent-troubleshooting/`, `/operate/ai-assist-providers/`, and
  `/operate/ai-assist-sessions/`;
- MCP lookup: `/reference/mcp-tools/`.

No route was moved, merged, or retired. The existing 33 redirects remain
unchanged and pass the redirect and retirement gates. The route ledger records
159 published routes: 126 live and 33 redirected.

Content splits without route retirement:

- installation alternatives moved out of the default install path;
- support policy and dated evidence moved out of the generated support lookup;
- strict CE policy and shared output behavior moved out of the Atlas landing;
- MCP setup, permissions, patching, diagnostics, providers, sessions, and tool
  lookup moved out of the two former AI catch-all pages.

## Reverified claims

- `internal/capabilityprobe/cells.go` generates the complete release-line
  table, census, non-probed reasons, and tag notes. The audit's contradictory
  authored counts and classifications are gone; authored prose cannot repeat
  those derived facts.
- The generated support lookup, authored policy, and dated evidence pages have
  separate sources and reader questions. A semantic gate rejects a future
  hand-maintained support census.
- The command references match 11 generated documentation blocks. The feature
  matrix matches 192 source rows, and flag-name evidence was checked against
  310 registered flags.
- Native `ptah`, default `ptah-compat`, strict CE mode, the pinned Atlas CE
  binary, and Ptah's Pro-like retained capabilities are described as separate
  surfaces. Registration alone is never used as runtime parity evidence.
- The two published quick starts executed exactly as written. Each ran six
  shell steps and eight stable output assertions.
- The inference tutorial reached a prepared candidate, backfill, catch-up,
  index, deterministic verification, digest-bound approval, and active pointer
  against the committed PostgreSQL, pgvector, and local embedding fixtures.

## Generated inventory and governance

`content-inventory.json` is generated from the content collection, sidebar,
frontmatter, and internal link graph. It records route, source path, sidebar
path, page type, reader contract, ownership, evidence, link relationships,
visible words, source bytes, verification date, aliases, and disposition.

The content schema fails on missing or invalid page contracts. Status pages
require a verification date and evidence; fully generated pages require a
generator and edit source. The editorial gate reports long pages, mixed-type
signals, formulaic first paragraphs, near-duplicate prose, and identical tab
panels. Identical panels fail. Judgment-based findings may carry a specific
waiver, and a stale waiver fails.

## Visuals and examples

The generated-text raster diagrams were removed. Their replacements are the
semantic, editable `product-journeys.svg` and
`inference-generation-lifecycle.svg` sources. The schema visualization page
shows checked-in Ptah output. The schema document and live schema pages show
deterministic UI screenshots generated from committed fixtures:

- `schema-document.png`;
- `schema-serve-matches.png`;
- `schema-serve-drift.png`.

The visual-source check permits raster output only for these real UI surfaces,
requires its regeneration record, validates image alternatives, and rejects a
new raster diagram containing authored text.

Every top-level example has the reader contract and appears in the generated
index. Four examples execute, the visualization artifacts are checked, and two
provider fixtures pass mechanical verification.

## Layout and accessibility

The page shell remains `70rem` wide for code, diagrams, generated matrices, and
wide tables. Ordinary prose stops at `40rem`. The responsive gate measures all
126 routes at 390px, 1280px, and 1920px, refuses document-level overflow, keeps
prose within 642px, and checks table density and local scrolling.

Axe WCAG A/AA checks pass on six representative routes at mobile and desktop
widths. Keyboard tests cover sidebar disclosure, breadcrumbs, tabs, glossary
panels, reference filters, and page actions. Page actions appear as Copy page
as Markdown, Edit this page, View source, and Report a documentation issue.

## Search and navigation

All ten top-level groups have explicit landing children and linked breadcrumb
ancestors. Advanced inference reference, generated reference, and Atlas
evidence groups are collapsed by default.

The Pagefind smoke test requires the canonical route in the first three results
for all 20 queries:

- install Ptah;
- first migration;
- apply migrations;
- rollback migration;
- schema drift;
- apply desired schema;
- adopt existing database;
- migrate from Atlas;
- checksum mismatch;
- PostgreSQL extension;
- SQL Server support;
- MySQL supported versions;
- pgvector;
- change embedding model;
- resume inference migration;
- MCP;
- Go annotations;
- visualize schema;
- generate protobuf;
- exit code 2.

## Manual desktop and mobile review

The following routes were captured and reviewed at 390px and 1440px:

- `/`;
- `/start/quick-start/`;
- `/inference/overview/`;
- `/databases/support-matrix/`;
- `/operate/troubleshooting/`;
- `/reference/native-commands/`;
- `/atlas/overview/`;
- `/schema/visualize/`;
- `/schema/document/`;
- `/schema/serve/`.

The review found consistent landing hierarchy, readable prose, locally
scrollable reference tables and code, usable command filtering, no
document-level overflow, and legible real UI output. The troubleshooting page
keeps each symptom, cause, diagnosis, resolution, and verification together at
both widths.

## Validation

The complete documentation workflow and the added checks passed locally:

- links: 126 pages, 1,134 heading anchors, and 1,148 links;
- redirects and route retirement: 33 redirects and 159 published routes;
- metadata, inventory, support consistency, exit codes, terminology,
  limitations, chronology, style, examples, and visual-source checks;
- docs build: 127 pages;
- responsive rendering: 126 routes at three widths;
- glossary: 82 panel openings across three pages and two widths;
- accessibility: six routes at two widths plus keyboard controls;
- visual review artifact: 20 desktop/mobile screenshots;
- navigation: ten section landings, breadcrumb links, and keyboard page
  actions;
- search ranking: 20 of 20 canonical routes in the first three results;
- generated docs: 11 docsync blocks and 192 feature-matrix rows;
- `go test ./internal/cmdref -count=1`;
- `scripts/check-quickstart.sh`;
- `scripts/check-examples.sh`;
- `check-inference-quick-start.sh` through `remote-dev-container`, followed by
  a check that its named containers, images, network, and volumes were gone.

`npm audit --audit-level=low` was attempted but the restricted executor could
not reach the public registry, and escalation for transmitting the dependency
manifest was not approved. The repository's Docs workflow does not contain an
`npm audit` gate; `npm ci` and the complete declared workflow passed.

## Editorial waivers

Twelve pages exceed their type's review threshold and remain intentionally
whole. Each reason is stored beside the check in
`scripts/data/editorial-waivers.json`:

- exhaustive generated or measured lookup:
  `/atlas/feature-matrix/`, `/reference/atlas-commands/`,
  `/reference/command-flags/`, and `/reference/native-commands/`;
- command-family reference with stable verb anchors:
  `/atlas/migrate-commands/` and `/atlas/schema-commands/`;
- one-language or one-engine reference:
  `/atlas/project-config/` and `/databases/postgresql/`;
- dated divergence evidence: `/atlas/retained-divergences/`;
- one stateful workflow or trust boundary: `/schema/protobuf/`,
  `/versioned/apply/`, and `/versioned/integrity-and-safety/`.

The check reports zero unwaived editorial warnings. A future split that removes
a finding makes its waiver stale and fails CI until the exemption is deleted.

## Follow-up

No unresolved documentation correctness or navigation defect is deferred from
this restructuring. Product and compatibility gaps remain linked to their
existing issues on the dated status pages; this change does not recast those
product gaps as documentation work.
