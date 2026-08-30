# Documentation content inventory

The machine-readable page map is
[`content-inventory.json`](content-inventory.json). It is generated from the
content collection, page frontmatter, the sidebar, and internal links. It
records every published page's route, title, audience, reader question, reader
outcome, page type, sources of truth, feature ownership, generated status,
overlaps, inbound and outbound links, source size, visible word count,
verification date, search aliases, and editorial disposition.

Do not add page counts, route lists, source sizes, or link counts to this file.
Those facts belong in the generated inventory:

```bash
cd docs/site
npm run inventory:write
npm run check:content-inventory:selftest
npm run check:content-inventory
```

`CONTENT_INVENTORY.md` contains only decisions and findings that cannot be
derived by reading files.

## Current audit baseline

The second restructuring audit was revalidated against `master` at
`cfe896af7a8d289916bcb7ebd0e5fe519b7eecc5`. The attached audit used
`5509bc64c02deb2e1c7676c4a63ace77b3eb1415`.

The baseline comparison found one material documentation change between those
commits: the experimental linked sidebar parents were removed. PR
[stokaro/ptah#2547](https://github.com/stokaro/ptah/pull/2547) restored each
landing page as an explicit child such as `Overview`.

### Findings already resolved before this phase

- Major workflow domains are top-level sidebar groups.
- Versioned migrations, direct schema changes, and inference migrations each
  expose an explicit landing child.
- Advanced Atlas evidence is collapsed by default.
- A generated release-line table and generated support census already existed.
- Route retirement, link, style, terminology, responsive, glossary, and docs
  build checks already existed and remain enabled.

### Findings resolved in governance Phase 1

- Authored support prose contradicted the generated release-line matrix. The
  authored census is removed. A semantic check now refuses hand-written counts
  and concrete release-line classifications outside the generated block.
- Page taxonomy and ownership existed only in this historical planning file.
  Every published page now carries validated editorial metadata in its own
  frontmatter.
- This file manually repeated routes, word counts, and sidebar structure from
  an old tree. The generated inventory now owns those facts.
- Status pages now name a verification date and evidence. Fully generated pages
  name both their generator and the file a contributor should edit.

### Findings resolved in onboarding Phase 2

- The docs home now routes by reader situation instead of repeating the same
  product flow as a diagram, prose walkthrough, command sample, card grid, and
  decision table.
- `/start/quick-start/` now produces and verifies a SQLite result directly. The
  longer workflow decision remains on `start/choose-a-workflow`.
- The direct onboarding path separates initial apply from schema evolution and
  CI drift detection.
- The versioned quick start keeps one apply-and-verify path. Rollback, integrity
  policy, generated migrations, and CI remain on their canonical task pages.
- The default install page now ends with verified binaries and a next action.
  Alternative methods and exhaustive installer options moved to
  `start/install-options`.
- The versioned landing now begins with the operational situation in which the
  workflow matters. The direct landing already passed that test and retains its
  structure.

### Findings resolved in navigation Phase 3

- Every top-level group now begins with an explicit landing page. The group
  heading remains a disclosure control, while its parent breadcrumb links to
  that first child.
- The validated top-level order is Start; Versioned migrations; Direct schema
  changes; Inference migrations; Define and understand schemas; Databases;
  Test, automate, and operate; Extend and integrate; Reference; Atlas
  compatibility. Versioned and direct work remain separate because they are
  different operational choices, not subdivisions of one reader task.
- Advanced inference strategy, reference, generated reference, and Atlas
  evidence groups are collapsed by default.
- Page actions now prioritize copy, edit, source, and issue reporting. Git
  history supplies Last updated; evidence-bearing pages also show their
  independent verification date.
- Search aliases are indexed without adding filler to visible introductions.
  The documentation workflow requires the canonical page to rank in the top
  three for all acceptance queries in `check-search-ranking.mjs`.
- The navigation check renders every top-level parent breadcrumb and exercises
  the page-action menu with the keyboard. A linked group heading or missing
  landing fails before merge.

### Findings that remain valid

- General troubleshooting, AI Assist, AI agents, Atlas overview, and database
  support still mix independent documentation jobs.
- Prose and wide reference content still share one `70rem` measure.

### Findings resolved in visuals and examples Phase 4

- The product and inference raster diagrams were replaced by semantic,
  theme-aware SVGs with titles, descriptions, and surrounding text equivalents.
- Schema visualization now shows the checked-in Ptah output. Schema document
  and live schema pages show deterministic screenshots generated by the real
  commands from committed fixtures.
- Every top-level example has one reader contract and appears in the generated
  examples index. The execution gate also repaired an annotation example that
  printed three errors while exiting successfully.
- The inference quick start now supplies PostgreSQL 17 with pgvector 0.8.1, a
  deterministic local embeddings endpoint, seeded rows, and one-command
  cleanup. Its acceptance gate proves plan, candidate build, verification,
  approval binding, and the active pointer.
- Axe WCAG A/AA, keyboard interactions, scroll-region focus, visual-source
  policy, and selected desktop/mobile review snapshots are gated in CI.

### Audit recommendation deliberately rejected

Sidebar group headings remain disclosure controls. A group does not sometimes
navigate and sometimes expand. Its landing page is the first explicit child,
usually `Overview`. Breadcrumb ancestors may become links only when every
ancestor has an unambiguous landing route; that change must not alter the
sidebar interaction contract.

## Page metadata contract

Every published page declares:

- one primary `type`;
- at least one `audience`;
- one `readerQuestion`;
- one reader outcome in `goal`;
- at least one `sourceOfTruth`;
- whether the page is `generated`;
- explicit semantic `overlaps`, including `[]` when none are recorded;
- one editorial `disposition`.

Status pages additionally declare `lastVerified` and `evidence`. Fully
generated pages declare `generator` and `editSource`. `owns` remains the
feature-ownership identifier checked by the repository's feature inventory.

The content schema and the generated inventory use the shared rules in
`src/lib/content-metadata.mjs`. A missing or invalid field fails both the site
build and the inventory check.

## Reader-journey report

The paths below are editorial acceptance criteria. The entry and canonical
pages were verified against the current sidebar and link graph. A journey that
depends on a runnable tutorial remains open until its commands and stable
results are re-executed in the onboarding or examples phase.

| Journey | Entry and decision | Canonical path | Current result |
| --- | --- | --- | --- |
| Install Ptah and get a first result | Home or `Start > Install Ptah`; run the default installer | `start/install` → `start/quick-start` | Complete: the tutorial applies and reads back a disposable SQLite schema in five primary steps. |
| Choose versioned or direct schema changes | `Start > Choose a workflow`; decide how a change reaches the database | `start/choose-a-workflow` → `versioned/overview` or `direct/overview` | Discoverable in one decision. |
| Adopt an existing database | `Start > Adopt an existing database`; choose baseline, import, or desired-schema adoption | `start/adopt-an-existing-database` → `direct/inspect` → versioned or direct task | Discoverable; command evidence must be refreshed when the page is edited. |
| Generate and apply a migration | `Versioned migrations > Overview`; decide whether the migration is authored or generated | `start/quick-start-migrations` or `versioned/overview` → `versioned/generate` → `versioned/apply` | Complete for an authored first migration; generated migrations remain a focused follow-up. |
| Detect and fix schema drift | `Direct schema changes > Overview`; decide whether the result is a CI gate or an apply | `start/quick-start-direct` or `direct/overview` → `direct/compare-and-drift` → `direct/plan-and-approve` or `direct/apply` | Complete for the onboarding evolution and gate; canonical search results are top-three gated. |
| Migrate persistent inference state | `Inference migrations > Overview`; decide whether this lifecycle applies | `inference/overview` → `inference/quick-start` or a focused guide | Complete: the tutorial uses disposable PostgreSQL and a deterministic local endpoint, and CI proves the generation reaches the active pointer. |
| Recover a failed inference migration | Copied error or inference sidebar | `inference/troubleshooting` → `inference/guides/resume-and-recover` | Symptom-oriented pages exist; the recovery guide is top-three gated for the recorded search query. |
| Check database and version support | `Databases > Overview`; choose lookup or live measurement | `databases/overview` → `databases/support-matrix` → engine page → `ptah db capabilities` | Correctness is generated and semantically gated; MySQL and SQL Server lookup queries are top-three gated. |
| Migrate from Atlas | `Atlas compatibility > Overview`; decide default compatibility, strict CE, or native adoption | `atlas/overview` → `atlas/adoption` → command or evidence page | Discoverable; the overview still mixes policy, translation, and security detail. |
| Embed Ptah as Go packages | `Extend and integrate > Overview`; choose the Go API path | `extend/overview` → `extend/public-api` → `extend/components` → `extend/query-builder` | Discoverable without entering CLI reference. |
| Connect an AI client through MCP | `Extend and integrate > Overview`; choose the AI client path | `extend/overview` → `operate/ai-agents` → native command reference | Discoverable and top-three gated for `MCP`; connection, security model, tool reference, and artifact protocol still share one page. |
| Arrive from a copied error | Search | subsystem troubleshooting page → canonical task page | Stable error headings exist; canonical checksum, recovery, and exit-code results are top-three gated. |
| Add or move a documentation page | `AGENTS.md` and the documentation-maintenance skill | style guide → page metadata → sidebar → redirect ledger → inventory regeneration | Governed by build checks and the current contributor instructions. |

## Implementation sequence

The remaining work stays split into reviewable phases:

1. Reference and compatibility: compact generated indexes, support
   policy/evidence separation, Atlas and AI page splits, and glossary scope.
2. Editorial sweep: duplication, generic introductions, redundant shell tabs,
   time-sensitive prose, width waivers, and the final desktop/mobile review.

Each phase must leave the current routes buildable and preserve redirects for
anything it moves.
