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

### Findings that remain valid

- The inference quick start still needs a disposable local provider fixture and
  a managed happy path.
- General troubleshooting, AI Assist, AI agents, Atlas overview, and database
  support still mix independent documentation jobs.
- Prose and wide reference content still share one `70rem` measure.
- Schema visualization does not show the generated diagram, and the generated
  documentation and live schema pages do not show their browser output.
- Example directories do not yet share one reader contract.
- Page actions prioritize assistant destinations before edit, source, and issue
  actions.
- Search ranking, axe, keyboard navigation, selected visual snapshots, and
  diagram-source policy are not yet gated.

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
| Detect and fix schema drift | `Direct schema changes > Overview`; decide whether the result is a CI gate or an apply | `start/quick-start-direct` or `direct/overview` → `direct/compare-and-drift` → `direct/plan-and-approve` or `direct/apply` | Complete for the onboarding evolution and gate; search ranking is not yet measured. |
| Migrate persistent inference state | `Inference migrations > Overview`; decide whether this lifecycle applies | `inference/overview` → `inference/quick-start` or a focused guide | Discoverable; the first tutorial still depends on an external model endpoint. |
| Recover a failed inference migration | Copied error or inference sidebar | `inference/troubleshooting` → `inference/guides/resume-and-recover` | Symptom-oriented pages exist; copied-error search ranking is not yet measured. |
| Check database and version support | `Databases > Database support matrix` | `databases/support-matrix` → engine page → `ptah db capabilities` | Correctness is generated and semantically gated in this phase. |
| Migrate from Atlas | `Atlas compatibility > Overview`; decide default compatibility, strict CE, or native adoption | `atlas/overview` → `atlas/adoption` → command or evidence page | Discoverable; the overview still mixes policy, translation, and security detail. |
| Embed Ptah as Go packages | `Integrations > Go integration` | `extend/public-api` → `extend/components` → `extend/query-builder` | Discoverable without the CLI reference. |
| Connect an AI client through MCP | `Integrations > AI and agents` | `operate/ai-agents` → native command reference | Discoverable; connection, security model, tool reference, and artifact protocol still share one page. |
| Arrive from a copied error | Search | subsystem troubleshooting page → canonical task page | Stable error headings exist in inference troubleshooting; site-wide ranking is not yet tested. |
| Add or move a documentation page | `AGENTS.md` and the documentation-maintenance skill | style guide → page metadata → sidebar → redirect ledger → inventory regeneration | Governed by build checks and the current contributor instructions. |

## Implementation sequence

The remaining work stays split into reviewable phases:

1. Navigation and discoverability: validated information architecture,
   breadcrumb links, page actions, edit and freshness affordances, aliases,
   ranking tests, and redirects.
2. Visuals and examples: semantic diagrams, real product output, deterministic
   fixtures, example contracts, accessibility checks, and selected snapshots.
3. Reference and compatibility: compact generated indexes, support
   policy/evidence separation, Atlas and AI page splits, and glossary scope.
4. Editorial sweep: duplication, generic introductions, redundant shell tabs,
   time-sensitive prose, width waivers, and the final desktop/mobile review.

Each phase must leave the current routes buildable and preserve redirects for
anything it moves.
