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

The hardening audit was revalidated against `master` at
`d790ee0ff3c8ece86deececc838828ed2080102c`. Its historical audit references
were `21e725e8524c6ad73256066a8f1068338578667c` and the same current commit.
The complete reconciliation is in
[`DOCUMENTATION_HARDENING_REPORT.md`](DOCUMENTATION_HARDENING_REPORT.md).

The first restructuring was revalidated against `master` at
`cfe896af7a8d289916bcb7ebd0e5fe519b7eecc5`; its attached audit used
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

### Findings resolved in reference and compatibility Phase 5

- The database support route is now a generated lookup. Support policy and
  dated measurement evidence have separate canonical pages, so engine detail,
  testing promises, and the current release census no longer compete on one
  page.
- Atlas compatibility overview now routes readers among native Ptah, default
  `ptah-compat`, and strict CE conformance. Strict-mode content and shared output
  redaction have focused pages.
- AI and agent documentation now separates Assist use, provider configuration,
  session retention, MCP connection, permissions, digest-bound patching,
  troubleshooting, and tool lookup.
- Native command, Atlas-compatible command, and generated flag references retain
  every generated row and add a keyboard-accessible filter for first-level
  lookup.
- The glossary now defines product, schema, migration, inference, support, and
  conformance terms from one registry instead of serving mainly as a support
  matrix tooltip demonstration.

### Findings resolved in editorial Phase 6

- General troubleshooting now routes by stable symptom and gives each entry a
  likely cause, diagnosis, resolution, and verification. Inference and MCP
  failures link to their focused symptom indexes.
- Ordinary prose uses a `40rem` reading measure while code, diagrams, generated
  matrices, and wide tables retain the `70rem` content shell. The responsive
  gate measures the separation in the rendered site.
- Atlas schema and migration command catalogs are classified as references,
  not how-to guides. Completed split and rewrite dispositions now read `keep`.
- Formulaic openings and one repeated schema-file-path paragraph were removed.
  A new editorial check warns about long pages, mixed page types, generic first
  paragraphs, and near-duplicate prose; twelve intentional long pages carry
  specific, stale-checked waivers.
- Identical tab panels are an objective failure. The existing Bash and
  PowerShell tabs remain because their file creation or cleanup syntax differs.

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

The paths below are editorial acceptance criteria. Their entry and canonical
pages are verified against the current sidebar and link graph; runnable
tutorials have separate acceptance checks for commands and stable results.

| Journey | Entry and decision | Canonical path | Current result |
| --- | --- | --- | --- |
| Install Ptah and get a first result | Home or `Start > Install Ptah`; run the default installer | `start/install` → `start/quick-start` | Complete: the tutorial applies and reads back a disposable SQLite schema in five primary steps. |
| Choose versioned or direct schema changes | `Start > Choose a workflow`; decide how a change reaches the database | `start/choose-a-workflow` → `versioned/overview` or `direct/overview` | Discoverable in one decision. |
| Adopt an existing database | `Start > Adopt an existing database`; choose baseline, import, or desired-schema adoption | `start/adopt-an-existing-database` → `direct/inspect` → versioned or direct task | Discoverable; command evidence must be refreshed when the page is edited. |
| Generate and apply a migration | `Versioned migrations > Overview`; decide whether the migration is authored or generated | `start/quick-start-migrations` or `versioned/overview` → `versioned/generate` → `versioned/apply` | Complete for an authored first migration; generated migrations remain a focused follow-up. |
| Detect and fix schema drift | `Direct schema changes > Overview`; decide whether the result is a CI gate or an apply | `start/quick-start-direct` or `direct/overview` → `direct/compare-and-drift` → `direct/plan-and-approve` or `direct/apply` | Complete for the onboarding evolution and gate; canonical search results are top-three gated. |
| Migrate persistent inference state | `Inference migrations > Overview`; decide whether this lifecycle applies | `inference/overview` → `inference/quick-start` or a focused guide | Complete: the tutorial uses disposable PostgreSQL and a deterministic local endpoint, and CI proves the generation reaches the active pointer. |
| Recover a failed inference migration | Copied error or inference sidebar | `inference/troubleshooting` → `inference/guides/resume-and-recover` | Symptom-oriented pages exist; the recovery guide is top-three gated for the recorded search query. |
| Check database and version support | `Databases > Overview`; choose lookup, policy, evidence, or live measurement | `databases/overview` → `databases/support-matrix` → `databases/support-policy` or `databases/support-evidence` → engine page or `ptah db capabilities` | Complete: generated lookup, testing promise, measurement evidence, and target capability report are distinct. |
| Migrate from Atlas | `Atlas compatibility > Overview`; decide default compatibility, strict CE, or native adoption | `atlas/overview` → `atlas/adoption`, `atlas/strict-ce-mode`, command, or evidence page | Complete: the landing routes the three surfaces without carrying command reference or strict-policy internals. |
| Embed Ptah as Go packages | `Extend and integrate > Overview`; choose the Go API path | `extend/overview` → `extend/public-api` → `extend/components` → `extend/query-builder` | Discoverable without entering CLI reference. |
| Connect an AI client through MCP | `Extend and integrate > Overview`; choose the AI client path | `extend/overview` → `operate/ai-agents` → `operate/ai-agent-connect` → permissions, patch workflow, or tool reference | Complete: setup, authority, changes, diagnostics, and exhaustive tool lookup are separate reader tasks. |
| Arrive from a copied error | Search | subsystem troubleshooting page → canonical task page | Complete: general, inference, and AI-agent symptom indexes route to focused fixes; canonical checksum, recovery, and exit-code results are top-three gated. |
| Add or move a documentation page | `AGENTS.md` and the documentation-maintenance skill | style guide → page metadata → sidebar → redirect ledger → inventory regeneration → editorial check | Governed by build checks, stale waiver detection, and the current contributor instructions. |

## Implementation sequence

The restructuring is implemented as six stacked phases: governance,
onboarding, navigation, visuals and examples, reference and compatibility, and
the editorial sweep. Each phase leaves its target branch buildable. Published
routes remain in the redirect ledger even when later work moves them.
