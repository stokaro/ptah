# Documentation hardening implementation report

Date: August 30, 2026

## Baseline commit

`d790ee0ff3c8ece86deececc838828ed2080102c`

## Historical audit baselines

- `21e725e8524c6ad73256066a8f1068338578667c`
- `d790ee0ff3c8ece86deececc838828ed2080102c`

The current default branch was fetched before implementation. The second
historical baseline is the current baseline. The only documentation changes
between the first historical baseline and the current baseline update pinned
GitHub Actions, PostgreSQL, pgvector, and Python versions; they do not change
the editorial or deployment findings below.

## Findings already fixed

- Every top-level documentation group has an explicit landing child. Group
  headings remain disclosure controls, and breadcrumb ancestors link to the
  landing.
- Every published page carries a validated page type, audience, reader
  question, outcome, source, generated state, overlaps, and disposition.
- The support lookup and census are generated from the release-line
  declaration. Authored prose no longer contradicts the generated counts.
- The default and versioned quick starts execute their published commands.
  The inference tutorial has a deterministic PostgreSQL and embeddings fixture.
- The site already checks links, redirects, retired routes, responsive layout,
  accessibility, navigation, search ranking, generated inventory, examples,
  and the current visual-source policy.
- Schema documentation and schema serve already show deterministic screenshots
  generated from committed fixtures. Schema visualization shows a checked-in
  Ptah SVG instead of only diagram source.
- Existing editorial checks report zero unwaived findings. Twelve specific,
  stale-checked page-length waivers remain.

## Findings still valid

- The Docs workflow has no stable aggregate result on every pull request, and
  the default branch requires no status check.
- The public site exposes no source commit. Before this phase,
  `/ptah/edge/build-info.json` returned HTTP 404, so the reviewed source and the
  deployed site could not be compared.
- Page edit and source actions hard-code `master`; generated pages do not route
  edits to their generator; downloadable samples use mutable `master` URLs.
- The support-fact checker normalizes the whole file with regular expressions.
  It has no row-aware or MDX-component acceptance tests.
- `lengthWaiver` is accepted in frontmatter but unused. `lastVerified` checks
  only the spelling, and repository-local metadata paths are not resolved.
- Core workflow pages still present Go annotations as the normal path.
  `versioned/generate`, direct compare and drift, and brownfield adoption all
  begin with `--root-dir ./models`.
- There is no command-by-source manifest and no equivalent multi-format
  fixture shared by workflow, export, visualization, action, and acceptance
  tests.
- Visual checks use hard-coded file-name sets. They prove that an image exists,
  not that the primary output is visible, readable, inspectable, downloadable,
  and keyboard-accessible.
- The schema visualization page shows one dense SVG. It does not show the
  default Mermaid rendering, filtering, detailed, and security variants as
  actual output. Schema documentation and serve use reduced full-page
  screenshots without focused preview controls.
- Schema security, migration safety, migration tests, and schema tests do not
  show downloadable real HTML or visual output where the product produces it.
- The inference quick-start acceptance builds Ptah from the repository and
  reads repository-relative fixtures. An installed binary plus an extracted
  versioned archive is not yet sufficient.
- The workflow selector still carries an external-tool comparison, and the
  reader-facing route still uses the label `Retained divergences`.

## Findings changed

- Deployment concurrency is documented as deployment-only, but it is declared
  at workflow scope. Validation and deployment therefore share one serialized
  master-push queue. The problem is not the earlier pull-request cancellation
  shape; it is that the implementation does not match its current comment or
  the required deployment-only contract.
- Implementation chronology has a working repository-wide checker for three
  unambiguous phrases. Broader historical wording remains a reading
  responsibility, as intended by the measured style-guide policy.
- The first restructuring added real UI captures and semantic diagrams. The
  remaining visual defect is proof quality and inspectability, not a total lack
  of visual assets.
- Editorial heuristics have zero unwaived findings, but new warnings still
  succeed. The baseline is ready for a zero-finding ratchet rather than another
  cleanup pass.

## New findings

- The default branch has no repository ruleset and no required status checks.
  Linear history and conversation resolution are enabled, while force pushes
  remain allowed.
- The deployed minimum routes all returned HTTP 200 at baseline, but no public
  response identifies the commit that produced them.
- `ptah schema inspect` and `ptah schema test` accept DBML, while several
  `--schema-file` help lines and export examples list only YAML, HCL, and SQL.
  Accepted transport and source expressiveness need command-level evidence.
- `ptah viz` and `ptah schema serve` are genuinely Go-annotation-only in the
  current command tree. Their pages need an early limitation and the nearest
  source-neutral alternative unless the product surface changes.
- Quick-start acceptance runs on every pull request in a separate workflow,
  but no aggregate documentation result depends on it. Example runtime
  acceptance is also outside the existing Docs workflow.

## Repository settings

Required documentation check: `Documentation gate`

Ruleset status: not configured at the baseline. Phase 0 adds the stable check;
the branch-protection API is updated and re-read after that workflow exists on
the default branch.

## Deployment

Built commit: pending the Phase 0 merge.

Public edge commit: unprovable at the baseline because build provenance is
absent.

Public smoke-test result: the six required routes returned HTTP 200. The
provenance route returned HTTP 404. The automated post-deployment smoke test is
implemented in Phase 0 and must pass after merge.

## Source-support audit

The command-by-source audit was executed on August 30, 2026 and is generated
as [`docs/source-support.json`](../source-support.json). It covers every cell
across the required commands, every schema-export target, and all eleven source
categories. Each cell names an exact invocation, implementation owner,
evidence, composability, external-execution opt-in, limitation, and one of the
six required statuses.

The verified common fixture describes the same four-table library schema as
SQL, YAML, HCL, DBML, Go annotations, explicit external SQL, and configured
external SQL. `scripts/check-source-equivalence.sh` renders every spelling with
the built CLI, materializes it on a fresh SQLite database, and compares the
inspection JSON byte for byte. Its mutation self-test changes one YAML column
name and requires the gate to fail.

The audit records these product gaps instead of inferring support from a shared
loader:

- `schema diff` does not accept an OCI source;
- direct `schema plan` and `schema apply` do not accept explicit or configured
  external programs;
- `schema test` cannot compose desired-schema sources;
- `schema lineage` and `schema push` do not accept explicit or configured
  external programs;
- `stokaro/ptah-action` remains explicitly scoped to Go annotations.

Supported cells without a command-level test remain visibly distinct from
verified cells. Later phases may close those test gaps, but this audit does not
launder shared-loader evidence into command-level proof.

## Visual proof infrastructure

`visual-output-inventory.json` now records ten output-producing capabilities:
schema visualization and its security form, generated schema documentation,
the live schema server, migration safety, migration and schema test reports,
lineage, statistics, and API contracts. Each row names the command, route,
fixture, generator, product-output classification, primary asset, variants,
downloads, theme behavior, required placement, acceptance test, owner, and
delivery state. Missing Phase 4 proof stays explicitly missing instead of being
replaced by an explanatory image.

`visual-assets.json` is the only asset allowlist. Seven current assets declare
their type, source or deterministic generator, fixture, reader routes, size
budget, theme behavior, full-size artifact, and versioned-download requirement.
The asset gate scans the tree for undeclared PNG or SVG files and validates the
two manifests together.

`ProductPreview.astro` establishes the reader contract for primary output: a
semantic figure and caption, a useful alternative, "what to notice," direct
full-size access that works without JavaScript, download/source actions, and an
optional reproduction command. The home product model is the first enforced
usage.

The browser checker no longer accepts an image count as proof. It captures 40
review artifacts — ten routes at mobile and desktop widths in light and dark
themes — and enforces the declared visual ID, caption, maximum word distance,
minimum dimensions, resolving actions, keyboard focus, and page overflow. The
current contract is intentionally one enforced proof; Phase 4 promotes the
product-output rows as their real variants and artifacts land.

## Remaining report sections

The documentation rewrite, visual proof, tutorials, waivers, and remaining
product issues are recorded here as their phases land. The final pull request
description uses the complete report template from the authoritative task.
