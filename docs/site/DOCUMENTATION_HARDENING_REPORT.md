# Documentation hardening implementation record

Date: August 30, 2026

This record distinguishes repository state, GitHub repository settings, local
validation, and public deployment evidence. Evidence that depends on the
current implementation pull request is marked explicitly and must be replaced
only after the corresponding merge or deployment exists.

## Current baseline commit

The default branch was fetched before implementation. The exact starting
commit was:

```text
3fd09fc02f4712748c51f950815ef251b3112cac
```

That commit was both the current `master` tip and the independently reviewed
historical baseline. Immediately before publication, the branch fetched
`master` again. Subsequent default-branch changes were fetched before final
handoff and the branch was rebased onto
`943501b3b44562d69642c4b974e6c5c0a5e54155`. The only textual conflict across
those rebases was the derived content inventory, which was regenerated from
the combined source tree.

- Ending implementation commit: `741fa58a44a6d65434b8237344cc143bbed2c2af`
- Current implementation PR:
  [#2593](https://github.com/stokaro/ptah/pull/2593)

## Historical baselines

- `5509bc64c02deb2e1c7676c4a63ace77b3eb1415` — supplied baseline for the
  first documentation restructuring audit.
- `cfe896af7a8d289916bcb7ebd0e5fe519b7eecc5` — default-branch state used to
  revalidate that restructuring before its delivery stack.
- `3fd09fc02f4712748c51f950815ef251b3112cac` — independently reviewed
  hardening baseline and the starting point for this change.

Every finding in the hardening task was rechecked against the third commit.
The sections below identify work already present, work implemented here, and
product behavior intentionally left unchanged.

## PRs included

The starting commit contains these merged documentation prerequisites:

- [#2553](https://github.com/stokaro/ptah/pull/2553),
  `8fd10aa6a0034d3530f1b3f8d1372937cb07b466` — content governance and the
  generated support census.
- [#2555](https://github.com/stokaro/ptah/pull/2555),
  `91b1e7e994cf9c95b99b66b7353af85843470da0` — verified onboarding.
- [#2556](https://github.com/stokaro/ptah/pull/2556),
  `a6bb85d99a8fa6b4663c64b06ce5345041d69c14` — explicit section landings.
- [#2557](https://github.com/stokaro/ptah/pull/2557),
  `c96697a250918256303d93adcaba53ef37f4af23` — reproducible visuals and
  example contracts.
- [#2558](https://github.com/stokaro/ptah/pull/2558),
  `9d4b4cdcad9b2c18aefe3a055d1163c622ec277b` — separated support and agent
  references.
- [#2559](https://github.com/stokaro/ptah/pull/2559),
  `21e725e8524c6ad73256066a8f1068338578667c` — editorial and layout sweep.
- [#2567](https://github.com/stokaro/ptah/pull/2567),
  `e98214ac4b9eefae138c2755cf17100ac647475f` — deployment and support
  correctness infrastructure.
- [#2569](https://github.com/stokaro/ptah/pull/2569),
  `e73d136a6caec76dad7dd8af73486f9aaa499357` — schema-source support audit.
- [#2570](https://github.com/stokaro/ptah/pull/2570),
  `5447d986f6674bd2170ffda6ce1ab7a0a318009d` — visual-proof contracts.
- [#2572](https://github.com/stokaro/ptah/pull/2572),
  `c7d2692308b99e406202547030c33c8ddf9b02b3` — source-neutral schema
  workflows.
- [#2579](https://github.com/stokaro/ptah/pull/2579),
  `979eb60710f4f52660143fb0df6e8733688280de` — verified product output.
- [#2580](https://github.com/stokaro/ptah/pull/2580),
  `3fd09fc02f4712748c51f950815ef251b3112cac` — public Go API documentation
  and testable examples.

The pull request containing the hardening recorded below is
[#2593](https://github.com/stokaro/ptah/pull/2593).

## Findings already fixed

These findings were already resolved at the starting commit and were
preserved:

- The existing information architecture has explicit landing children for
  major groups, linked breadcrumb ancestors, collapsed advanced sections, and
  redirects for historical routes.
- Authored pages declare a page type and reader contract. A generated content
  inventory records the content collection, sidebar, link graph, ownership,
  sources, and disposition.
- Database support lookup, policy, and evidence are separate pages. Release
  lines, support levels, probes, counts, and enumerations come from the same
  declarations as the generated support census.
- Native `ptah`, default `ptah-compat`, and strict Atlas Community Edition mode
  remain separate documented surfaces backed by conformance evidence.
- `docs/source-support.json`, the canonical SQL, YAML, HCL, DBML, Go, and
  external-loader fixture, source-equivalence tests, and source-workflow tests
  already prevented transport support from being inferred from a shared
  loader.
- Generated CLI and feature references, quick-start acceptance, example
  contracts, links, routes, page health, accessibility, navigation, search,
  and responsive checks were present.
- `ProductPreview`, semantic diagrams, real schema-document and schema-serve
  output, migration and test reports, and machine-readable visual manifests were
  present.
- Build provenance and a public-site smoke-test framework were present. The
  public evidence still needed the staleness and selector work recorded below.

## Findings fixed by this change

The following baseline findings remained valid and are implemented in this
change:

- Classic branch protection now requires the strict `Documentation gate`
  context. The aggregate job remains present on every pull request, succeeds
  for unrelated changes, and fails when an applicable validation group fails.
  Self-tests cover unrelated Go, documentation, generated command reference,
  example, inference quick-start, and default quick-start changes.
- Public smoke verification uses build provenance, stable product-output
  selectors, content and asset checks, full-size and downloadable artifacts,
  generated HTML samples, a finite retry window, and an explicit stale-commit
  failure. Deployment serialization applies to Pages deployment and does not
  cancel validation.
- One source-context resolver controls authored and generated page actions for
  `edge` and release builds. It separates the exact viewed revision from the
  latest editable revision and includes version, source, rendered path,
  generator, and edit-source context in issue reports.
- The inference quick start publishes a deterministic ZIP and checksum. Bash
  and PowerShell helpers run with an installed `ptah`, configurable ports, and
  no repository-relative paths, `sed`, or `tee` dependency.
- Generic Ptah workflows no longer position Ptah as Go-first. The workflow
  selector answers only whether a change should use versioned migrations or
  direct schema changes. The unrelated competitor comparison was removed.
- The Atlas page keeps its historical route but now uses the reader-facing
  title **Compatibility differences**. `retained divergences` remains a search
  alias.
- New unwaived editorial findings fail. Specific waivers remain centralized,
  reasoned, and stale-checked.
- Semantic metadata validation rejects the dead `lengthWaiver` field,
  impossible or future dates, mistyped repository paths, invalid page
  contracts, and inappropriate generated-page fields. The content schema,
  inventory, and standalone checker share the same implementation.
- Pages that consume schema sources declare a source mode. The source-page
  checker derives command support from `docs/source-support.json` and prevents
  source-neutral pages from regressing to a Go-only first path.
- Pagefind ranking covers non-Go reader language and enforces source-neutral
  pages above Go-specific pages for generic queries.
- Source-neutral visual output uses the canonical static fixture where the
  product permits it. Go-only and product-UI fixtures remain separate only
  where their capability requires them.

Two findings were newly reported while this change was in progress:

- Ordinary prose and the page heading now use a centered `60rem` measure when
  the viewport has room. Wide code, tables, diagrams, and generated matrices
  retain their separate wide shell. The responsive gate checks the measure and
  centering instead of accepting a left-aligned or narrow container.
- Product previews center their media and bound the primary and variant image
  heights. The visual snapshot gate rejects a preview that again occupies an
  unreasonable share of the viewport.

The reader-facing Flyway section about upgrading directories created by older
Ptah builds was also removed. Ptah is pre-GA and has no supported installed
base that needs this migration history in the task flow.

## Findings intentionally left as product issues

- [`stokaro/ptah-action#1`](https://github.com/stokaro/ptah-action/issues/1)
  owns schema-file support in the separate GitHub Action repository. Ptah
  documentation continues to describe that action as Go-specific.

## Finding changed during implementation

The starting baseline treated source-specific `ptah schema test` selectors as
an open product gap in `stokaro/ptah#2571`. That issue was implemented by
[#2591](https://github.com/stokaro/ptah/pull/2591) on the default branch while
this change was in progress. After rebasing, the how-to, OCI guidance,
source-support table, generated product-output command, and regression
contract use `--schema-file` for static files and OCI artifacts,
`--source-db-url` for a live desired schema, and `--root-dir` only for Go
annotations. The compatibility overload remains accepted by the CLI but is
not promoted in reader workflows.

## Current repository ruleset status

GitHub returned no repository ruleset for the default branch. The repository
continues to use classic branch protection.

Classic protection was updated and read back through the GitHub API on August
30, 2026. The verified response records:

- strict required-status-check updates: enabled;
- required context: `Documentation gate`;
- check source: GitHub Actions, application ID `15368`;
- linear history and conversation resolution: enabled;
- administrator enforcement: disabled;
- force pushes: allowed;
- branch deletion: disabled.

The readback proves the repository setting. PR #2593 reports
`mergeStateStatus: BLOCKED` while its required checks are incomplete; the live
aggregate check URL and conclusion are recorded in the next section.

## Required status check

Required check name: `Documentation gate`

The workflow keeps this job name stable and runs the aggregate on every pull
request. The local gate self-test passed all six path shapes required by the
hardening task. The Docs workflow for PR #2593 is
[#33317872526](https://github.com/stokaro/ptah/actions/runs/33317872526).
Its required
[`Documentation gate`](https://github.com/stokaro/ptah/actions/runs/33317872526/job/99275149038)
completed successfully after every applicable documentation and acceptance job
passed.

## Public edge source commit

The public `edge` site observed during implementation reports:

```text
documentation_version: edge
source_ref: master
source_commit: 0ef7afb42ed9f26934f3bb3d318e0d14389fb6e3
built_at: 2026-08-30T14:16:51.348Z
```

That commit predates the current default-branch base of PR #2593 and the
unmerged hardening change. It is evidence for the public site's current state,
not evidence that this implementation has deployed.

- Expected implementation commit: the merge commit of PR #2593, not yet
  created.
- Observed implementation commit: not deployed; the public site still reports
  `0ef7afb42ed9f26934f3bb3d318e0d14389fb6e3`.
- Deployment workflow run: not started because PR #2593 is unmerged.

## Public smoke-test result

Deployment run
[#33316131267](https://github.com/stokaro/ptah/actions/runs/33316131267)
published the currently observed commit; deploy job `99272175567` succeeded.
Its smoke job `99272174987` exposed the stale alternate-text selector. The
current checker uses stable visual-proof identifiers and bounded retries. A
result against the implementation commit cannot exist until PR #2593 merges;
this record does not claim one.

The checker covers `/`, `/schema/visualize/`, `/schema/document/`,
`/schema/serve/`, `/schema/security/`, `/schema/lineage/`,
`/versioned/generate/`, and `/testing/migrations-and-schema/`. It verifies the
expected build commit, required live-DOM proof, assets, full-size actions,
downloads, and generated HTML rather than accepting status codes alone.

## Command-by-source manifest status

`docs/source-support.json` remains the machine-readable command-by-source
authority. It distinguishes accepted, verified, supported-but-unverified,
unsupported, equivalent-only, and not-applicable cells across eleven source
categories. Each cell names its invocation, implementation owner, evidence,
composability, external-execution policy, and limitation.

The canonical library fixture renders from SQL, YAML, HCL, DBML, Go
annotations, explicit external SQL, and configured external SQL. The
equivalence gate materializes each rendering in SQLite and compares normalized
inspection output. Source-workflow tests exercise the consuming commands
instead of treating loader registration as command evidence.

The new page contract found and checked 40 authored pages that consume schema
sources. Its 12 accepted and rejected fixtures cover source-neutral, Go-only,
live-database-only, and command-specific pages, including early limitation and
manifest-consistency failures.

## Visual-output inventory status

The current visual contract declares 27 assets, ten output-producing
capabilities, and 11 enforced product proofs. It covers schema visualization,
security, generated schema documentation, the live schema server, migration
safety, migration and schema test reports, lineage, statistics, and API
contracts.

Every proof names its route, command, fixture, generator, primary asset,
variants, downloads, theme behavior, placement, acceptance test, and owner.
The regenerated source-neutral outputs use the common SQL fixture; separate
Go and UI fixtures carry a recorded reason. `ProductPreview` exposes captions,
alternatives, full-size and download actions, and reproduction information.

The layout keeps the wide content shell while centering ordinary prose and the
page heading within `60rem`. Preview media now use intrinsic sizing, centered
placement, and viewport-aware height caps. The visual asset contract
passed. The final local browser run produced 68 light/dark mobile/desktop
snapshots for 17 routes and enforced 11 manifest-backed product proofs. The
responsive run rendered all 126 routes at three viewports and verified the
centered 960px wide-screen prose target. Public proof for the implementation
commit remains a post-merge deployment check.

## Portable inference fixture result

The generated archive is published as
`samples/inference-quick-start.zip`, with
`samples/inference-quick-start.zip.sha256` beside it. The current deterministic
SHA-256 is:

```text
7276b6399f0d870e6c6fcc0ebce3ca08a6d6fa78d0113c73d97cf15512d25783
```

The archive contains Compose configuration, database initialization, pgvector,
the deterministic embeddings provider and Dockerfile, the inference
specification, Bash and PowerShell lifecycle helpers, cleanup helpers, and a
reader README. Host ports, the Docker context, and the Compose project name are
configurable.

The isolated acceptance copied only a built `ptah` binary and the archive into
a temporary directory. It completed plan, prepare, backfill, catch-up, index,
verification, digest-bound approval, cutover, and active-pointer verification
twice through the explicit `remote-dev-container` Docker context. Cleanup left
no named container, image, network, or volume. PowerShell syntax was checked in
a disposable PowerShell container. Publication and download checks for the
implementation archive remain a post-merge deployment check.

## Version-aware page-action result

The shared resolver produces the documentation version, exact view reference,
latest edit reference, rendered source path, edit source path, generator path,
and generated state.

- Authored `edge` pages edit the latest page and view the exact built commit
  when provenance is available.
- Authored release pages view the release tag and label the `master` action as
  editing the latest documentation.
- Generated `edge` pages edit the generator source and view generated output;
  they never offer a direct edit of generated Markdown.
- Generated release pages view generated output at the release tag and label
  generator edits as changes to the latest documentation.

Issue reports include the rendered URL, documentation version, exact source
reference, rendered Markdown path, generator, and edit source as applicable.
The mutable-link check rejects unlabeled `master` links that claim to represent
a released page or versioned artifact.

Component and browser tests passed for authored and generated `edge` pages,
exact source references, issue bodies, generator routing, versioned sample
downloads, and the absence of a generated-Markdown edit action. The maintained
six-file UI overlay was also applied to the immutable `v0.3.0` content at
`9278f8566c88c3bf949bd3c5cd22fad1d37006b4`; its Astro build and authored and
generated release-action browser cases passed.

## Editorial ratchet result

The editorial checker reports zero unwaived findings and 12 active waivers.
Its nine self-test assertions prove that a new unwaived page-length,
generic-introduction, mixed-type, near-duplicate, or duplicate-tab finding
fails. Waivers remain exact route/check/target records with a substantive
reason; a stale waiver fails.

The checker no longer prints success while carrying a new unwaived warning.
No directory wildcard, inherited legacy exemption, or arbitrary baseline file
was introduced.

## Metadata validation result

One shared semantic validator now serves the Astro content schema, generated
content inventory, and standalone metadata check. It enforces:

- real `lastVerified` calendar dates that are not later than the build date;
- reader questions that end with a question mark;
- repository paths that resolve to an existing file or directory;
- distinct GitHub issue, external URL, repository identifier, named evidence,
  and generated-evidence forms;
- generator and edit-source metadata only on generated pages;
- generated-page requirements for generator and edit source;
- the absence of the retired `lengthWaiver` field;
- valid source-mode values and source-consuming page contracts.

The inventory self-test passed 24 assertions. Invalid dates, future dates,
misspelled local paths, external generator/edit-source values, empty metadata
entries, malformed questions, dead fields, and misplaced generated metadata
fail through the same implementation used by the site build.

## Search queries tested

All 36 Pagefind cases returned their canonical page in the first three
results. Thirty-four ranked first; `Go annotations` ranked second and `MCP`
ranked third.

The tested queries were:

- onboarding and workflows: `install Ptah`, `first migration`,
  `generate migration`, `apply migrations`, `rollback migration`,
  `schema drift`, `apply desired schema`, `adopt existing database`, and
  `migrate from Atlas`;
- non-Go sources: `generate migrations from SQL`,
  `generate migrations from YAML`, `generate migrations from HCL`,
  `generate migrations from DBML`, `generate migration without Go`,
  `adopt database as SQL`, `adopt database as HCL`,
  `adopt database as DBML`, `test YAML schema`, `test DBML schema`,
  `external schema loader`, `OCI schema source`, and
  `use Ptah Action with schema file`;
- product and support lookup: `retained divergences`, `checksum mismatch`,
  `PostgreSQL extension`, `SQL Server support`, `MySQL supported versions`,
  `pgvector`, `change embedding model`, `resume inference migration`, `MCP`,
  `visualize schema`, `generate protobuf`, and `exit code 2`;
- Go-specific controls: `Go annotations` and
  `generate migration from Go structs`.

The generic `generate migration`, `schema drift`, and `apply desired schema`
cases additionally require the source-neutral workflow page to outrank the
Go-annotation page.

## Manual browser routes reviewed

The final local browser run captured all routes below at both required
viewports and in light and dark themes. The generated screenshots were reviewed
for the centered prose measure and heading, bounded preview media, wide-content
containment, readable mobile reflow, and the absence of document-level
horizontal overflow. The geometry assertions independently covered all 126
routes at three viewports. An interactive Chrome connection was unavailable,
so this record does not claim an interactive hosted-site review.

The final review matrix is:

- viewports: `390x844` and `1440x900`;
- themes: light and dark;
- routes: `/`, `/start/choose-a-workflow/`, `/inference/quick-start/`,
  `/schema/visualize/`, `/schema/document/`, `/schema/serve/`,
  `/schema/security/`, `/versioned/generate/`,
  `/testing/migrations-and-schema/`, and
  `/atlas/retained-divergences/`.

The review confirmed the centered `60rem` prose measure, centered page heading,
bounded diagram previews, local wide-content overflow, and the absence of
document-level horizontal overflow. Separate navigation checks exercised all
four page-action modes and keyboard access.

## Remaining waivers

Twelve page-length waivers remain. Each is specific and stale-checked:

- generated or measured lookup:
  `/atlas/feature-matrix/`, `/reference/atlas-commands/`,
  `/reference/command-flags/`, and `/reference/native-commands/`;
- command-family reference: `/atlas/migrate-commands/` and
  `/atlas/schema-commands/`;
- one configuration or engine reference: `/atlas/project-config/` and
  `/databases/postgresql/`;
- dated compatibility evidence: `/atlas/retained-divergences/`;
- one stateful workflow or trust boundary: `/schema/protobuf/`,
  `/versioned/apply/`, and `/versioned/integrity-and-safety/`.

The route `/atlas/retained-divergences/` is preserved for compatibility; its
reader-facing title is **Compatibility differences**.

## Remaining product gaps

- `stokaro/ptah-action` remains Go-specific; schema-file support is tracked in
  [`stokaro/ptah-action#1`](https://github.com/stokaro/ptah-action/issues/1).
- `ptah viz` and `ptah schema serve` remain Go-annotation-only. Their pages
  state the limit near the beginning and link to the nearest source-neutral
  output path where one exists.
- Accepted source transport does not imply equal format expressiveness. Some
  formats cannot represent every Ptah schema object.
- Some supported command-by-source cells still lack focused command-level
  tests. The manifest reports them as supported-but-unverified rather than
  upgrading shared-loader evidence into a verified claim.

No product behavior was broadened to make this documentation change appear
complete.
