---
name: ptah-documentation-maintenance
description: Use when updating Ptah documentation after changes to CLI behavior, config, generated SQL, migrations, parsers/renderers, public Go APIs, examples, conformance status, Atlas parity, or agent workflow. Also use for deep documentation audits that must scan all affected Markdown and code documentation instead of only the nearest README.
---

# Ptah Documentation Maintenance

Use this skill for Ptah documentation work that must stay aligned with current
behavior. The goal is not a checklist-shaped pass over nearby files; it is a
reader-focused audit across every affected documentation surface.

## Load The Style Guide First

Read `docs/STYLE_GUIDE.md` before writing or editing any documentation. It is
authoritative for page taxonomy, page templates, voice and language, canonical
Ptah terminology, example conventions, admonition rules, link rules, anti-slop
rules, and the per-PR review checklist. Do not restate its rules from memory;
follow the file.

Before writing, classify the page type (landing, tutorial, concept, how-to,
reference, troubleshooting, status, or contributor) and use the matching
template from the style guide. One primary type per page; if a page needs two
types, it is two pages.

## Maintain The Content Inventory

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
are review warnings. Keep a deliberate warning quiet only through a reasoned
entry in `scripts/data/editorial-waivers.json`; the check rejects stale
waivers.

## Preserve The Navigation Contract

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

## Start With The Change Class

Classify the change before editing docs:

- **CLI/user-visible behavior**: command tree, command names, flags, environment
  variables, help text, output formats, exit codes, warnings, errors, or safety
  prompts.
- **Config behavior**: `ptah.yaml`, `.ptah-lint.yaml`, `atlas.hcl`, project
  config, environment selection, defaults, validation, or precedence.
- **SQL/parser/migration behavior**: generated SQL, parsed SQL, migration file
  layout, directives, revision tables, hash files, validation, repair, rollback,
  dialect support, or destructive-safety behavior.
- **Public API behavior**: exported Go packages, extension points, capability
  APIs, or documented embedding surfaces.
- **Conformance and Atlas parity**: the `ptah-compat` drop-in binary,
  conformance results, known gaps, clean-room/license boundary, or drop-in
  replacement claims.
- **Examples and operational tooling**: runnable examples, CI workflows,
  GitHub Action behavior, integration test docs, database setup, or release
  process.
- **Code-internal only**: refactors that do not alter public behavior,
  generated artifacts, accepted inputs, user-facing output, or documented
  workflows. Still confirm this in self-review.

## Build The Documentation Impact Map

Inspect every surface that can be stale for the change class:

- Root entrypoints: `README.md`, `docs/README.md`, `docs/site/README.md`.
- Detailed references: `docs/*.md`.
- Starlight site: `docs/site/src/content/docs/`.
- Examples: `examples/**/README.md` and generated artifacts committed under
  `examples/**`.
- Integration docs: `integration/*.md`.
- Package documentation: `internal/parser/README.md`,
  `migration/generator/README.md`, `migration/migrator/README.md`, and package
  comments when exported APIs change.
- Agent workflow: `AGENTS.md` and `.agents/skills/**/SKILL.md` when agent
  behavior changes.
- Release, CI, and operational docs when workflows, checks, or deployment
  behavior changes.
- `docs/site/CONTENT_INVENTORY.md` when an editorial decision or reader
  journey changes. The page inventory itself is computed, so nothing needs
  regenerating when pages, metadata, navigation, or links change.

Do not stop at the first matching file. Ptah often has both a terse root
entrypoint and deeper reference/site pages for the same behavior.

## Ptah-Specific Routing

Use these routes to avoid missing a class of docs:

- **Native CLI**: `README.md`,
  `docs/site/src/content/docs/reference/native-commands.md`, the
  `docs/site/src/content/docs/versioned/`, `direct/`, `testing/`, and
  `operate/` workflow pages, and `docs/exit_codes.md`.
- **Atlas-compatible CLI and parity claims**: `README.md`, `docs/conformance.md`,
  `docs/site/src/content/docs/index.mdx`,
  `docs/site/src/content/docs/atlas/overview.md`,
  `docs/site/src/content/docs/atlas/strict-ce-mode.md`,
  `docs/site/src/content/docs/atlas/migrate-commands.md`,
  `docs/site/src/content/docs/atlas/schema-commands.mdx`,
  `docs/site/src/content/docs/atlas/output-and-redaction.md`,
  `docs/site/src/content/docs/reference/atlas-commands.md`,
  `docs/site/src/content/docs/atlas/feature-matrix.md` (generated from
  `docs/site/scripts/data/feature-matrix-rows.json`),
  `docs/site/src/content/docs/atlas/retained-divergences.md`,
  `docs/site/src/content/docs/reference/exit-codes.md`, `docs/exit_codes.md`,
  `docs/site/src/content/docs/atlas/conformance.md`, and
  `docs/site/src/content/docs/atlas/license-boundary.md`.
- **Config and `atlas.hcl`**: `docs/project_config.md`,
  `docs/atlas_project_config.md`, `docs/atlas_hcl_schema.md`,
  `docs/site/src/content/docs/reference/configuration.md`,
  `docs/site/src/content/docs/atlas/project-config.md`, and related CLI
  workflow pages.
- **Go annotations, YAML, and Atlas HCL schema sources**:
  `docs/yaml_schema.md`, `docs/atlas_hcl_schema.md`,
  `docs/site/src/content/docs/schema/go-annotations.md`,
  `docs/site/src/content/docs/reference/go-annotations.md`, and the
  `docs/site/src/content/docs/schema/` source pages (`yaml.md`, `hcl.md`,
  `sql.md`, `orm-and-external.md`, `composite.md`).
- **Migrations, sums, directives, revision tables, and safety**:
  `migration/migrator/README.md`, `migration/generator/README.md`, the
  `docs/site/src/content/docs/versioned/` lifecycle pages (`overview.md`,
  `generate.md`, `apply.md`, `rollback.md`, `integrity-and-safety.md`,
  `maintain-history.md`, `import.md`, `checkpoints.md`, `reference-data.md`),
  `docs/site/src/content/docs/concepts/migration-directory.md`,
  `docs/site/src/content/docs/atlas/migrate-commands.md`, and
  `docs/site/src/content/docs/operate/troubleshooting.md`.
- **OCI artifacts and seeds**: `docs/oci_registry.md`,
  `docs/site/src/content/docs/operate/oci-registry.md`, and
  `docs/site/src/content/docs/operate/seed-data.md`.
- **Parser/renderer or dialect behavior**: `internal/parser/README.md`,
  dialect docs such as `docs/sqlite.md` and `docs/sqlserver.md`, the
  `docs/site/src/content/docs/databases/` engine pages, capability docs, and
  schema source pages.
- **Capabilities**: `docs/capabilities.md`,
  `docs/site/src/content/docs/reference/capabilities.md`,
  `docs/site/src/content/docs/concepts/dialects-and-capabilities.md`,
  `docs/site/src/content/docs/databases/support-matrix.md`,
  `docs/site/src/content/docs/databases/support-policy.md`,
  `docs/site/src/content/docs/databases/support-evidence.md`, and any dialect or
  conformance pages that mention the changed capability.
- **Assist and MCP agent behavior**: the `operate/ai-*` workflow pages,
  `docs/site/src/content/docs/reference/mcp-tools.md`, the native command and
  flag references, `internal/cli/assist`, `internal/cli/mcp`, and the `internal/assist*`,
  `internal/aiprovider`, `internal/agent*`, and `internal/mcpserver` packages.
- **Public Go API**: `docs/public_api.md`,
  `docs/site/src/content/docs/extend/*.md`, and package comments for exported
  identifiers. A change an embedder sees may also need one in
  [stokaro/ptah-testkit](https://github.com/stokaro/ptah-testkit), which is a
  separate repository and is not edited from here.
- **GitHub Action, CI, or generated reports**: `docs/github_action.md`,
  `docs/site/src/content/docs/testing/ci.md`, `docs/release_process.md`, and
  integration docs.

## Search Deeply

Use `rg` over Markdown and package docs before finishing. Search both old and
new terms:

- command paths, old aliases, new names, and Atlas spellings;
- flag names, environment variables, config keys, and default values;
- issue numbers, feature names, dialect names, capability names, and conformance
  gap labels;
- generated output labels, exact warning/error text, safety messages, and exit
  descriptions;
- names of examples, fixtures, migrations, reports, and workflow files.

Prefer targeted searches, for example:

```bash
rg -n "migrate apply|schema inspect|ptah-compat|atlas_schema_revisions" README.md docs examples integration
rg -n "MY_FLAG|old-config-key|new-config-key" --glob '*.md'
rg -n "Exact error text|Exact output label" --glob '*.md' --glob '*.go'
```

## Quality Bar

The binding rules are in `docs/STYLE_GUIDE.md`. Use Inventario's docs as an
additional quality reference for tone and structure, especially:

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

## Ptah Invariants

Preserve these statements unless current evidence changes:

- Ptah is pre-GA. Do not document legacy aliases or backward-compatibility
  wrappers as supported behavior.
- Atlas OSS command parity belongs in the separate `ptah-compat` drop-in
  binary; do not document Atlas aliases inside the native `ptah` binary.
- Ptah is a clean-room implementation and does not use Atlas source code.
- Do not claim full Atlas parity or drop-in replacement status unless current
  conformance evidence proves it.
- If docs describe conformance gaps, keep them specific and tied to current
  evidence.

For Atlas parity or drop-in replacement claims, current evidence means the
latest checked state of `stokaro/ptah-atlas-conformance`, especially `gaps.md`,
`gaps-live.md`, `gaps-diff.md`, and `PARITY.md`, or freshly run relevant targets:
`make probe*`, `make gate*`, or `make budget*`. Record the checked commit,
report file, or command output in the PR.

## Validate Against Current Behavior

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

- For any documentation change, run the complete suite. It is every check
  `.github/workflows/docs.yml` runs over the working tree, grouped by the job
  that runs it, and `check-skill-suite.mjs` fails when the two disagree, so a
  local failure is a CI failure. The checks that need a built site follow
  `npm run build`; the deployment's own steps are not here, and the guard names
  each one with the reason.

  <!-- documentation-suite -->

  ```bash
  # From the repository root: the Go steps of the Documentation style job.
  go test ./internal/doccommands -count=1
  cd docs/site
  npm ci
  # Documentation style. None of these needs the install above.
  node scripts/check-style.mjs --selftest
  node scripts/check-style.mjs
  node scripts/check-terminology.mjs --selftest
  node scripts/check-terminology.mjs
  node scripts/check-translations.mjs --selftest
  node scripts/check-translations.mjs
  node scripts/check-limitations.mjs --selftest
  node scripts/check-limitations.mjs
  node scripts/check-implementation-chronology.mjs --selftest
  node scripts/check-implementation-chronology.mjs
  node scripts/check-skill-suite.mjs --selftest
  node scripts/check-skill-suite.mjs
  node scripts/build-content-inventory.mjs --selftest
  node scripts/check-editorial-shape.mjs --selftest
  node scripts/check-editorial-shape.mjs
  node scripts/check-examples.mjs --selftest
  node scripts/check-examples.mjs
  node scripts/check-visual-assets.mjs --selftest
  node scripts/check-visual-assets.mjs
  node scripts/check-support-matrix.mjs --selftest
  node scripts/check-support-matrix.mjs
  node scripts/build-source-support.mjs --selftest
  node scripts/build-source-support.mjs
  node scripts/check-documentation-gate.mjs --selftest
  node scripts/check-deployment-candidate.mjs --selftest
  node scripts/check-build-info.mjs --selftest
  node scripts/write-build-info.mjs --selftest
  node scripts/check-public-docs.mjs --selftest
  node scripts/apply-release-ui-overlay.mjs --selftest
  node scripts/apply-docs-address.mjs --selftest
  node scripts/check-release-page-actions.mjs --selftest
  node scripts/check-mutable-source-links.mjs --selftest
  node scripts/check-mutable-source-links.mjs
  node scripts/check-source-page-contracts.mjs --selftest
  node scripts/check-source-page-contracts.mjs
  node scripts/build-inference-quick-start.mjs --selftest
  node scripts/build-inference-quick-start.mjs
  node scripts/check-matrix-verdict-prose.mjs --selftest
  node scripts/check-matrix-verdict-prose.mjs
  node scripts/check-matrix-citations.mjs --selftest
  node scripts/check-matrix-citations.mjs
  node scripts/check-matrix-flag-names.mjs --selftest
  node scripts/check-matrix-flag-names.mjs
  node scripts/build-feature-matrix.mjs --check
  node scripts/publish-root-assets.mjs --selftest
  node scripts/check-pages-root.mjs --selftest
  node scripts/check-pages-root.mjs
  # Build versioned site.
  npm run versions:selftest
  node scripts/release-doc-tags.mjs --selftest
  node scripts/check-route-retirement.mjs --against origin/master
  npm run check:links:selftest
  npm run check:links
  npm run check:docroutes:selftest
  npm run check:redirects:selftest
  npm run check:redirects
  npm run check:route-retirement:selftest
  npm run check:route-retirement
  npm run check:core-doc-links:selftest
  npm run check:core-doc-links
  npm run check:page-health:selftest
  npm run check:page-health
  npm run check:content-inventory:selftest
  npm run check:exit-codes:selftest
  npm run check:exit-codes
  # Journey SVGs use Linux text metrics, so off Linux this refuses to run;
  # run the same check in a container instead:
  # PTAH_DOCKER_CONTEXT=<context> npm run diagrams:docker -- --check
  npm run check:journey-diagrams
  npm run check:journey-themes:selftest
  npm run check:responsive:selftest
  npm run check:glossary-coverage:selftest
  npm run check:accessibility:selftest
  npm run check:visual-snapshots:selftest
  npm run check:graphic-preview:selftest
  npm run check:version-picker:selftest
  npm run check:navigation:selftest
  npm run check:search-ranking:selftest
  npm run check:heading-anchors:selftest
  npm run check:faq-groups:selftest
  npm run check:faq-page:selftest
  npm run build
  npm run check:journey-themes
  # Renders every built page at 390px and 1280px and fails on horizontal
  # overflow. Requires Playwright's chromium (npx playwright install
  # chromium); without it the check skips locally and fails in CI.
  npm run check:responsive
  npm run check:glossary-coverage
  npm run check:accessibility
  npm run check:visual-snapshots -- --output /tmp/ptah-docs-snapshots
  npm run check:version-picker
  npm run check:navigation
  npm run check:search-ranking
  npm run check:heading-anchors
  npm run check:faq-groups
  npm run check:faq-page
  npm run check:graphic-preview -- --output /tmp/ptah-docs-graphic-preview
  npm run check:build-info
  # Generated documentation: needs ptah built into ../../bin.
  node scripts/build-source-support.mjs --binary ../../bin/ptah
  # Generated assets: needs ptah and ptah-compat built into ../../bin, and a
  # Docker context for the Graphviz renderer. Off Linux it also rewrites the
  # Mermaid SVGs and screenshots under src/assets with this host's text
  # metrics; nothing compares those, so restore them unless you meant to
  # change them.
  npm run check:generated-assets:selftest
  PTAH_DOCKER_CONTEXT=<context> PTAH_BIN=../../bin/ptah PTAH_COMPAT_BIN=../../bin/ptah-compat npm run check:generated-assets
  npm audit --audit-level=low
  ```

  The Test documentation contracts step of the style job also runs a list of
  Go packages that read pages as test inputs; it runs only when a page they read
  changed, so read that step for the packages rather than a copy here.

  From the repository root, also run `scripts/check-examples.sh`. When the
  inference tutorial or its fixture changes, run
  `docs/site/scripts/check-inference-quick-start.sh` with an explicit
  `PTAH_DOCKER_CONTEXT`. For a remote context, also set `PTAH_FIXTURE_HOST` to
  the remote host name so the locally built CLI reaches the published ports.
  The script removes only its named Compose project and locally built images.

  `check:style` covers every layer the style guide governs — `docs/site`,
  `docs/*.md`, `examples/**`, `integration/*.md`, every package `README.md`,
  and `AGENTS.md` — so run it even when the change touches no site page. It has
  no npm dependencies, so `node docs/site/scripts/check-style.mjs` works from a
  bare checkout; CI runs it that way in its own job. Section 16 of
  `docs/STYLE_GUIDE.md` lists which rules these checks enforce; anything not in
  that table is your responsibility to review by reading.

  Never weaken a check to make a change pass. If a rule fires on correct
  content, fix the rule and add the case to that check's `--selftest`, so the
  next agent inherits the decision instead of rediscovering it.

  Two of those rules govern table density, and the fix is almost never to
  shorten the sentence. A row that needs a paragraph wants the table to become
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

- After the suite passes, `rg` for stale links, retired command spellings, and
  unsupported claims. The checks verify structure, not whether a sentence is
  still true.

## Self-Review

Before opening or updating a PR, perform three passes:

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
