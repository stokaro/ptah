# Complete documentation validation suite

Use this reference when reproducing a docs CI job or collecting complete local
validation evidence. The skill's local feedback pass selects relevant checks;
CI and the merge requirements still cover every applicable job.

- [Prerequisites](#prerequisites)
- [Complete suite](#complete-suite)
- [Evidence](#evidence)

## Prerequisites

Read `.github/workflows/docs.yml` for job conditions and setup. Run commands
from the repository root until the block changes directory. Build the required
binaries before checks that use them. Install site dependencies once when a
selected check needs them; the standalone style scripts need no install.

Replace `<context>` with a Docker context for disposable fixtures. Tutorial
scripts remove only their named Compose project and locally built images.
Browser checks require Playwright Chromium. Generated diagrams require Linux
text metrics or the documented container. A missing service or browser is an
unverified check; it does not establish that the content passes.

## Complete suite

`check-skill-suite.mjs` reads this fixed file and compares the marked block with
the workflow in both directions. Keep every applicable invocation, including
its mode flags. Deployment-only exceptions live in the checker with reasons.

<!-- documentation-suite -->

```bash
# Agent guidance needs only Node and Git; run it before importing provider pages.
node docs/site/scripts/check-agent-guidance.mjs --selftest
node docs/site/scripts/check-agent-guidance.mjs
# The provider pages, imported from each provider repository at the release
# docs/site/scripts/data/provider-docs.json names. Every check below reads
# them, the Go ones included.
node docs/site/scripts/import-provider-docs.mjs --selftest
node docs/site/scripts/import-provider-docs.mjs
# From the repository root: the Go and shell checks of the Docs workflow.
go test ./internal/doccommands -count=1
go test ./docs ./internal/docsembed -count=1
go test ./internal/cli/atlas/internal/migratetests ./internal/cli/internal/terminologyguard ./internal/cli/oci ./internal/cli/root ./core/query ./internal/agentapi ./internal/agentdiag ./internal/assistloop ./internal/atlascompatpolicy ./internal/atlasfilter ./internal/lintcatalog ./internal/parser ./migration/migrator -count=1
go test ./internal/mcpserver -run '^(TestSearchDocs_|TestDescribeSession_ReportsWhatTheDocumentationToolCanReach$|TestToolDocs_)' -count=1
go test ./internal/cmdref -count=1
scripts/check-feature-inventory.sh
scripts/check-docsync.sh
scripts/check-examples.sh
# These need ptah built into bin/.
PTAH_BIN=bin/ptah scripts/check-source-equivalence.sh
PTAH_BIN=bin/ptah scripts/check-source-equivalence-selftest.sh
PTAH_BIN=bin/ptah scripts/check-source-workflows.sh
# The tutorials start their fixtures on a Docker context. On a remote one,
# also set PTAH_FIXTURE_HOST to the host that publishes the ports.
PTAH_DOCKER_CONTEXT=<context> docs/site/scripts/check-inference-quick-start.sh
PTAH_DOCKER_CONTEXT=<context> docs/site/scripts/check-postgresql-quick-start.sh
PTAH_DOCKER_CONTEXT=<context> docs/site/scripts/check-zero-downtime-changes.sh
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
node scripts/build-ydb-walkthrough.mjs --selftest
node scripts/build-ydb-walkthrough.mjs --check
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

## Evidence

Run each applicable check with its self-test where the workflow requires it.
Record the command, result, tree revision, and missing prerequisites. Reuse
successful evidence only while its inputs and environment are unchanged.
Do not label a focused local pass as the complete suite. Required CI failures
remain failures even when the local checks pass.

Never weaken a check to make content pass. If correct content exposes a defect
in a checker, fix that checker and add the case to its self-test. Search for
stale links, retired command spellings, and unsupported claims after validation;
structural checks cannot establish whether every sentence is true.
