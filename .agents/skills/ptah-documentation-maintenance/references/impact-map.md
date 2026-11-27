# Documentation impact map

Read the sections that match the change. Search all affected surfaces; there is
no need to read unrelated pages or load the complete documentation tree.

- [Change class](#start-with-the-change-class)
- [Affected surfaces](#build-the-documentation-impact-map)
- [Package and feature routes](#ptah-specific-routing)
- [Search terms](#search-deeply)
- [Product and compatibility claims](#ptah-invariants)

## Start with the change class

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

## Build the documentation impact map

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
- Agent workflow: `AGENTS.md`, `CLAUDE.md`, and `.agents/skills/` and `.claude/skills/` when agent
  behavior changes.
- Release, CI, and operational docs when workflows, checks, or deployment
  behavior changes.
- `docs/site/CONTENT_INVENTORY.md` when an editorial decision or reader
  journey changes. The page inventory itself is computed, so nothing needs
  regenerating when pages, metadata, navigation, or links change.

Do not stop at the first matching file. Ptah often has both a terse root
entrypoint and deeper reference/site pages for the same behavior.

## Ptah-specific routing

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

## Search deeply

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

## Ptah invariants

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
