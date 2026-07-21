---
name: ptah-documentation-maintenance
description: Use when updating Ptah documentation after changes to CLI behavior, config, generated SQL, migrations, parsers/renderers, public Go APIs, examples, conformance status, Atlas parity, or agent workflow. Also use for deep documentation audits that must scan all affected Markdown and code documentation instead of only the nearest README.
---

# Ptah Documentation Maintenance

Use this skill for Ptah documentation work that must stay aligned with current
behavior. The goal is not a checklist-shaped pass over nearby files; it is a
reader-focused audit across every affected documentation surface.

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
- **Public API behavior**: exported Go packages, extension points, testkit
  behavior, capability APIs, or documented embedding surfaces.
- **Conformance and Atlas parity**: `ptah atlas <command> ...`, conformance
  results, known gaps, clean-room/license boundary, or drop-in replacement
  claims.
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
  `migration/generator/README.md`, `migration/migrator/README.md`,
  `testkit/README.md`, and package comments when exported APIs change.
- Agent workflow: `AGENTS.md` and `.agents/skills/**/SKILL.md` when agent
  behavior changes.
- Release, CI, and operational docs when workflows, checks, or deployment
  behavior changes.

Do not stop at the first matching file. Ptah often has both a terse root
entrypoint and deeper reference/site pages for the same behavior.

## Ptah-Specific Routing

Use these routes to avoid missing a class of docs:

- **Native CLI**: `README.md`, `docs/native_cli.md`,
  `docs/site/src/content/docs/reference/commands.md`,
  `docs/site/src/content/docs/workflows/*.md`, and `docs/exit_codes.md`.
- **Atlas-compatible CLI and parity claims**: `README.md`, `docs/conformance.md`,
  `docs/site/src/content/docs/index.mdx`,
  `docs/site/src/content/docs/documentation-map.md`,
  `docs/site/src/content/docs/workflows/atlas-cli.md`,
  `docs/site/src/content/docs/reference/commands.md`,
  `docs/site/src/content/docs/reference/comparison.md`,
  `docs/site/src/content/docs/reference/exit-codes.md`, `docs/exit_codes.md`,
  `docs/site/src/content/docs/operate/conformance.md`, and
  `docs/site/src/content/docs/operate/license-boundary.md`.
- **Config and `atlas.hcl`**: `docs/project_config.md`,
  `docs/atlas_project_config.md`, `docs/atlas_hcl_schema.md`,
  `docs/site/src/content/docs/reference/configuration.md`, and related CLI
  workflow pages.
- **Go annotations, YAML, and Atlas HCL schema sources**:
  `docs/go_annotations_vs_atlas_hcl.md`, `docs/yaml_schema.md`,
  `docs/site/src/content/docs/workflows/go-schema.md`,
  `docs/site/src/content/docs/workflows/schema-files.md`, and matching example
  pages.
- **Migrations, sums, directives, revision tables, and safety**:
  `migration/migrator/README.md`, `migration/generator/README.md`,
  `docs/site/src/content/docs/workflows/migrations.md`,
  `docs/site/src/content/docs/examples/atlas-migrations.md`, and
  `docs/site/src/content/docs/operate/troubleshooting.md`.
- **Parser/renderer or dialect behavior**: `internal/parser/README.md`,
  dialect docs such as `docs/sqlite.md` and `docs/sqlserver.md`,
  capability docs, and schema example pages.
- **Capabilities**: `docs/capabilities.md`,
  `docs/site/src/content/docs/reference/capabilities.md`, and any dialect or
  conformance pages that mention the changed capability.
- **Public Go API or testkit**: `docs/public_api.md`,
  `docs/site/src/content/docs/reference/*.md`, `testkit/README.md`, and package
  comments for exported identifiers.
- **GitHub Action, CI, or generated reports**: `docs/github_action.md`,
  `docs/site/src/content/docs/workflows/ci.md`, `docs/release_process.md`, and
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
rg -n "migrate apply|schema inspect|ptah atlas|atlas_schema_revisions" README.md docs examples integration
rg -n "MY_FLAG|old-config-key|new-config-key" --glob '*.md'
rg -n "Exact error text|Exact output label" --glob '*.md' --glob '*.go'
```

## Quality Bar

Use Inventario's docs as the quality reference, especially:

- `/Users/buster/Work/denis/inventario/docs/site/src/content/docs/index.mdx`
- `/Users/buster/Work/denis/inventario/docs/site/src/content/docs/getting-started.md`
- `/Users/buster/Work/denis/inventario/docs/site/src/content/docs/self-hosting.md`
- `/Users/buster/Work/denis/inventario/docs/site/src/content/docs/backup-and-restore.md`
- `/Users/buster/Work/denis/inventario/docs/site/src/content/docs/groups-and-sharing.md`
- `/Users/buster/Work/denis/inventario/docs/site/src/content/docs/reports.md`

Match the discipline, not the product structure exactly:

- Start with what the reader is trying to do.
- Include concrete, copy-pasteable commands where a command is useful.
- Show expected output or verification commands when the result can be checked.
- Use comparison tables for command parity, feature support, config precedence,
  or dialect behavior when prose would be ambiguous.
- Add troubleshooting notes for likely failure modes.
- Link to related docs, examples, issues, or tests instead of duplicating large
  blocks.
- Keep American English spelling.

## Ptah Invariants

Preserve these statements unless current evidence changes:

- Ptah is pre-GA. Do not document legacy aliases or backward-compatibility
  wrappers as supported behavior.
- Atlas OSS command parity belongs under `ptah atlas <command> ...`; do not
  document root-level Atlas aliases.
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
  binary.
- For config docs, test parse/validation behavior with representative fixtures.
- For generated SQL or migration docs, generate or apply a small fixture when
  feasible.
- For public Go API docs, run the relevant guards:

  ```bash
  scripts/check-public-api.sh
  scripts/check-public-api-snapshot.sh
  scripts/check-public-api-released.sh
  ```

- For docs site changes, run:

  ```bash
  cd docs/site
  npm ci
  npm run build
  npm run versions:selftest
  npm audit --audit-level=low
  ```

- Run Markdown/link checks when the project has a checker available. If there is
  no checker, at least run `rg` searches for stale links, old command names, and
  unsupported claims.

## Self-Review

Before opening or updating a PR, perform two passes:

1. **Coverage pass**: for each change class, list which docs were checked,
   changed, or intentionally left unchanged.
2. **Truth pass**: verify examples, command paths, claims, links, American
   English spelling, and absence of legacy aliases or unsupported parity claims.

The PR should summarize the documentation impact map and validation commands.
