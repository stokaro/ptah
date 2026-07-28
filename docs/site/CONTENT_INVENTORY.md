# Documentation content inventory

This is the committed audit for the documentation restructuring tracked in
[stokaro/ptah#804](https://github.com/stokaro/ptah/issues/804). It records, for
every reader-facing documentation surface: audience, the reader question the
page answers, page type, source of truth, overlaps, and disposition. It also
records the reader journeys, the terminology audit, the target navigation with
rationale, verified missing content, and the Atlas-pattern decisions behind the
target structure.

Audited repository state: `master` at `f5c59b5` ("Forward atlas migrate test
and schema test to native test runners"). Every command claim below was checked
against the command tree in `cmd/` at that commit, not copied from earlier
analysis.

Page types used below: `concept`, `tutorial`, `howto`, `reference`,
`troubleshooting`, `status` (compatibility/evidence pages), `navigation`
(routing-only pages), and `contributor` (repo-layer docs that stay out of
reader navigation).

Dispositions used below: `keep`, `rewrite` (in place), `split`, `merge` (into a
named absorbing page), `move` (URL changes, old URL redirects), and `retire`
(page removed; content and URL have named successors).

## Maintenance rule

- Owner: Ptah maintainers, enforced through
  `.agents/skills/ptah-documentation-maintenance/SKILL.md`.
- Any PR that adds, moves, merges, splits, or retires a reader-facing page must
  update this inventory in the same PR. "Reader-facing" means anything under
  `docs/site/src/content/docs/`, `docs/*.md`, `examples/**` READMEs, or
  `integration/*.md`.
- Dispositions in this file describe the target end state for #804. As
  restructuring PRs land, rows should be updated to `done (new path)` rather
  than deleted, so the file stays a truthful map of where content went.

## 1. Site pages (`docs/site/src/content/docs/`)

37 pages total: 3 top-level entries, 11 under `Use Ptah`, 5 under `Examples`,
14 under `Reference`, 3 under `Operate`, plus the `index.mdx` splash page.
Word counts were measured with `wc -w` at the audited commit.

### Top level

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `index.mdx` (531) | everyone | What is Ptah and where do I start? | navigation | this page | `documentation-map` (routing), root `README.md` "Start Here", layers table duplicated on `documentation-map` | rewrite in place: regroup cards and "Choose your path" around the target groups (final PR of the migration) |
| `getting-started.md` (788) | new Go user | How do I try Ptah end to end locally? | tutorial | this page (runnable SQLite flow with expected output and cleanup) | root `README.md` minimal example; `install.md` | done: moved → `start/quick-start`, old URL redirects |
| `install.md` (386) | new user | How do I install, build, and verify the CLI? | howto | this page | root `README.md` "Install Or Build" | done: moved → `start/install`, old URL redirects |
| `documentation-map.md` (441) | everyone | Where is the documentation for task X? | navigation | none (routing only) | the entire sidebar; layers table duplicated on `index.mdx`; maintenance rule overlaps `AGENTS.md` | retire: redirect → home; layers and maintenance-rule text move to `docs/README.md`; this inventory takes over the meta function |

### `Start` group (added by the restructuring)

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `start/choose-a-workflow.md` (742) | new user deciding integration shape | Should changes reach my databases as versioned migration files or as direct applies? | concept | this page; command shapes verified against the built binary | `workflows/migrations`, `workflows/atlas-cli`, `reference/comparison` | created (section 9, item 4); keep |
| `start/adopt-an-existing-database.md` (1,000) | brownfield database adopter | How do I put an existing database under Ptah management without recreating it? | howto | this page; `ptah introspect`, `ptah migrations baseline`, `ptah migrations import` runs against the built binary | `workflows/checkpoints` (baseline contrast), `workflows/migrations` import section | created (section 9, item 3); keep |

### `Model your schema` group (added by the restructuring)

Moved pages keep their rows in the groups below with `done` dispositions; this
table lists the pages the restructuring created.

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `schema/composite.md` (630) | multi-source schema author | How do several schema sources merge into one desired schema? | howto | this page; merge, conflict, and error behavior run against the built binary | per-source `schema/*` pages (each links here instead of restating the rules), `workflows/migrations` compose section | created (canonical multi-source page, deduplicating the compose sections that lived on `workflows/go-schema`, `workflows/schema-files`, and `workflows/migrations`); keep |
| `schema/visualize.md` (560) | any schema author | How do I render schema diagrams? | howto | this page; `ptah viz` runs against the built binary; `examples/viz/` artifacts | `start/install` (Graphviz optional tool), `operate/troubleshooting` (Graphviz symptom) | created (section 9, item 5, grown from `examples/schema-viz`); keep |

### `Direct schema changes` group (added by the restructuring)

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `direct/inspect.md` (631) | any user with a live database | How do I see the schema a live database actually has? | howto | this page; `ptah db read`, `ptah introspect`, and `ptah atlas schema inspect` runs against the built binary | `start/adopt-an-existing-database` (introspect flow, canonical there), `workflows/atlas-cli` (inspect template surface, canonical there) | created (section 10 target, per D5); keep |
| `direct/compare-and-drift.md` (739) | direct-workflow user, CI operator | How does a live database differ from the desired schema, and how do I gate on that? | howto | this page; `ptah schema compare`, `ptah schema drift` (severity, formats, exit codes), and `ptah migrations plan` runs against the built binary | `start/choose-a-workflow` (workflow decision), `versioned/generate` (plan-to-files path), `reference/commands` rows | created (section 10 target, per D5); keep |
| `direct/apply.md` (865) | direct-workflow user | How do I apply desired-schema changes straight to a database? | howto | this page; `ptah atlas schema apply` (dry-run, prompt, auto-approve), `ptah atlas schema plan` plan files, and the stale-plan refusal run against the built binary | `workflows/atlas-cli` (full `schema apply`/`schema plan` flag surface, canonical there), `start/choose-a-workflow` (hybrid pattern) | created (section 10 target, per D5: states that native Ptah has no direct apply verb and that direct application ships on the Atlas-compatible surface); keep |

### `Versioned migrations` group (added by the restructuring)

The six lifecycle pages split out of `workflows/migrations.md` are covered by
its row below; `versioned/checkpoints` and `versioned/reference-data` keep
their rows in the `Use Ptah` table with `done` dispositions. This table lists
the page the restructuring created.

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `versioned/maintain-history.md` (827) | versioned-migration user | How do I change unapplied migrations and recover a dirty revision state? | howto | this page; `edit`, `rebase`, `rm`, and `repair` (including the dirty-state and `--resume-from` flows) run against the built binary | `versioned/rollback` (failed-down dirty state), `versioned/checkpoints` (squash contrast), Atlas forwards in exit-code and command tables | created (section 9, item 10); keep |

### `Use Ptah` group

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `workflows/go-schema.md` (717) | Go app developer | How do I use annotated Go structs as the desired schema? | howto | this page for the workflow; annotation grammar's real source is `core/goschema` + `internal/annotationmeta` (no directive reference page exists anywhere) | `examples/go-model`, root `README.md` minimal example, `docs/go_annotations_vs_atlas_hcl.md` | done: moved + rewritten → `schema/go-annotations` (absorbed `examples/go-model`; composite section now points to `schema/composite`), old URL redirects |
| `workflows/schema-files.md` (1,198) | schema-file user | How do I feed YAML, HCL, or SQL files to Ptah? | howto (four sources plus external programs plus composition on one page) | this page | `reference/yaml-schema`, `reference/hcl-schema`, `examples/yaml-schema`, `examples/atlas-hcl`, `workflows/orm-loaders` (external-program section) | done: split → `schema/yaml`, `schema/hcl`, `schema/sql`, `schema/composite`; external-program section merged into `schema/orm-and-external`; old URL redirects to `schema/yaml` |
| `workflows/orm-loaders.md` (592) | ORM/external-provider user | How do I feed my ORM's schema into Ptah? | howto | this page (GORM path verified per its own claim) | `workflows/schema-files.md` "Load from an external program" | done: moved + rewritten → `schema/orm-and-external` (absorbed the external-program contract), old URL redirects |
| `workflows/api-schema-export.md` (737) | API developer | How do I export entities to OpenAPI or GraphQL? | howto with an embedded type-mapping reference table | `cmd/schema` `export`; backing depth in `docs/api_schema_export.md` | `docs/api_schema_export.md` | done: moved → `schema/export`, old URL redirects |
| `workflows/migrations.md` (1,031) | versioned-migration user | How do I run the migration lifecycle? | howto hub: 14 sections; "Squashing history", "Testing", and "Reference data" are two-to-three-sentence link-out stubs, "Operational hooks" is a link-out; "Importing from another tool" and "Safety gates" carry real depth | this page for the loop; `migration/migrator/README.md` and `docs/native_cli.md` carry deeper detail | `workflows/checkpoints`, `workflows/testing`, `workflows/reference-data`, `workflows/oci-registry`, `docs/native_cli.md` | done: split → `versioned/overview` (loop + mental model + directory formats), `versioned/generate` (plan, generate, composite, shadow verification, manual create), `versioned/apply` (up, status, hooks, OCI-sourced runs), `versioned/rollback`, `versioned/integrity-and-safety` (hash/validate, dev-database replay, lint, destructive gate, pre-migration checks), `versioned/import`; stub sections dissolved into links; every example rerun against the built binary; old URL redirects to `versioned/overview` |
| `workflows/oci-registry.md` (1,865) | operator/platform engineer | How do I publish, pin, and consume artifacts through an OCI registry? | howto | this page; deeper detail in `docs/oci_registry.md` (2,393 words) | `docs/oci_registry.md`, `workflows/migrations.md` OCI section | move + rewrite in place → `operate/oci-registry` |
| `workflows/checkpoints.md` (884) | versioned-migration user | How do I squash migration history? | howto with concept material (file format, rollback boundary) | this page; `cmd/migratecheckpoint` | currently the only site prose describing `ptah migrations baseline` (the "Checkpoint versus baseline" contrast) | done: moved → `versioned/checkpoints`, old URL redirects; baseline how-to content has its own home in `start/adopt-an-existing-database` |
| `workflows/testing.md` (766) | developer/CI operator | How do I test migrations and schemas declaratively? | howto | this page; `docs/testing.md` | `reference/testing` (intentional howto/reference pair — the model split to generalize), `docs/testing.md` | done: moved → `testing/migrations-and-schema`, old URL redirects |
| `workflows/reference-data.md` (906) | app developer | How do I manage reference/lookup rows declaratively? | howto | this page; `cmd/migratedata` | contains the only prose comparing declarative data with `ptah seed` | done: moved → `versioned/reference-data`, old URL redirects; the seed contrast feeds `operate/seed-data` |
| `workflows/atlas-cli.md` (4,137) | Atlas migration user | How do I use Atlas-style commands with Ptah? | mixed: concept (translation model) + reference (three command tables duplicating `reference/commands.md`) + tutorial (worked example) + status (parity expectations) | `cmd/atlas`; conformance evidence in `stokaro/ptah-atlas-conformance` | `reference/commands.md` Atlas table, `reference/comparison.md`, `examples/atlas-migrations` | split → `atlas/overview` (concept, surfaces, parity expectations), `atlas/migrate-commands`, `atlas/schema-commands`; command-status tables deduplicate into `reference/atlas-commands`; old URL redirects to `atlas/overview` |
| `workflows/ci.md` (371) | CI operator | How do I gate pull requests with Ptah? | howto | this page; `docs/github_action.md` | `docs/github_action.md` | done: moved + rewritten → `testing/ci` (absorbed GitHub Action usage — inputs, outputs, permissions, pinning — from `docs/github_action.md`, which stays as the backing reference), old URL redirects |

### `Examples` group

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `examples/go-model.md` (238) | new Go user | What does a minimal annotated model look like? | howto (worked example) | this page | `workflows/go-schema`, root `README.md` | done: merged → `schema/go-annotations`, old URL redirects |
| `examples/yaml-schema.md` (141) | schema-file user | What does a minimal YAML schema look like? | howto (worked example) | this page | `workflows/schema-files`, `reference/yaml-schema` | done: merged → `schema/yaml`, old URL redirects |
| `examples/atlas-hcl.md` (177) | schema-file user | What does a minimal HCL schema look like? | howto (worked example) | this page | `workflows/schema-files`, `reference/hcl-schema` | done: merged → `schema/hcl`, old URL redirects |
| `examples/atlas-migrations.md` (297) | Atlas migration user | How do I use an Atlas-style migration directory? | howto (worked example) | this page | `workflows/atlas-cli` | merge → `atlas/migrate-commands` worked-example section |
| `examples/schema-viz.md` (289) | any schema author | How do I render schema diagrams? | howto (worked example); currently the only site coverage of the `ptah viz` workflow | this page; `examples/viz/` artifacts | `install.md` (Graphviz optional tool), `operate/troubleshooting` (Graphviz symptom) | done: grown → `schema/visualize` (full `ptah viz` workflow: Mermaid/DOT/SVG, Graphviz prerequisite, committed artifacts), old URL redirects |

### `Reference` group

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `reference/commands.md` (2,026) | every CLI user | What commands exist and what do they do exactly? | reference; the Atlas table stuffs 150–250-word status prose into cells | `cmd/` command tree and `--help` output | `workflows/atlas-cli` tables, `docs/native_cli.md` (near-duplicate native tree) | split → `reference/native-commands` (complete verb table; add the four missing rows and fix the two stale Atlas-test rows, see section 9) + `reference/atlas-commands` (a section per command replacing giant cells); old URL redirects to `reference/native-commands` |
| `reference/configuration.md` (523) | every user | What goes in `ptah.yaml` and which setting wins? | reference | this page; deeper `docs/project_config.md` (1,088 words) | `docs/project_config.md`; config-precedence section currently lives in `reference/comparison.md` | rewrite in place: absorb the precedence section from `comparison`; stays at its URL |
| `reference/yaml-schema.md` (774) | schema-file user | What is the exact YAML schema format? | reference | this page for readers; `docs/yaml_schema.md` (protected by `check-core-doc-links.mjs`) for engineering depth | `docs/yaml_schema.md`, `workflows/schema-files` | keep |
| `reference/hcl-schema.md` (570) | schema-file user | What HCL subset does Ptah support? | reference | this page for readers; `docs/atlas_hcl_schema.md` (protected) | `docs/atlas_hcl_schema.md`, `workflows/schema-files` | keep |
| `reference/atlas-project-config.md` (992) | Atlas migration user | What `atlas.hcl` subset does Ptah read? | reference | this page for readers; `docs/atlas_project_config.md` (protected) | `docs/atlas_project_config.md`, `reference/configuration` | move → `atlas/project-config` |
| `reference/public-api.md` (727) | Go API embedder | Which Go packages are stable to embed? | reference | this page for readers; `docs/public_api.md` (protected) + `docs/public_api.snapshot` + guard scripts | `docs/public_api.md`, `reference/reusable-components` | move → `extend/public-api` |
| `reference/reusable-components.md` (3,167) | Go API embedder | How do I use Ptah as a library? | mixed howto/reference: component map, seven end-to-end examples, seven use-case narratives, comparisons | this page; package docs under `core/`, `migration/`, `dbschema/` | `reference/public-api`, `reference/query-builder`, `examples/migrator/README.md`; its comparisons section overlaps `reference/comparison` | move + tighten → `extend/components`; use-case narratives compressed; comparisons section merges into `atlas/comparison` |
| `reference/query-builder.md` (2,390) | Go API embedder | How do I build dialect-aware queries with `core/query`? | reference | `core/query` package | `reference/reusable-components` | move → `extend/query-builder` |
| `reference/testing.md` (487) | developer/CI operator | What are the exact test-command flags and YAML model? | reference | `cmd/migrationstest`, `cmd/schema` test, `docs/testing.md` | `workflows/testing` (the intentional pair) | move/rename → `reference/test-cases` (clarifies it is not about testing Ptah itself) |
| `reference/capabilities.md` (528) | every user | Which features work on which dialect? | reference with concept prose mixed in | `docs/capabilities.md` (protected) and capability code | `docs/capabilities.md`, `reference/dialect-notes` | keep + tighten: concept prose moves to `concepts/dialects-and-capabilities` |
| `reference/dialect-notes.md` (495) | every user | How does my engine behave differently? | reference (six engines compressed into one short page) | dialect code and `docs/sqlite.md`, `docs/sqlserver.md`, PostgreSQL feature docs | `reference/capabilities`, `docs/sqlite.md`, `docs/sqlserver.md` | dissolve → `databases/support-matrix` (plus per-engine pages as content justifies); old URL redirects to `databases/support-matrix` |
| `reference/comparison.md` (4,795) | evaluator/Atlas migration user | At least four distinct questions: how Ptah positions against Atlas; command parity; evidence per feature; known gaps; config precedence; safety/exit behavior | mixed status/reference; evidence-table cells run past 400 words | conformance evidence in `stokaro/ptah-atlas-conformance`; command claims from `cmd/` | `workflows/atlas-cli`, `reference/commands`, `reference/configuration`, `reference/exit-codes`, `operate/conformance` | move + split → `atlas/comparison` (positioning, command parity, trimmed comparison, gap register); config precedence → `reference/configuration`; safety/exit behavior → `versioned/integrity-and-safety` and `reference/exit-codes` |
| `reference/atlas-docs-coverage.md` (3,164) | Atlas migration user/maintainer | Which Atlas documentation areas does Ptah cover? | status (crosswalk matrix; "Research date: July 22, 2026" convention) | this page, refreshed against Atlas docs and conformance runs | `reference/comparison`, `operate/conformance` | move → `atlas/docs-coverage` |
| `reference/exit-codes.md` (1,652) | CI operator | What exit code means what? | reference | `docs/exit_codes.md` is the script-checked source (`check-exit-codes.mjs` hardcodes both paths) | `docs/exit_codes.md` (deliberate, mechanically checked copy) | keep at the same path (script-coupled) |

### `Operate` group

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `operate/troubleshooting.md` (390) | every user | My command failed — what now? | troubleshooting (seven symptom entries, already symptom-first) | this page | none significant | keep + grow: absorb failure modes surfaced by the workflow rewrites; stays symptom → cause → diagnosis → fix → verify |
| `operate/conformance.md` (503) | evaluator | Where is the Atlas-compatibility evidence and how do I read it? | status | `stokaro/ptah-atlas-conformance` reports (`gaps*.md`, `PARITY.md`) | `reference/comparison`, `reference/atlas-docs-coverage`, `docs/conformance.md` | move → `atlas/conformance` |
| `operate/license-boundary.md` (190) | evaluator/legal reviewer | How is Ptah kept license-clean relative to Atlas? | concept | this page; root `README.md` boundary diagram | root `README.md` | move → `atlas/license-boundary` |

## 2. Repository docs (`docs/*.md` and companions)

Authority rule adopted by this audit: the published site is canonical for
everything reader-facing; `docs/*.md` files are engineering references
(design detail, evidence, release process) or check-script sources of truth. A
fact reachable from the sidebar must not have a second, potentially divergent
reader-facing home.

Seven files are protected by `check-core-doc-links.mjs` (site pages must not
link to them on GitHub): `yaml_schema.md`, `atlas_hcl_schema.md`,
`atlas_project_config.md`, `public_api.md`, `capabilities.md`, `sqlite.md`,
`sqlserver.md`. Retiring any of them requires editing that script's protected
list in the same PR.

| File (words) | Audience | Purpose | Site counterpart | Disposition |
| --- | --- | --- | --- | --- |
| `README.md` (45) | contributor | Docs directory entry point | `index.mdx` | keep + rewrite: update links; hosts the documentation-layers and maintenance-rule text retired from `documentation-map` |
| `exit_codes.md` (1,557) | contributor/CI | Source of truth for `check-exit-codes.mjs` | `reference/exit-codes` | keep (script-coupled) |
| `yaml_schema.md` (1,348) | contributor | YAML format engineering depth | `reference/yaml-schema` | keep (protected) |
| `atlas_hcl_schema.md` (1,415) | contributor | HCL subset engineering depth | `reference/hcl-schema` | keep (protected) |
| `atlas_project_config.md` (2,143) | contributor | `atlas.hcl` subset engineering depth | `reference/atlas-project-config` | keep (protected) |
| `public_api.md` (878) | contributor/embedder | API guardrails, snapshot process | `reference/public-api` | keep (protected; pairs with `public_api.snapshot`, `public_api_approvals.txt`) |
| `capabilities.md` (2,544) | contributor | Full capability matrices | `reference/capabilities` | keep (protected) |
| `sqlite.md` (532) | contributor | SQLite behavior detail | `reference/dialect-notes` | keep (protected); essentials absorbed by `databases/*` pages |
| `sqlserver.md` (1,269) | contributor | SQL Server behavior detail | `reference/dialect-notes` | keep (protected); essentials absorbed by `databases/*` pages |
| `native_cli.md` (1,267) | contributor | Native command tree walkthrough | `reference/commands` | retirement candidate once `reference/native-commands` is complete (near-duplicate today; not protected) |
| `go_annotations_vs_atlas_hcl.md` (727) | evaluator | Source-format comparison | none (gap) | feeds `start/choose-a-workflow` and `reference/go-annotations`; retirement candidate after absorption |
| `migrations-import.md` (720) | contributor | Import converter detail | `workflows/migrations` import section | keep as backing reference for `versioned/import` |
| `pre-migration-checks.md` (727) | contributor | `-- +ptah check` directive detail | `workflows/migrations` safety section | keep as backing reference for `versioned/integrity-and-safety` |
| `oci_registry.md` (2,393) | contributor | OCI transport detail | `workflows/oci-registry` | keep as backing reference |
| `api_schema_export.md` (764) | contributor | Export mapping detail | `workflows/api-schema-export` | keep as backing reference |
| `testing.md` (1,081) | contributor | Declarative testing detail | `testing/migrations-and-schema` + `reference/testing` | keep as backing reference |
| `github_action.md` (627) | contributor | GitHub Action detail | `testing/ci` | keep as backing reference for `testing/ci` |
| `project_config.md` (1,088) | contributor | `ptah.yaml` full reference | `reference/configuration` | keep as backing reference |
| `conformance.md` (522) | contributor | Conformance process detail | `operate/conformance` | keep as backing reference |
| `online-ddl.md` (881) | contributor | Online-DDL behavior | none | feeds `databases/postgresql` and `databases/mysql-mariadb`; keep as engineering depth |
| `postgresql_extension_ignore.md` (687) | contributor | Extension-ignore behavior | none | feeds `databases/postgresql`; keep |
| `POSTGRESQL_ROLES.md` (1,453) | contributor | Roles/RLS annotations | none | feeds `databases/postgresql` and `reference/go-annotations`; keep |
| `sequences.md` (674) | contributor | Sequence annotations | none | feeds `databases/postgresql` and `reference/go-annotations`; keep |
| `user_defined_types.md` (586) | contributor | UDT annotations | none | feeds `databases/postgresql` and `reference/go-annotations`; keep |
| `dml_upsert.md` (352) | contributor | Upsert rendering | none | feeds `databases/*` pages; keep |
| `system_design.md` (1,809) | contributor | Architecture overview | none (intentional) | keep (contributor surface, out of reader navigation) |
| `release_process.md` (295) | maintainer | Release steps (also the only mention of the `ptah-ls` binary anywhere in the docs) | none | keep (maintainer surface) |
| `diagrams/*.mmd` (2 files) | contributor | Architecture diagrams | none | keep |

## 3. Root and package READMEs

| File (words) | Audience | Purpose | Overlaps | Disposition |
| --- | --- | --- | --- | --- |
| `README.md` (643) | everyone landing on GitHub | Project pitch, start-here table, install, surfaces, compatibility status | `index.mdx`, `install.md`, `getting-started.md`, `operate/license-boundary` | keep + rewrite links when site URLs move; must never diverge from the site on parity claims |
| `docs/site/README.md` (107) | contributor | How to build the site | none | keep |
| `testkit/README.md` (178) | Go embedder | Test-harness package (separate Go module) | `reference/public-api` | keep; link from `extend/public-api` |
| `internal/parser/README.md` (1,331) | contributor | SQL parser internals | none | keep (contributor surface) |
| `migration/generator/README.md` (1,222) | contributor/embedder | Generator package detail | `reference/reusable-components` | keep; canonical for package-level API detail |
| `migration/migrator/README.md` (2,887) | contributor/embedder | Migrator package detail | `workflows/migrations`, `reference/reusable-components` | keep; canonical for package-level API detail |
| `cmd/lint/testdata/sarif/README.md` (46) | contributor | Test-fixture note | none | keep (not reader-facing documentation) |

## 4. Examples (`examples/**`)

| Path | Audience | Purpose | Site counterpart | Disposition |
| --- | --- | --- | --- | --- |
| `examples/migrator/README.md` (340) + `migrations/` fixtures | Go embedder | Embedded-migrator runnable example | `reference/reusable-components` "Embed The Migrator" | keep; link from `extend/components` |
| `examples/viz/README.md` (178) + committed `schema.{mmd,dot,sql,svg}` artifacts | schema author | Visualization runnable example with generated artifacts | `schema/visualize` | keep; the artifact backing for `schema/visualize` |
| `examples/migrator_parser/` (no README) | contributor | Parser API demo | `internal/parser/README.md` | keep; no reader-facing obligation |
| `examples/extension_ignore/` (no README) | contributor | Extension-ignore demo | `docs/postgresql_extension_ignore.md` | keep; no reader-facing obligation |
| `examples/reusable_components/` (test only) | contributor | Executable doc-examples for `reference/reusable-components` | `reference/reusable-components` | keep; keeps site snippets honest |

## 5. Integration docs (`integration/*.md`)

| File (words) | Audience | Purpose | Disposition |
| --- | --- | --- | --- |
| `integration/README.md` (1,571) | contributor | Integration test framework guide | keep (contributor surface, out of reader navigation) |
| `integration/DYNAMIC_TESTING.md` (617) | contributor | Dynamic test scenarios | keep |
| `integration/MIGRATION_GENERATOR_VALIDATION.md` (654) | contributor | Generator validation harness | keep |

## 6. Reader journeys

The ten journeys required by #804, with today's path (and its friction) and the
target path. The target navigation in section 10 is considered valid only
because each journey below resolves without consulting a meta-map.

| Journey | Today | Target |
| --- | --- | --- |
| New Go user | `getting-started` → `workflows/go-schema` → `workflows/migrations`; works, but the versioned-versus-direct decision is never presented | Home → `start/quick-start` → `schema/go-annotations` → `versioned/overview` |
| Schema-file user | `workflows/schema-files` covers four formats on one page; reference pages are two clicks away | Home → `start/choose-a-workflow` → `schema/yaml` / `schema/hcl` / `schema/sql` |
| ORM/external-provider user | `workflows/orm-loaders`, duplicated by a section of `schema-files` | `schema/orm-and-external` |
| Brownfield database adopter | no path: `ptah introspect` appears only in reference tables, `migrations baseline` has no how-to (only a contrast note in `checkpoints` and an exit-code row) | `start/adopt-an-existing-database` → `direct/inspect` → `versioned/import` or baseline |
| Versioned-migration user | `workflows/migrations` hub; per-step depth requires jumping to four other pages | `versioned/overview` → lifecycle pages |
| Declarative/direct-workflow user | no home: `schema compare`/`drift` live in command tables; direct apply is only inside `workflows/atlas-cli` | `start/choose-a-workflow` → `direct/*` |
| CI/operator | `workflows/ci` (371 words) → `reference/exit-codes`; safety gates buried in `workflows/migrations` | `testing/ci` → `versioned/integrity-and-safety` → `reference/exit-codes` → `operate/troubleshooting` |
| Atlas migration user | `workflows/atlas-cli` (4,137 words, four page types mixed) → `reference/comparison` (4,795 words, four questions mixed) | `atlas/overview` → `atlas/migrate-commands` / `atlas/schema-commands` → `atlas/comparison` / `atlas/conformance` |
| Go API embedder | `reference/reusable-components` + `reference/public-api` + `reference/query-builder`, all filed under generic `Reference` | `extend/public-api` → `extend/components` → `extend/query-builder` |
| Contributor | `AGENTS.md` → skill; no style guide exists yet | `AGENTS.md` → `docs/STYLE_GUIDE.md` → skill → this inventory |

## 7. Terminology audit

Verified with repository-wide searches at the audited commit.

| Term | Current usage (measured) | Canonical decision |
| --- | --- | --- |
| native commands | consistent: "native" used for the `ptah <verb>` tree | keep; never described with Atlas spellings; root-level Atlas aliases are documented as intentionally absent |
| `ptah atlas ...` | consistent prefix across all pages | keep; always spelled with the `ptah atlas` prefix |
| `ptah-compat` | mentioned in prose on 10 site pages; never a table column | keep: a binary-level drop-in described in prose, never a third command surface |
| desired schema vs desired state | "desired schema" appears 60 times across 18 pages; "desired state" 3 times (`workflows/migrations.md` twice, `workflows/schema-files.md` once) | standardize on **desired schema**; retire "desired state" outside the composite-source discussion |
| schema source | used informally | canonicalize: Go annotations, YAML, HCL, SQL file, external loader, or live database used as input |
| dev database / shadow database / throwaway database | all three exist and are real, distinct flags: `--dev-url` (replay validation on `migrations validate`, `migrations lint`, Atlas-compatible verbs), `--shadow-db` (`migrations generate`, `checkpoint`, `baseline` verification replay), throwaway databases in `migrations test` / `schema test` | keep all three as distinct terms; define each once in `concepts/database-urls-and-dev-databases` and link instead of re-defining |
| migration directory / integrity file / revision table | consistent; integrity files are `ptah.sum` (native) and `atlas.sum` (Atlas-format) | keep |
| dialect vs database/engine | mostly consistent; `reference/dialect-notes` blurs the two | dialect = SQL rendering flavor; database/engine = the product you connect to |
| capability | consistent | keep: per-dialect feature gate |
| drift | consistent (`ptah schema drift`) | keep |
| conformance | consistent; evidence lives in `stokaro/ptah-atlas-conformance` | keep; claims must cite current reports |
| clean-room / license-clean | consistent | keep; never describe Atlas internals |
| heading case | mixed: workflow pages use sentence case; several reference pages use Title Case ("Supported Blocks", "MySQL And MariaDB", "Rule Of Thumb") | standardize on sentence case (style guide rule) |

## 8. Content-quality sample

The nine areas the issue requires sampling, assessed against the actual pages:

- **Quick start** (`getting-started.md`): the strongest page. Runnable SQLite
  flow, expected output, verification, rollback, cleanup, next steps. This is
  the tutorial template the style guide codifies.
- **Migrations** (`workflows/migrations.md`): teaches the loop well but cannot
  host depth; three sections are two-to-three-sentence stubs pointing
  elsewhere, so readers bounce between five pages for one lifecycle.
- **Command reference** (`reference/commands.md`): native table is missing four
  shipped verbs and two Atlas rows are stale (section 9); Atlas cells run
  150–250 words — prose stuffed into cells.
- **Configuration** (`reference/configuration.md`): thin (523 words) relative
  to `docs/project_config.md`; precedence rules live on a different page
  (`comparison`).
- **Dialect guidance** (`reference/dialect-notes.md`): 495 words for six
  engines; real depth exists but only in unlinked-from-here `docs/*.md` files.
- **Troubleshooting** (`operate/troubleshooting.md`): correct symptom-first
  shape; only seven entries, so most failure modes documented in workflow pages
  never land here.
- **Examples**: runnable and truthful, but siloed in their own group (141–297
  words each) away from the workflow pages answering the same question.
- **Public API** (`reference/public-api.md`): solid reference;
  `reusable-components` beside it mixes reference, tutorial, and marketing-ish
  use-case narrative on one 3,167-word page.
- **Conformance** (`operate/conformance.md`): good status page; evidence-linked
  and honest about red checks.

## 9. Missing content (verified against the code)

Each item was checked against `cmd/` and repository-wide searches; none of
these are merely misplaced content.

1. **Four native command rows.** `ptah migrations import`, `baseline`,
   `checkpoint`, and `repair` are registered in `cmd/migrations/migrations.go`
   but absent from the native table in `reference/commands.md`. Fix in
   `reference/native-commands`.
2. **Two stale Atlas rows.** `reference/commands.md` still lists
   `ptah atlas migrate test` and `ptah atlas schema test` among "Registered
   Atlas CE boundary stubs"; both were flipped to working forwards of the
   native test runners at the audited commit itself (`f5c59b5`, #805, which
   updated `workflows/atlas-cli.md`, `workflows/testing.md`, exit codes, and
   conformance pages but not `reference/commands.md`). Fix wherever the Atlas
   command reference lands first.
3. **Brownfield adoption path.** `ptah introspect` and
   `ptah migrations baseline` ship today, but no page teaches adopting Ptah on
   an existing database. Baseline's only site presence is a contrast note in
   `workflows/checkpoints.md` and an exit-code row. New page:
   `start/adopt-an-existing-database`. Done: page created with introspect,
   drift-check, baseline (dry-run and `--shadow-db`), and import flows run
   against the built binary.
4. **Workflow choice.** No page presents the versioned versus direct/declarative
   versus hybrid decision; it is implied by `reference/comparison` tables only.
   New page: `start/choose-a-workflow`. Done: page created.
5. **`ptah viz` workflow page.** Only an example page exists; the workflow
   (formats, Graphviz prerequisite, committed artifacts) is scattered across
   `install.md` and `operate/troubleshooting.md`. New page: `schema/visualize`.
   Done: page created with every command run against the built binary.
6. **`ptah seed` page.** The command ships (`cmd/seed`,
   environment-scoped SQL seed files) but has no page; it appears only in the
   `reference-data` contrast, command row, and exit-code rows. New page:
   `operate/seed-data`.
7. **Go annotation directive reference.** No page on the site (or in
   `docs/*.md`) enumerates the `//migrator:schema:*` directives; the grammar
   lives in `core/goschema` and `internal/annotationmeta`, with fragments in
   `docs/POSTGRESQL_ROLES.md`, `docs/sequences.md`, and
   `docs/user_defined_types.md`. New page: `reference/go-annotations`.
8. **Database URL and dev-database reference.** No central page documents
   accepted URL formats (`sqlite://`, `postgres://`, ...) or distinguishes
   `--dev-url`, `--shadow-db`, and throwaway test databases. New page:
   `concepts/database-urls-and-dev-databases`.
9. **`ptah-ls` is undocumented.** The repository ships an annotation language
   server binary (`cmd/ptah-ls`, `internal/ptahls`) that is built and verified
   during releases (`docs/release_process.md`), yet no reader-facing page
   mentions it. Proposed home: an editor-support section in
   `reference/go-annotations`, with an install note in `start/install`.
   (Found by this audit; not in the pre-implementation design.)
10. **Maintenance how-to.** `migrations edit`, `rebase`, `rm`, and `repair`
    have command rows (except `repair`, see item 1) and exit-code rows but no
    how-to home. New page: `versioned/maintain-history`. Done: page created
    with every verb (including the dirty-state repair and `--resume-from`
    flows) run against the built binary.

## 10. Target navigation and page map

Ordering principle: reading order for a new user first, lookup surfaces last.
Group rationale follows the table. Current → new mappings are in the
disposition columns of sections 1–2.

```text
Home (index.mdx, splash)
Start
  start/install                       howto
  start/quick-start                   tutorial (current getting-started)
  start/choose-a-workflow             concept (new)
  start/adopt-an-existing-database    howto (new: introspect, baseline, import, drift)
Model your schema
  schema/go-annotations               howto (+ example from examples/go-model)
  schema/yaml                         howto (+ example from examples/yaml-schema)
  schema/hcl                          howto (+ example from examples/atlas-hcl)
  schema/sql                          howto
  schema/orm-and-external             howto
  schema/composite                    howto (canonical multi-source page)
  schema/visualize                    howto (new, from examples/schema-viz)
  schema/export                       howto (current workflows/api-schema-export)
Direct schema changes
  direct/inspect                      howto (new: db read, introspect, atlas schema inspect)
  direct/compare-and-drift            howto (new: schema compare, schema drift, plan-only runs)
  direct/apply                        howto (new: Atlas-compatible schema apply; hybrid patterns)
Versioned migrations
  versioned/overview                  concept + core loop (split from workflows/migrations)
  versioned/generate                  howto
  versioned/apply                     howto
  versioned/rollback                  howto
  versioned/integrity-and-safety      howto (hash/validate/verify-sum, lint, destructive gate,
                                      pre-migration checks, shadow verification)
  versioned/maintain-history          howto (edit, rebase, rm, repair)
  versioned/import                    howto (golang-migrate/Goose/Flyway/Liquibase)
  versioned/checkpoints               howto (current workflows/checkpoints)
  versioned/reference-data            howto (current workflows/reference-data)
Test and CI
  testing/migrations-and-schema       howto (current workflows/testing)
  testing/ci                          howto (current workflows/ci + GitHub Action)
Distribute and operate
  operate/oci-registry                howto (current workflows/oci-registry)
  operate/seed-data                   howto (new, small: ptah seed vs reference data)
  operate/troubleshooting             troubleshooting (kept; grows symptom-first)
Databases
  databases/support-matrix            reference-flavored landing (from dialect-notes)
  databases/postgresql                guide (P1)
  databases/mysql-mariadb             guide (P2, may start as a support-matrix section)
  databases/sqlite                    guide (P2)
  databases/sqlserver                 guide (P2)
Atlas compatibility
  atlas/overview                      concept (surfaces, translation model, ptah-compat)
  atlas/migrate-commands              howto (+ example from examples/atlas-migrations)
  atlas/schema-commands               howto
  atlas/project-config                reference (current reference/atlas-project-config)
  atlas/comparison                    status (slimmed reference/comparison)
  atlas/conformance                   status (current operate/conformance)
  atlas/docs-coverage                 status (current reference/atlas-docs-coverage)
  atlas/license-boundary              concept (current operate/license-boundary)
Extend Ptah
  extend/public-api                   reference (current reference/public-api)
  extend/components                   howto/reference (current reference/reusable-components)
  extend/query-builder                reference (current reference/query-builder)
Concepts
  concepts/desired-schema-and-sources concept (new)
  concepts/migration-directory        concept (new)
  concepts/database-urls-and-dev-databases  concept (new)
  concepts/dialects-and-capabilities  concept (new)
Reference
  reference/native-commands           reference (split from reference/commands)
  reference/atlas-commands            reference (split from reference/commands)
  reference/go-annotations            reference (new: //migrator:schema:* directives)
  reference/configuration             reference (kept; + precedence)
  reference/yaml-schema               reference (kept)
  reference/hcl-schema                reference (kept)
  reference/test-cases                reference (current reference/testing)
  reference/capabilities              reference (kept; matrix only)
  reference/exit-codes                reference (kept at same path; script-coupled)
```

Retired from the sidebar: `documentation-map` (redirect to home), the
`Examples` group (content merged where the decision is made), and the
`Use Ptah` and `Operate` labels.

Rationale by group (each maps to the top-level question a reader faces, in
order):

1. **Start** — "What is this and how do I try it?" Install, a runnable quick
   start, the one decision that shapes everything else (versioned vs direct vs
   hybrid), and the brownfield entry, which today has shipped commands but no
   documented path.
2. **Model your schema** — "How do I tell Ptah what the schema should be?"
   Ptah's defining dimension: Go annotations, YAML, HCL, SQL, external
   loaders, and composite merging are peer sources feeding one pipeline.
   Visualization and export live here because they are outputs of the modeled
   schema, not of the migration lifecycle.
3. **Direct schema changes** and **Versioned migrations** — "How do I change
   the database?" Two peer groups mirroring the choose-a-workflow decision.
   Inspection becomes first-class; the versioned group decomposes along the
   lifecycle (generate → apply → rollback → integrity → maintain → import),
   with checkpoints and reference data as named capabilities inside the
   lifecycle they serve. Native Ptah has no direct `schema apply` verb; the
   `direct/apply` page says exactly that and shows the Atlas-compatible
   command plus hybrid patterns.
4. **Test and CI** — "How do I know it's safe?"
5. **Distribute and operate** — "How do I ship and run it?" OCI artifacts,
   seeds, troubleshooting.
6. **Databases** — "What about my engine?" Support matrix plus per-engine
   guides where behavior materially differs (tiered: PostgreSQL first).
7. **Atlas compatibility** — "I'm coming from Atlas / need parity evidence."
   One front door for the compat surface, usage, config, comparison,
   conformance, docs coverage, and the license boundary, instead of today's
   spread across three groups.
8. **Extend Ptah** — "I want to build on Ptah."
9. **Concepts** and **Reference** — "What exactly does X mean / accept?"
   Last, because they serve returning readers.

Page count grows from 37 to roughly 59; the growth is splits plus the verified
missing content in section 9. Tiering keeps scope honest: the per-engine
database guides other than PostgreSQL may launch as sections of
`databases/support-matrix` and split out when content justifies it.

## 11. Atlas patterns adopted and rejected

Adopted, in Ptah terms:

- Navigation by database-work domain rather than by how the docs accumulated.
- The versioned/direct decision surfaced early (`start/choose-a-workflow`)
  instead of buried in a comparison table.
- Large workflows decomposed into focused lifecycle pages instead of one hub.
- Schema representation as its own navigation dimension, independent of the
  workflow that consumes it.
- A small concepts layer separated from procedures.
- CLI reference as pure reference; workflows carry the teaching.

Rejected or adapted:

- No "Cloud" or "Integrations" groups: Ptah's distribution story is OCI
  registries and a GitHub Action — workflows, not a partner catalog.
- No separate "Guides" tree: at Ptah's size, scenario content lives inside the
  workflow groups; a separate guides hub would recreate the Examples silo.
- Atlas's "Declarative workflow" as a peer of "Versioned workflow" is adapted,
  not copied: native Ptah has no direct `schema apply`; direct application
  ships on the Atlas-compatible surface, while native direct work is
  inspect/compare/drift/plan. The group is therefore "Direct schema changes"
  and is honest about which surface does what.
- Atlas's page-per-command reference is rejected; Ptah keeps two consolidated
  command reference pages (native, Atlas-compatible) with per-command anchors.
- No Atlas prose, examples, diagrams, assets, taxonomy, or page sequence is
  copied; the structure above is derived from Ptah's own command tree and
  reader journeys.

## 12. Corrections to the pre-implementation design

The design proposal attached to #804 was drafted against `master` at
`3fdc3c8`. This audit re-verified it at `f5c59b5` and corrects the record:

- **#805 landed in between.** `ptah atlas migrate test` and
  `ptah atlas schema test` are now working forwards to the native test
  runners, not CE boundary stubs. `workflows/atlas-cli.md` grew from 3,942 to
  4,137 words; `reference/commands.md` was not updated and is now stale on
  those two rows (section 9, item 2).
- **"`migrations baseline` is entirely undocumented" was overstated.** It has
  a contrast note in `workflows/checkpoints.md` and an exit-code row; what is
  missing is a how-to path and its command-table row.
- **"Roughly half of `workflows/migrations.md` is two-sentence stubs" was
  overstated.** Three of fourteen sections are stubs and one more is a
  link-out; the import and safety-gate sections carry real depth. The split
  rationale stands, but for depth-hosting reasons, not because the page is
  hollow.
- **`ptah-ls` was missed entirely** by the design's missing-content list
  (section 9, item 9).
- The four missing native command rows, the `ptah seed` and `ptah viz` gaps,
  the absent brownfield path, the absent directive reference, and the absent
  database-URL reference were all confirmed exactly as designed.

## 13. Tooling constraints that shape the migration

All enforced by scripts in `docs/site/scripts/` (verified by reading them):

- `check-page-health.mjs`: every content page needs `title` and `description`
  frontmatter, no TODO-style markers, and a `slug:` entry in
  `astro.config.mjs`. Every added or moved page must enter the sidebar in the
  same PR; orphan pages are impossible.
- `check-links.mjs`: internal links must be docs-relative and resolve to an
  existing route. Every move must update all inbound links in the same PR;
  content must always link a new home directly, never a redirect URL.
- `check-core-doc-links.mjs`: site pages must not link to root docs on GitHub
  and must not mention the seven protected `docs/*.md` paths. Retiring a
  protected file means editing the script's list in the same PR.
- `check-exit-codes.mjs`: hardcodes `docs/exit_codes.md` and
  `src/content/docs/reference/exit-codes.md`; this is why
  `reference/exit-codes` keeps its URL.
- These scripts scan only `src/content/docs/`, which is why this inventory
  lives at `docs/site/CONTENT_INVENTORY.md` without tripping them.
- The site builds under a `/ptah/<version>/` base (`DOCS_VERSION`, default
  `edge`); redirects for moved URLs must be verified under that base.
  Historical tag builds rebuild their own snapshots, so renames never break
  released versions.
- Moved URLs are declared in the `redirectRoutes` map in `astro.config.mjs`.
  Astro emits redirect destinations verbatim, without the base, so the config
  prepends `/ptah/<version>/` before handing the map to Astro; the emitted
  meta-refresh stubs in `dist/` were verified to carry the base.
  `check-redirects.mjs` (`npm run check:redirects`) enforces that every
  redirect source is a retired route and every target resolves to a real page.
