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
| `index.mdx` (1023 prose words, two inline SVG diagrams) | everyone | What is Ptah and where do I start? | navigation | this page (product model, product-flow diagram, the two workflows and the command that bridges them) | root `README.md` product description, which states the same product model | done: rewritten in place — product sentence in the hero, product-flow diagram with the same five stages as text, one card per sidebar group in sidebar order, "Choose your path" kept, Atlas compatibility and license moved below the product explanation |
| `getting-started.md` (788) | new Go user | How do I try Ptah end to end locally? | tutorial | this page (runnable SQLite flow with expected output and cleanup) | root `README.md` minimal example; `install.md` | done: moved → `start/quick-start`, old URL redirects |
| `install.md` (386) | new user | How do I install, build, and verify the CLI? | howto | this page | root `README.md` "Install Or Build" | done: moved → `start/install`, old URL redirects |
| `documentation-map.md` (441) | everyone | Where is the documentation for task X? | navigation | none (routing only) | the entire sidebar; layers table duplicated on `index.mdx`; maintenance rule overlaps `AGENTS.md` | done: retired — old URL redirects to home; layers and maintenance-rule text moved to `docs/README.md`; this inventory carries the meta function |

### `Start` group (added by the restructuring)

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `start/quick-start.mdx` (routing) | new user | Which quick start should I run? | navigation | this page (the one decision, plus what both quick starts need) | `start/choose-a-workflow` carries the longer version of the same decision | split → `start/quick-start-migrations` and `start/quick-start-declarative`; URL kept, so the `/getting-started/` redirect still resolves |
| `start/quick-start-migrations.mdx` | new user, any language | How do I write a migration, apply it, and roll it back? | tutorial | this page; every command and output block run against a built binary | `versioned/generate` (the hand-written half), `versioned/rollback` | created (stokaro/ptah#1228 onboarding split); keep |
| `start/quick-start-declarative.mdx` | new user, any language | How do I make a database match a file that describes the schema I want? | tutorial | this page; every command and output block run against a built binary | `direct/apply`, `direct/compare-and-drift` | created (stokaro/ptah#1228 onboarding split); keep |
| `start/choose-a-workflow.md` (742) | new user deciding integration shape | Should changes reach my databases as versioned migration files or as direct applies? | concept | this page; command shapes verified against the built binary | `workflows/migrations`, `workflows/atlas-cli`, `reference/comparison` | created (section 9, item 4); keep |
| `start/adopt-an-existing-database.md` (1,000) | brownfield database adopter | How do I put an existing database under Ptah management without recreating it? | howto | this page; `ptah introspect`, `ptah migrations baseline`, `ptah migrations import` runs against the built binary | `workflows/checkpoints` (baseline contrast), `workflows/migrations` import section | created (section 9, item 3); keep |

### `Model your schema` group (added by the restructuring)

Moved pages keep their rows in the groups below with `done` dispositions; this
table lists the pages created in this group.

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `schema/composite.md` (630) | multi-source schema author | How do several schema sources merge into one desired schema? | howto | this page; merge, conflict, and error behavior run against the built binary | per-source `schema/*` pages (each links here instead of restating the rules), `workflows/migrations` compose section | created (canonical multi-source page, deduplicating the compose sections that lived on `workflows/go-schema`, `workflows/schema-files`, and `workflows/migrations`); keep |
| `schema/visualize.md` (560) | any schema author | How do I render schema diagrams? | howto | this page; `ptah viz` runs against the built binary; `examples/viz/` artifacts | `start/install` (Graphviz optional tool), `operate/troubleshooting` (Graphviz symptom) | created (section 9, item 5, grown from `examples/schema-viz`); keep |
| `schema/document.md` (1,658) | any schema author, and whoever reviews their schema | How do I generate a schema reference people can read? | howto | this page; `ptah schema export --to markdown` and `--to html` run against the built binary over the YAML fixture the page shows; the self-containment claim measured by grepping the generated file for external references | `schema/export` (the shared `--to`/`--out`/table-filter flags, canonical there), `schema/visualize` (the standalone diagram), `reference/exit-codes` (`ptah schema export` row) | created (the two documentation targets of `ptah schema export`, reachable but with no reader page); keep — the API contract targets on `schema/export` answer a different question and carry different field-exposure behavior |
| `schema/serve.md` (1,468) | schema author changing models against a live database | How do I watch drift between my models and a database while I work? | howto | this page; `ptah schema serve` run against the built binary over a SQLite fixture, including the drift panel, the 405 refusal, the unreachable-database banner and the exit codes | `direct/compare-and-drift` (the one-shot check and the exit code, canonical there), `schema/document` (the same schema renderer as `--to html`), `reference/native-commands` (§"A live view", a scope note plus the flag table) | created (the live drift view, reachable but with no reader page); keep — a served, refreshing surface and a one-shot pipeline check answer different reader questions |
| `schema/protobuf.md` (2,473) | API developer publishing a Protobuf contract | How do I export entities to Protobuf without breaking wire compatibility? | howto with embedded type-mapping and policy reference tables | this page; `ptah schema export --to protobuf` runs against the built binary; the Edition 2024 rejection re-verified with `protoc` 35.1 and the `WIRE_JSON`/`FILE` contrast with `buf` 1.72.0; backing depth in `docs/api_schema_export.md` | `schema/export` (the shared `--to`/`--out`/table-filter flags, canonical there), `reference/exit-codes` (`ptah schema export` row) | created (#893); keep — the stateful compatibility contract (committed `.proto`, digest header, three policy flags) is too large to live as a section of `schema/export` |
| `schema/validate-and-format.md` (1,062) | schema author adding a pre-commit or pipeline check | How do I check my schema files before anything touches a database? | howto | this page; `ptah schema validate` and `ptah schema fmt` run against the built binary over a YAML fixture and an HCL fixture, including the exit codes | `schema/work-with-a-source` (rendering, canonical there), `testing/ci` (the pull-request contour rows), `reference/exit-codes` (both verb rows) | created (feature-coverage pass: neither verb had a reader page); keep — both answer one question, "is this schema file fit to commit", and neither needs a database |
| `schema/stats.md` (941) | operator putting schema shape on a dashboard | How many objects of each kind does this database hold? | howto | this page; `ptah schema stats` run against the built binary over a SQLite fixture, including the complete OpenMetrics body | `reference/native-commands` §Schema object counts (verb placement and flag rows, canonical there), `reference/exit-codes` (the verb row) | created (split from the `schema/analyze` draft, which taught three independent tasks); keep |
| `schema/lineage.md` (944) | engineer about to drop or rename a column | Which base columns does each view column read? | howto | this page; `ptah schema lineage` run against the built binary over a SQL fixture, each `undecided` reason and the unaliased-expression defect measured separately | `reference/native-commands` §Column lineage (verb placement and flag rows, canonical there), `reference/exit-codes` (the verb row) | created (split from the `schema/analyze` draft, which taught three independent tasks); keep |
| `schema/security.md` (1,515) | engineer reviewing privileges, owners and roles | Which privileges and owners on this database are worth review? | howto | this page; `ptah schema security` run against the built binary, the findings against a live PostgreSQL 18 seeded by the DDL the page shows and the skipped-rule block against SQLite | `reference/native-commands` §Schema security findings (per-rule and per-engine detail, canonical there), `reference/exit-codes` (the `--fail-on` gate), `schema/visualize` (`ptah viz --security` marks the same findings on a diagram) | created (split from the `schema/analyze` draft, which taught three independent tasks); keep |

### `Databases` group (added by the restructuring)

D2 outcome recorded here: `databases/support-matrix` and `databases/postgresql`
were the committed P1 pages. SQLite and SQL Server launched as compact engine
pages as well, because their backing engineering docs carry enough
reader-relevant material (URL forms and rebuild rules for SQLite; connection
`schema` parameter, collation semantics, and filtered-index drift guidance for
SQL Server). MySQL/MariaDB has no dedicated backing doc, so it launched as a
section of the support matrix per the tiering rule and splits out when content
justifies it.

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `databases/support-matrix.md` (831) | every user | Which engines does Ptah support and how does each behave differently? | reference | dialect and capability code (`core/platform`); URL schemes verified against `dbschema` and the built binary | `reference/capabilities` (capability keys), per-engine `databases/*` pages | created (dissolving `reference/dialect-notes`; carries the MySQL/MariaDB, PostgreSQL-compatible, and ClickHouse sections); keep |
| `databases/postgresql.md` (924) | PostgreSQL user | What does Ptah manage on PostgreSQL beyond portable DDL? | reference-flavored guide | this page; behavior from the PostgreSQL feature docs in `docs/` (roles, sequences, UDTs, extension ignore) and capability presets | `reference/go-annotations` (directive syntax, canonical there), `versioned/apply` (lock flags) | created (P1 engine page); keep |
| `databases/sqlite.md` (492) | local/example user | How does SQLite behave differently, and which changes need a rebuild? | reference-flavored guide | this page; SQLite behavior detail in `docs/` | `start/quick-start` (SQLite examples), `testing/migrations-and-schema` (default test databases) | created (compact engine page); keep |
| `databases/sqlserver.md` (714) | SQL Server user | What subset does Ptah support and how do collation and filtered indexes behave? | reference-flavored guide | this page; SQL Server behavior detail in `docs/` | `extend/public-api` (comparison APIs), `reference/go-annotations` (index attributes) | created (compact engine page); keep |

### `Concepts` group (added by the restructuring)

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `concepts/desired-schema-and-sources.md` (369) | every user | What is a desired schema and what counts as a schema source? | concept | this page; source-format independence sourced from the prose that lived on `reference/capabilities` | `schema/composite` (merge rules, canonical there), `start/choose-a-workflow` | created; keep |
| `concepts/migration-directory.md` (465) | versioned-migration user | What exactly is a migration directory and what records what? | concept | this page; sourced from the directory sections that lived on `versioned/overview` (overview now summarizes and links here) | `versioned/overview`, `versioned/integrity-and-safety` | created; keep |
| `concepts/database-urls-and-dev-databases.md` (520) | every user | Which URL formats are accepted, and what are dev, shadow, and throwaway databases? | concept | this page; schemes verified against `dbschema` URL handling and the built binary; flag surfaces verified against `--help` | `versioned/generate`, `versioned/integrity-and-safety`, `testing/migrations-and-schema` (each links here at first term use) | created (section 9, item 8); keep |
| `concepts/dialects-and-capabilities.md` (506) | every user | What is the difference between a dialect, an engine, and a capability? | concept | this page; model sourced from capability code and the concept prose that lived on `reference/capabilities` | `databases/support-matrix`, `reference/capabilities` | created; keep |

### `Direct schema changes` group (added by the restructuring)

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `direct/overview.md` (522) | reader choosing how a change reaches the database | What is the direct workflow, and which verb do I run first? | concept | this page; the `db read` → `schema drift` → `schema plan` → `schema apply --plan` loop and the `migrations status` reading run against the built binary over a SQLite fixture | `start/choose-a-workflow` (the two axes, canonical there), `versioned/overview` (the sibling workflow's index) | created (nested navigation); keep — the group's other four pages are how-tos, and none of them answers what the workflow is or which verb comes first |
| `direct/inspect.md` (631) | any user with a live database | How do I see the schema a live database actually has? | howto | this page; `ptah db read`, `ptah introspect`, and `ptah schema inspect` runs against the built binary | `start/adopt-an-existing-database` (introspect flow, canonical there), `workflows/atlas-cli` (inspect template surface, canonical there) | created (section 10 target, per D5); keep; #850 pass: leads with the native `ptah schema inspect`, compat spelling mentioned secondarily |
| `direct/compare-and-drift.md` (739) | direct-workflow user, CI operator | How does a live database differ from the desired schema, and how do I gate on that? | howto | this page; `ptah schema compare`, `ptah schema drift` (severity, formats, exit codes), and `ptah migrations plan` runs against the built binary | `start/choose-a-workflow` (workflow decision), `versioned/generate` (plan-to-files path), `reference/commands` rows | created (section 10 target, per D5); keep |
| `direct/apply.md` (865) | direct-workflow user | How do I apply desired-schema changes straight to a database? | howto | this page; `ptah schema apply` (dry-run, prompt, auto-approve), `ptah schema plan` plan files, and the stale-plan refusal run against the built binary | `workflows/atlas-cli` (full `schema apply`/`schema plan` flag surface, canonical there), `start/choose-a-workflow` (hybrid pattern) | created (section 10 target, per D5); keep; #850 pass: rewritten to lead with the native `ptah schema apply`/`ptah schema plan` verbs, compat spellings mentioned secondarily |
| `direct/plan-and-approve.md` (1,845) | direct-workflow user, release reviewer | How do I have someone review a schema change and prove the applied SQL is the reviewed SQL? | howto | this page; `ptah schema plan --save`, `ptah schema approve`, `ptah schema verify-approval`, and `ptah schema apply --require-approval` runs against the built binary, including the refusal of an edited plan | `direct/apply` (plan files, canonical for the apply verb), `reference/native-commands` §Plan approval (verb rows) | created (#804 feature-coverage pass); keep |

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
| `workflows/api-schema-export.md` (737) | API developer | How do I export entities to OpenAPI or GraphQL? | howto with an embedded type-mapping reference table | `cmd/schema` `export`; backing depth in `docs/api_schema_export.md` | `docs/api_schema_export.md`; `schema/protobuf` (the third `--to` target — the shared flags stay canonical here, the Protobuf compatibility contract stays there) | done: moved → `schema/export`, old URL redirects; #893 pass: the intro, `--to` row, and `--out` row gained `protobuf` and link to `schema/protobuf` |
| `workflows/migrations.md` (1,031) | versioned-migration user | How do I run the migration lifecycle? | howto hub: 14 sections; "Squashing history", "Testing", and "Reference data" are two-to-three-sentence link-out stubs, "Operational hooks" is a link-out; "Importing from another tool" and "Safety gates" carry real depth | this page for the loop; `migration/migrator/README.md` and `docs/native_cli.md` carry deeper detail | `workflows/checkpoints`, `workflows/testing`, `workflows/reference-data`, `workflows/oci-registry`, `docs/native_cli.md` | done: split → `versioned/overview` (loop + mental model + directory formats), `versioned/generate` (plan, generate, composite, shadow verification, manual create), `versioned/apply` (up, status, hooks, OCI-sourced runs), `versioned/rollback`, `versioned/integrity-and-safety` (hash/validate, dev-database replay, lint, destructive gate, pre-migration checks), `versioned/import`; stub sections dissolved into links; every example rerun against the built binary; old URL redirects to `versioned/overview` |
| `workflows/oci-registry.md` (1,865) | operator/platform engineer | How do I publish, pin, and consume artifacts through an OCI registry? | howto | this page; deeper detail in `docs/oci_registry.md` (2,393 words) | `docs/oci_registry.md`, `workflows/migrations.md` OCI section | done: moved → `operate/oci-registry` in the `Distribute and operate` group (closing links reshaped into a next-steps section), old URL redirects |
| `workflows/checkpoints.md` (884) | versioned-migration user | How do I squash migration history? | howto with concept material (file format, rollback boundary) | this page; `cmd/migratecheckpoint` | currently the only site prose describing `ptah migrations baseline` (the "Checkpoint versus baseline" contrast) | done: moved → `versioned/checkpoints`, old URL redirects; baseline how-to content has its own home in `start/adopt-an-existing-database` |
| `workflows/testing.md` (766) | developer/CI operator | How do I test migrations and schemas declaratively? | howto | this page; `docs/testing.md` | `reference/testing` (intentional howto/reference pair — the model split to generalize), `docs/testing.md` | done: moved → `testing/migrations-and-schema`, old URL redirects |
| `workflows/reference-data.md` (906) | app developer | How do I manage reference/lookup rows declaratively? | howto | this page; `cmd/migratedata` | contains the only prose comparing declarative data with `ptah seed` | done: moved → `versioned/reference-data`, old URL redirects; the seed contrast feeds `operate/seed-data` |
| `workflows/atlas-cli.md` (4,137) | Atlas migration user | How do I use Atlas-style commands with Ptah? | mixed: concept (translation model) + reference (three command tables duplicating `reference/commands.md`) + tutorial (worked example) + status (parity expectations) | `cmd/atlas`; conformance evidence in `stokaro/ptah-atlas-conformance` | `reference/commands.md` Atlas table, `atlas/comparison`, the merged worked example | done: split → `atlas/overview` (concept: surfaces, translation model, waiver boundaries, parity expectations), `atlas/migrate-commands`, `atlas/schema-commands`; verb tables re-verified against the built binary (the schema table gained the missing `schema clean` row); old URL redirects to `atlas/overview`; the dedup with the command reference landed with the `reference/atlas-commands` split: that page is the per-verb status home and the behavior tables here are one-line maps |
| `workflows/ci.md` (371) | CI operator | How do I gate pull requests with Ptah? | howto | this page; `docs/github_action.md` | `docs/github_action.md` | done: moved + rewritten → `testing/ci` (absorbed GitHub Action usage — inputs, outputs, permissions, pinning — from `docs/github_action.md`, which stays as the backing reference), old URL redirects |

### `Examples` group

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `examples/go-model.md` (238) | new Go user | What does a minimal annotated model look like? | howto (worked example) | this page | `workflows/go-schema`, root `README.md` | done: merged → `schema/go-annotations`, old URL redirects |
| `examples/yaml-schema.md` (141) | schema-file user | What does a minimal YAML schema look like? | howto (worked example) | this page | `workflows/schema-files`, `reference/yaml-schema` | done: merged → `schema/yaml`, old URL redirects |
| `examples/atlas-hcl.md` (177) | schema-file user | What does a minimal HCL schema look like? | howto (worked example) | this page | `workflows/schema-files`, `reference/hcl-schema` | done: merged → `schema/hcl`, old URL redirects |
| `examples/atlas-migrations.md` (297) | Atlas migration user | How do I use an Atlas-style migration directory? | howto (worked example) | this page | `workflows/atlas-cli` | done: merged → `atlas/migrate-commands` worked-example section (rerun end to end on the Atlas-compatible surface against the built binary), old URL redirects; the `Examples` sidebar group is retired |
| `examples/schema-viz.md` (289) | any schema author | How do I render schema diagrams? | howto (worked example); currently the only site coverage of the `ptah viz` workflow | this page; `examples/viz/` artifacts | `install.md` (Graphviz optional tool), `operate/troubleshooting` (Graphviz symptom) | done: grown → `schema/visualize` (full `ptah viz` workflow: Mermaid/DOT/SVG, Graphviz prerequisite, committed artifacts), old URL redirects |

### `Reference` group

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `reference/commands.md` (2,026) | every CLI user | What commands exist and what do they do exactly? | reference; the Atlas table stuffs 150–250-word status prose into cells | `cmd/` command tree and `--help` output | `workflows/atlas-cli` tables, `docs/native_cli.md` (near-duplicate native tree) | done: split → `reference/native-commands` (complete per-namespace verb tables; the four missing `migrations` rows and a `ptah completion` row added, every row verified against the built binary's `--help`) + `reference/atlas-commands` (a section per command replacing the giant cells; the canonical per-verb status home — the behavior tables on `atlas/migrate-commands` and `atlas/schema-commands` slimmed to one-line maps pointing at it); old URL redirects to `reference/native-commands` |
| `reference/configuration.md` (523) | every user | What goes in `ptah.yaml` and which setting wins? | reference | this page; deeper `docs/project_config.md` (1,088 words) | `docs/project_config.md`; this page already carries the canonical precedence table, and the duplicate that lived on the comparison page was dropped when it moved to `atlas/comparison` | done: stays at its URL and carries the canonical precedence table (verified: no duplicate precedence table remains on any other page) |
| `reference/yaml-schema.md` (774) | schema-file user | What is the exact YAML schema format? | reference | this page for readers; `docs/yaml_schema.md` (protected by `check-core-doc-links.mjs`) for engineering depth | `docs/yaml_schema.md`, `workflows/schema-files` | keep |
| `reference/hcl-schema.md` (570) | schema-file user | What HCL subset does Ptah support? | reference | this page for readers; `docs/atlas_hcl_schema.md` (protected) | `docs/atlas_hcl_schema.md`, `workflows/schema-files` | keep |
| `reference/atlas-project-config.md` (992) | Atlas migration user | What `atlas.hcl` subset does Ptah read? | reference | this page for readers; `docs/atlas_project_config.md` (protected) | `docs/atlas_project_config.md`, `reference/configuration` | done: moved → `atlas/project-config` (headings to sentence case), old URL redirects |
| `reference/public-api.md` (727) | Go API embedder | Which Go packages are stable to embed? | reference | this page for readers; `docs/public_api.md` (protected) + `docs/public_api.snapshot` + guard scripts | `docs/public_api.md`, `reference/reusable-components` | done: moved → `extend/public-api` (headings to sentence case; `testkit` module linked), old URL redirects |
| `reference/reusable-components.md` (3,167) | Go API embedder | How do I use Ptah as a library? | mixed howto/reference: component map, seven end-to-end examples, seven use-case narratives, comparisons | this page; package docs under `core/`, `migration/`, `dbschema/` | `reference/public-api`, `reference/query-builder`, `examples/migrator/README.md`; its comparisons section overlaps `reference/comparison` | done: moved + tightened → `extend/components` (headings to sentence case; the seven use-case narratives compressed into a table pointing at the end-to-end examples; the comparisons section merged into `atlas/comparison` "Other tools"), old URL redirects |
| `reference/query-builder.md` (2,390) | Go API embedder | How do I build dialect-aware queries with `core/query`? | reference | `core/query` package | `reference/reusable-components` | done: moved → `extend/query-builder` (title to sentence case), old URL redirects |
| `reference/testing.md` (487) | developer/CI operator | What are the exact test-command flags and YAML model? | reference | `cmd/migrationstest`, `cmd/schema` test, `docs/testing.md` | `workflows/testing` (the intentional pair) | done: moved/renamed → `reference/test-cases` (clarifies it is not about testing Ptah itself), old URL redirects |
| `reference/capabilities.md` (528) | every user | Which features work on which dialect? | reference with concept prose mixed in | `docs/capabilities.md` (protected) and capability code | `docs/capabilities.md`, `reference/dialect-notes` | done: kept + tightened — the capability model moved to `concepts/dialects-and-capabilities`, the dialect-coverage summary to `databases/support-matrix`, and the source-format-independence prose to `concepts/desired-schema-and-sources`; the page now carries the capability-key, testing, and OCI tables only |
| `reference/dialect-notes.md` (495) | every user | How does my engine behave differently? | reference (six engines compressed into one short page) | dialect code and `docs/sqlite.md`, `docs/sqlserver.md`, PostgreSQL feature docs | `reference/capabilities`, `docs/sqlite.md`, `docs/sqlserver.md` | done: dissolved → `databases/support-matrix` (per-engine sections), with engine depth on `databases/postgresql`, `databases/sqlite`, and `databases/sqlserver`; old URL redirects to `databases/support-matrix` |
| `reference/comparison.md` (4,795) | evaluator/Atlas migration user | At least four distinct questions: how Ptah positions against Atlas; command parity; evidence per feature; known gaps; config precedence; safety/exit behavior | mixed status/reference; evidence-table cells run past 400 words | conformance evidence in `stokaro/ptah-atlas-conformance`; command claims from `cmd/` | `atlas/overview`, `reference/commands`, `reference/configuration`, `reference/exit-codes`, `atlas/conformance` | done: moved + slimmed → `atlas/comparison`; the duplicated config-precedence table was dropped (the canonical table already lives on `reference/configuration`) and the safety/exit table became pointers to `versioned/integrity-and-safety` and `reference/exit-codes`; live (10 → 39 observations) and Atlas CE differential (5 → 30 observations) evidence rows refreshed against the current conformance reports; old URL redirects; its hand-maintained Atlas Pro analyzer table later moved to `reference/lint-rules` (#1482), where every analyzer code carries a status and the Ptah side is generated from the registries |
| `reference/atlas-docs-coverage.md` (3,164) | Atlas migration user/maintainer | Which Atlas documentation areas does Ptah cover? | status (crosswalk matrix; "Research date" convention) | this page, refreshed against Atlas docs and conformance runs | `atlas/comparison`, `atlas/conformance` | done: moved → `atlas/docs-coverage` (research date refreshed to July 28, 2026; headings to sentence case), old URL redirects |
| `atlas/retained-divergences.md` (1,481) | Atlas migration user/maintainer | Which `ptah-compat` refusals are deliberately retained although the pinned Atlas community binary exits `0`? | status | `cmd/atlas/compat_1241_retained_divergence_test.go`; measurements recorded in `stokaro/ptah#1241` | `atlas/comparison`, `atlas/conformance`, `reference/atlas-commands` | created (#1241 retained-divergence register); keep |
| `reference/exit-codes.md` (1,652) | CI operator | What exit code means what? | reference | `docs/exit_codes.md` is the script-checked source (`check-exit-codes.mjs` hardcodes both paths) | `docs/exit_codes.md` (deliberate, mechanically checked copy) | keep at the same path (script-coupled) |

### `Reference` group pages added by the restructuring

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `reference/go-annotations.md` (2,895) | Go schema author | Which directives and attributes does the annotation parser accept? | reference | `internal/annotationmeta` exported by `ptah schema annotations` (committed copy: `schemas/ptah-annotations.schema.json`); placement, bare-boolean, and unknown-attribute behavior spot-checked against the built binary | `schema/go-annotations` (workflow home), directive fragments in `docs/POSTGRESQL_ROLES.md`, `docs/sequences.md`, `docs/user_defined_types.md` | created (section 9, items 7 and 9, including the `ptah-ls` editor-support section); keep |
| `reference/lint-rules.md` (3,692) | CI operator, migration author, Atlas migration user | Which lint identifier is this, which command can report it, and is the name Atlas's or Ptah's? | reference | the rule registries themselves: `migration/lint` and `internal/sqllint`, joined in `internal/lintcatalog` and rendered between generated markers; the apply-gate scope is rendered from `internal/migrationlintgate`; the Atlas analyzer check list is a reading of <https://atlasgo.io/lint/analyzers> | `atlas/comparison` (its Pro analyzer table moved here), `versioned/integrity-and-safety` and `atlas/migrate-commands` (lint workflow, canonical there), `reference/exit-codes` (what a blocking finding exits with) | created (#1482); keep — the generated block is gated by `scripts/check-lint-rules.sh` and by a Go test, so a rule added to either registry cannot leave the page behind |

### `Distribute and operate` group (added by the restructuring)

`operate/oci-registry` and `operate/troubleshooting` keep their rows in the
`Use Ptah` and `Operate` tables with `done` dispositions; this table lists the
page the restructuring created.

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `operate/ai-agents.md` (1568) | operator/app developer | How do I connect an AI client to Ptah, and what may it read, propose, and write? | howto | this page; `cmd/mcp`, `internal/mcpserver` and `internal/agentapi`; every tool name and flag run against the built binary over the protocol | `reference/native-commands` (`ptah mcp` row and section, summary there), `atlas/feature-matrix` (the Copilot row's Ptah answer) | created (stokaro/ptah#1487); keep |
| `operate/ai-assist.md` (1820) | operator/app developer | How do I point Ptah Assist at my own model, and check that it works? | howto | this page; `cmd/assist`, `internal/aiprovider` and `internal/assistconfig`; every command run against the built binary, and the conversation, resume and write paths measured against a live endpoint | `operate/ai-agents` (the other surface, no provider needed), `reference/native-commands` (the two command rows) | created (stokaro/ptah#1488); keep |
| `operate/inference-migrations.md` (1080) | operator/app developer | How do I change an embedding model without breaking what queries read? | howto | this page; `cmd/inference` and the `internal/embed*` packages; every command and flag run against the built binary, and the lifecycle measured against a live PostgreSQL with pgvector | `reference/native-commands` (the ten verb rows), `operate/oci-registry` (where the evidence goes) | created (stokaro/ptah#2068); keep |
| `operate/seed-data.md` (506) | app developer/operator | How do I load one-off, environment-scoped setup rows? | howto | this page; `cmd/seed` and `migration/seeder`; every command and output run against the built binary | `versioned/reference-data` (declarative contrast, canonical there), `reference/exit-codes` (`ptah seed` row) | created (section 9, item 6); keep |

### `Operate` group

| Page (words) | Audience | Reader question | Type | Source of truth | Overlaps | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| `operate/troubleshooting.md` (390) | every user | My command failed — what now? | troubleshooting (seven symptom entries, already symptom-first) | this page | none significant | keep + grow: absorb failure modes surfaced by the workflow rewrites; stays symptom → cause → diagnosis → fix → verify |
| `operate/conformance.md` (503) | evaluator | Where is the Atlas-compatibility evidence and how do I read it? | status | `stokaro/ptah-atlas-conformance` reports (`gaps*.md`, `PARITY.md`) | `atlas/comparison`, `atlas/docs-coverage`, `docs/conformance.md` | done: moved → `atlas/conformance`, old URL redirects |
| `operate/license-boundary.md` (190) | evaluator/legal reviewer | How is Ptah kept license-clean relative to Atlas? | concept | this page; root `README.md` boundary diagram | root `README.md` | done: moved → `atlas/license-boundary`, old URL redirects |

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
| `public_api.md` (878) | contributor/embedder | API guardrails, snapshot process | `extend/public-api` | keep (protected; pairs with `public_api.snapshot`, `public_api_approvals.txt`) |
| `capabilities.md` (2,544) | contributor | Full capability matrices | `reference/capabilities` | keep (protected) |
| `sqlite.md` (532) | contributor | SQLite behavior detail | `databases/sqlite` | keep (protected); done: reader essentials absorbed by `databases/sqlite` |
| `sqlserver.md` (1,269) | contributor | SQL Server behavior detail | `databases/sqlserver` | keep (protected); done: reader essentials absorbed by `databases/sqlserver` |
| `native_cli.md` (1,267) | contributor | Native command tree walkthrough | `reference/native-commands`, `atlas/comparison`, and the command workflow pages | done: retired after its command, analyzer, plan, scoping, safety, waiver, and exit-code material gained canonical site homes |
| `go_annotations_vs_atlas_hcl.md` (727) | evaluator | Source-format comparison and Go-to-HCL export behavior | `schema/go-annotations`, `reference/go-annotations`, and `reference/hcl-schema` | done: retired after the workflow, parity contract, cleanup behavior, and format guidance gained canonical site homes |
| `migrations-import.md` (720) | contributor | Import converter detail | `workflows/migrations` import section | keep as backing reference for `versioned/import` |
| `pre-migration-checks.md` (727) | contributor | `-- +ptah check` directive detail | `workflows/migrations` safety section | keep as backing reference for `versioned/integrity-and-safety` |
| `oci_registry.md` (2,393) | contributor | OCI transport detail | `workflows/oci-registry` | keep as backing reference |
| `api_schema_export.md` (764) | contributor | Export mapping detail | `workflows/api-schema-export` | keep as backing reference |
| `testing.md` (1,081) | contributor | Declarative testing detail | `testing/migrations-and-schema` + `reference/test-cases` | keep as backing reference |
| `github_action.md` (627) | contributor | GitHub Action detail | `testing/ci` | keep as backing reference for `testing/ci` |
| `project_config.md` (1,088) | contributor | `ptah.yaml` full reference | `reference/configuration` | keep as backing reference |
| `conformance.md` (522) | contributor | Conformance process detail | `operate/conformance` | keep as backing reference |
| `roadmap-post-ga.md` (1,829) | maintainer/evaluator | Evidence-dated classification of open work deferred beyond GA | none; this is the issue-roadmap status source | keep; refresh the evidence snapshot when issue labels, states, or supporting claims change |
| `online-ddl.md` (881) | contributor | Online-DDL behavior (gh-ost / pt-osc routing — MySQL/MariaDB material, not PostgreSQL as the design assumed) | `databases/support-matrix` MySQL/MariaDB section | done: reader summary lives on `databases/support-matrix`; keep as engineering depth |
| `postgresql_extension_ignore.md` (687) | contributor | Extension-ignore behavior | `databases/postgresql` | done: reader summary in the extensions section of `databases/postgresql`; keep |
| `POSTGRESQL_ROLES.md` (1,453) | contributor | Roles/RLS annotations | `databases/postgresql` | done: reader summary on `databases/postgresql`; directive syntax on `reference/go-annotations`; keep |
| `sequences.md` (674) | contributor | Sequence annotations | `databases/postgresql` | done: reader summary on `databases/postgresql`; directive syntax on `reference/go-annotations`; keep |
| `user_defined_types.md` (586) | contributor | UDT annotations | `databases/postgresql` | done: reader summary on `databases/postgresql`; directive syntax on `reference/go-annotations`; keep |
| `dml_upsert.md` (352) | contributor | Upsert rendering | `databases/sqlserver` | done: one-line surface note on `databases/sqlserver`; keep as engineering depth for embedders |
| `system_design.md` (1,809) | contributor | Architecture overview | none (intentional) | keep (contributor surface, out of reader navigation) |
| `release_process.md` (295) | maintainer | Release steps (also the only mention of the `ptah-ls` binary anywhere in the docs) | none | keep (maintainer surface) |
| `diagrams/*.mmd` (2 files) | contributor | Architecture diagrams | none | keep |

## 3. Root and package READMEs

| File (words) | Audience | Purpose | Overlaps | Disposition |
| --- | --- | --- | --- | --- |
| `README.md` (643) | everyone landing on GitHub | Project pitch, start-here table, install, surfaces, compatibility status | `index.mdx`, `install.md`, `getting-started.md`, `operate/license-boundary` | keep + rewrite links when site URLs move; must never diverge from the site on parity claims |
| `docs/site/README.md` (107) | contributor | How to build the site | none | keep |
| `testkit` README (178) | Go embedder | Test-harness package (separate repository and module) | `extend/public-api` | moved to stokaro/ptah-testkit; linked from `extend/public-api` |
| `internal/parser/README.md` (1,331) | contributor | SQL parser internals | none | keep (contributor surface) |
| `migration/generator/README.md` (1,222) | contributor/embedder | Generator package detail | `extend/components` | keep; canonical for package-level API detail |
| `migration/migrator/README.md` (2,887) | contributor/embedder | Migrator package detail | `workflows/migrations`, `extend/components` | keep; canonical for package-level API detail |
| `cmd/lint/testdata/sarif/README.md` (46) | contributor | Test-fixture note | none | keep (not reader-facing documentation) |

## 4. Examples (`examples/**`)

| Path | Audience | Purpose | Site counterpart | Disposition |
| --- | --- | --- | --- | --- |
| `examples/migrator/README.md` (340) + `migrations/` fixtures | Go embedder | Embedded-migrator runnable example | `extend/components` "Embed the migrator" | keep; linked from `extend/components` |
| `examples/viz/README.md` (178) + committed `schema.{mmd,dot,sql,svg}` artifacts | schema author | Visualization runnable example with generated artifacts | `schema/visualize` | keep; the artifact backing for `schema/visualize` |
| `examples/annotation_parser/` (no README) | contributor | Parser API demo | `internal/parser/README.md` | keep; no reader-facing obligation |
| `examples/extension_ignore/` (no README) | contributor | Extension-ignore demo | `docs/postgresql_extension_ignore.md` | keep; no reader-facing obligation |
| `examples/reusable_components/` (test only) | contributor | Executable doc-examples for `extend/components` | `extend/components` | keep; keeps site snippets honest |

## 5. Integration docs (`integration/*.md`)

| File (words) | Audience | Purpose | Disposition |
| --- | --- | --- | --- |
| `integration/README.md` (1,571) | contributor | Integration test framework guide | keep (contributor surface, out of reader navigation) |
| `integration/DYNAMIC_TESTING.md` (617) | contributor | Dynamic test scenarios | keep |
| `integration/MIGRATION_GENERATOR_VALIDATION.md` (654) | contributor | Generator validation harness | keep |

## 6. Reader journeys

The ten journeys required by #804, plus the contributor journey, with the path
each one took before the restructuring (and its friction) and the path it takes
through the navigation the sidebar declares today. Section 10's tree is
considered valid only because each journey below resolves by reading the
sidebar, without consulting a meta-map.

Paths are written the way the sidebar reads: `Group > Subgroup > Page`, using
the labels a reader sees. A step with no subgroup is a leaf directly inside its
top-level group.

| Journey | Before the restructuring | Path through the navigation |
| --- | --- | --- |
| Migration-first user, any language | `start/quick-start-migrations` runs the whole hand-written migration workflow — write, hash, validate, apply, status, read back, roll back — before any schema source appears | Home → `Start > Quick start` → `Start > Quick start: versioned migrations` → `Workflows > Versioned migrations > Overview` |
| New Go user | reaches the quick start like everybody else and finds Go as one of four schema sources rather than as the entry point | Home → `Start > Quick start` → either tutorial → `Schema > Sources > Go annotations` → `Workflows > Versioned migrations > Overview` |
| Schema-file user | `workflows/schema-files` covers four formats on one page; reference pages are two clicks away | Home → `Start > Choose a workflow` → `Schema > Sources > Work with a desired schema` → the format page beside it (`SQL schema`, `YAML schema`, `HCL schema`, `DBML`) |
| ORM/external-provider user | `workflows/orm-loaders`, duplicated by a section of `schema-files` | `Schema > Sources > ORM and external loaders`, a sibling of the hand-written formats rather than a separate group |
| Brownfield database adopter | no path: `ptah introspect` appears only in reference tables, `migrations baseline` has no how-to (only a contrast note in `checkpoints` and an exit-code row) | `Start > Adopt an existing database` → `Workflows > Direct schema changes > Inspect a database` → `Workflows > Versioned migrations > Import from another tool` or `Checkpoints` |
| Versioned-migration user | `workflows/migrations` hub; per-step depth requires jumping to four other pages | `Workflows > Versioned migrations > Overview` → the seven lifecycle pages under the same subgroup, all visible at once |
| Direct-workflow user | no home: `schema compare`/`drift` live in command tables; direct apply is only inside `workflows/atlas-cli` | `Start > Choose a workflow` → `Workflows > Direct schema changes > Overview` → the four verbs under it, in the order a change passes through them |
| CI/operator | `workflows/ci` (371 words) → `reference/exit-codes`; safety gates buried in `workflows/migrations` | `Workflows > Test and CI > CI` → `Workflows > Versioned migrations > Integrity and safety` → `Concepts and reference > Rules and diagnostics > Exit codes` → `Workflows > Distribute and operate > Troubleshooting` |
| Atlas migration user | `workflows/atlas-cli` (4,137 words, four page types mixed) → `reference/comparison` (4,795 words, four questions mixed) | `Atlas compatibility > Overview` → `Adopting an Atlas project` → `Commands and configuration > Atlas migrate commands` / `Atlas schema commands` → `Differences and evidence > Feature matrix` / `Conformance`, the one collapsed subgroup, because evidence sits outside the learning path |
| Go API embedder | `reference/reusable-components` + `reference/public-api` + `reference/query-builder`, all filed under generic `Reference` | `Integrations > Go integration > Public Go API` → `Reusable components` → `Query builder` |
| Contributor | `AGENTS.md` → skill; no style guide exists yet | `AGENTS.md` → `docs/STYLE_GUIDE.md` → skill → this inventory |

Two properties of the tree are what make these paths short, and both are stated
here because section 10 records the tree and this section records the walking
of it:

- **A journey crosses a top-level group boundary at most twice**, counting
  from the first group it lands in — Home is the splash page and belongs to no
  group. Two rows reach two crossings: the new Go user (`Start` → `Schema` →
  `Workflows`) and the CI/operator (`Workflows` → `Concepts and reference` →
  `Workflows`). The CI/operator row is the longest at four stops, and it is
  long inside its groups rather than across them, because the questions are
  genuinely four: how you test, what protects a directory, what an exit code
  means, and what to do when it fails.
- **Every sibling list a journey lands in is readable without scrolling past
  it.** Measured from `src/sidebar.mjs`: no list of siblings anywhere in the
  tree runs past eight, and the two that reach eight — `Versioned migrations`
  and `Sources` — are the two a reader arrives at deliberately rather than
  scans.

## 6a. The onboarding narrative (stokaro/ptah#1228)

The quick start taught Ptah through annotated Go structs: the first thing a
reader wrote was Go, and every later step was driven from it. That made a schema
source look like the product's identity, and it made the page unusable as an
introduction for a reader who does not write Go.

`start/quick-start` is now a routing page carrying one decision, and the work
sits in two tutorials below it. `start/quick-start-migrations` writes a
migration by hand and takes it through hash, validate, apply, status, read back
and roll back. `start/quick-start-declarative` keeps one `schema.sql`, applies
it, adds a column, and checks for drift. Neither needs an application language,
a database server, or a source checkout: both start from an installed binary
and a local SQLite file.

The four ways to declare a schema — SQL, YAML, HCL, Go — live on
`schema/work-with-a-source`, which owns the question and states the one place
they render differently rather than smoothing it over: an HCL column is
`NOT NULL` unless it says `null = true`. Deriving a migration from a desired
schema lives on `versioned/generate`. The quick starts link to both instead of
re-hosting them.

The landing page follows the same rule. Its "Choose your path" table leads with
migrations you write yourself, an existing migration directory, and a live
database, and "Model your schema" points at the format-neutral concept page
rather than at the Go one.

## 6b. The two workflow axes (stokaro/ptah#1228)

`start/choose-a-workflow` framed the decision as one axis — versioned files
versus direct applies — and stated that both read a desired schema. That is
true of the direct workflow and false of the versioned one: only `plan` and
`generate` read a schema source, and a project can run entirely on migrations
it writes by hand.

The page now states two independent questions — where the change comes from,
and how it reaches the database — with the four combinations in a table. One
cell is empty and says why: a direct apply computes a difference, so with no
description of the schema you want there is nothing to compute against.

`versioned/overview` follows the same shape: the two origins of a migration
file are named side by side, and the lifecycle they share is one section rather
than a continuation of the generated one. `versioned/generate` covers both
origins and no longer describes a hand-written migration as what you reach for
when generation cannot express the change.

## 6c. Formats as peers (stokaro/ptah#1228)

`schema/go-annotations` carried the workflow for every source: comparing,
planning, generating, applying, composing, and rendering across dialects were
all explained there in terms of `--root-dir`, and the YAML, HCL and SQL pages
read as adapters onto it. A reader who kept HCL learned the workflow from a
page about Go.

`schema/work-with-a-source` now holds those operations once, with the source
flag in synchronized tabs — `--schema-file`, `--root-dir`, `--schema-cmd` — and
the format pages keep what is genuinely theirs: syntax, supported constructs,
and source-specific behavior. The Go page keeps its annotation model and its
HCL export path, plus the one operation worth repeating there: rendering more
than one dialect, because portable annotations are where a mapping surprise
shows.

The concept page points at the shared page first and at the format pages after
it, in that order.

## 6d. Where tabs are, and where they are not (stokaro/ptah#1228)

Tabs are used where one operation has genuinely equivalent expressions, and
nowhere else:

| Page | Tabs | Why they are equivalent |
| --- | --- | --- |
| `start/quick-start-migrations`, `start/quick-start-declarative` | Bash, PowerShell | one tutorial step in two shells; every `ptah` invocation is identical in both panels, and only the shell's own commands differ. `internal/quickstart` reads each panel as the program for its shell, so a Windows reader is shown PowerShell throughout and CI runs what the panel says |
| `schema/work-with-a-source` | the source flag | `--schema-file`, `--root-dir`, `--schema-cmd` name the same input |
| `start/adopt-an-existing-database` | Go, HCL | one adoption path, two things to keep afterwards |
| `reference/configuration` | `ptah.yaml`, `atlas.hcl` | a native command reads either through `--env`; measured with `migrations status --env dev` beside both |

They all share `syncKey`, so a reader who picks a format keeps it across pages.

Where they are deliberately absent: `direct/apply` and `versioned/generate`
name both source flags in one line of prose, and a tab whose only content is a
different flag name would be a tab created to make every format appear
everywhere -- which the issue asks not to do. `reference/configuration` also
says where the two files are NOT interchangeable, because `--config` names a
`ptah.yaml` and refuses an `atlas.hcl` by name.

## 6e. The pages the review left alone, and why

The structural review #1228 asks for covered every reader-facing section. Most
needed nothing: `direct/*`, `versioned/*` past the overview, `testing/*`,
`operate/*`, `databases/*`, `atlas/*`, `extend/*` and the reference pages
describe operations rather than a schema source, and already name both flags
where a source is named.

Four places led with Go where the operation is format-neutral, and were
levelled rather than restructured: the OCI publish example now shows a file
source first with the Go flag named beside it, `direct/compare-and-drift` says
its Go models are the example rather than the requirement, `testing` lists the
sources `schema test` accepts without putting one first, and the landing page's
path table leads with the language-neutral rows.

No page was moved for cosmetic reasons, and the sections that already served
the general-purpose narrative were left as they were.

## 7. Terminology audit

Verified with repository-wide searches at the audited commit.

| Term | Current usage (measured) | Canonical decision |
| --- | --- | --- |
| native commands | consistent: "native" used for the `ptah <verb>` tree | keep; never described with Atlas spellings; root-level Atlas aliases are documented as intentionally absent |
| Atlas-compatible commands | `ptah-compat <command> ...` invocations across the atlas group and references | #850 pass: the `ptah atlas` namespace was removed from the main binary; Atlas-compatible invocations are spelled `ptah-compat <command> ...` (there is no `atlas` command), with the drop-in rename documented once on the Atlas compatibility overview |
| `ptah-compat` | the only Atlas-compatible command surface | #850 pass: promoted from prose-only mention to the documented host of the Atlas-compatible tree (including the `atlas/comparison` command-parity column) |
| desired schema vs desired state | "desired schema" appears 60 times across 18 pages; "desired state" 3 times (`workflows/migrations.md` twice, `workflows/schema-files.md` once) | standardize on **desired schema**; retire "desired state" outside the composite-source discussion (done: the final pass also replaced the hyphenated "desired-state" uses in Atlas status prose) |
| schema source | used informally | canonicalize: Go annotations, YAML, HCL, SQL file, external loader, or live database used as input |
| direct schema changes vs declarative schema changes | the nested-navigation pass measured the exact phrase "declarative schema changes" 4 times across 2 pages (`index.mdx` frontmatter description, hero tagline, and the H3 at line 171; `atlas/conformance` under a **Native Ptah.** label) and "declarative schema management" 3 times across 2 pages, against "Direct schema changes" as the sidebar group, the home page card, and the `start/choose-a-workflow` heading | standardize on **direct schema changes** for the workflow that runs a difference against the database with no migration file in between; retire "declarative schema changes" and "declarative schema management" as names for it, because `ptah migrations generate` reads the same desired schema and "declarative" therefore names where a change came from rather than how it lands. "Declarative" stays correct for the authoring model (`concepts/desired-schema-and-sources`) and inside capability names that carry it, such as declarative reference data and declarative test cases (done under `docs/site/src/content/docs/` and `docs/conformance.md`: 0 occurrences of either retired phrase remain; `direct/overview` states the reasoning in a heading, and `docs/STYLE_GUIDE.md` section 7 carries the rule. Outstanding: the root `README.md` opening sentence and its "Two ways to change a schema" table row, plus the verbatim quote of that sentence in `docs/roadmap-post-ga.md`, are left for the README rewrite that owns the file) |
| dev database / shadow database / throwaway database | all three exist and are real, distinct flags: `--dev-url` (replay validation on `migrations validate`, `migrations lint`, Atlas-compatible verbs), `--shadow-db` (`migrations generate`, `checkpoint`, `baseline` verification replay), throwaway databases in `migrations test` / `schema test` | keep all three as distinct terms; define each once in `concepts/database-urls-and-dev-databases` and link instead of re-defining (done: page exists and first uses link to it) |
| migration directory / integrity file / revision table | consistent; integrity files are `ptah.sum` (native) and `atlas.sum` (Atlas-format) | keep |
| dialect vs database/engine | mostly consistent; `reference/dialect-notes` blurs the two | dialect = SQL rendering flavor; database/engine = the product you connect to (done: `concepts/dialects-and-capabilities` defines both and the `Databases` group replaced `dialect-notes`) |
| capability | consistent | keep: per-dialect feature gate |
| drift | consistent (`ptah schema drift`) | keep |
| conformance | consistent; evidence lives in `stokaro/ptah-atlas-conformance` | keep; claims must cite current reports |
| clean-room / license-clean | consistent | keep; never describe Atlas internals |
| heading case | mixed: workflow pages use sentence case; several reference pages use Title Case ("Supported Blocks", "MySQL And MariaDB", "Rule Of Thumb") | standardize on sentence case (style guide rule; done: the final pass corrected the remaining Title Case headings on `reference/yaml-schema`, `reference/hcl-schema`, and `reference/exit-codes`) |

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
   `reference/native-commands`. Done: rows added (plus `ptah completion`),
   every row verified against the built binary's `--help`.
2. **Two stale Atlas rows.** `reference/commands.md` still lists
   the Atlas-compatible `migrate test` and `schema test` among "Registered
   Atlas CE boundary stubs"; both were flipped to working forwards of the
   native test runners at the audited commit itself (`f5c59b5`, #805, which
   updated `workflows/atlas-cli.md`, `workflows/testing.md`, exit codes, and
   conformance pages but not `reference/commands.md`). Fix wherever the Atlas
   command reference lands first. Done: both verbs are documented as working
   forwards on `reference/atlas-commands`.
3. **Brownfield adoption path.** `ptah introspect` and
   `ptah migrations baseline` ship today, but no page teaches adopting Ptah on
   an existing database. Baseline's only site presence is a contrast note in
   `workflows/checkpoints.md` and an exit-code row. New page:
   `start/adopt-an-existing-database`. Done: page created with introspect,
   drift-check, baseline (dry-run and `--shadow-db`), and import flows run
   against the built binary.
4. **Workflow choice.** No page presents the versioned versus direct versus
   hybrid decision; it is implied by `reference/comparison` tables only.
   New page: `start/choose-a-workflow`. Done: page created.
5. **`ptah viz` workflow page.** Only an example page exists; the workflow
   (formats, Graphviz prerequisite, committed artifacts) is scattered across
   `install.md` and `operate/troubleshooting.md`. New page: `schema/visualize`.
   Done: page created with every command run against the built binary.
6. **`ptah seed` page.** The command ships (`cmd/seed`,
   environment-scoped SQL seed files) but has no page; it appears only in the
   `reference-data` contrast, command row, and exit-code rows. New page:
   `operate/seed-data`. Done: page created with the naming convention,
   apply/no-op/protected-environment/`--force --idempotent` flows and outputs
   run against the built binary.
7. **Go annotation directive reference.** No page on the site (or in
   `docs/*.md`) enumerates the `//ptah:schema:*` directives; the grammar
   lives in `core/goschema` and `internal/annotationmeta`, with fragments in
   `docs/POSTGRESQL_ROLES.md`, `docs/sequences.md`, and
   `docs/user_defined_types.md`. New page: `reference/go-annotations`. Done:
   page created from the `ptah schema annotations` JSON Schema export (all 21
   directives with attribute tables, placement rules, and bare-boolean and
   platform-override syntax), with behavior spot-checked against the built
   binary.
8. **Database URL and dev-database reference.** No central page documents
   accepted URL formats (`sqlite://`, `postgres://`, ...) or distinguishes
   `--dev-url`, `--shadow-db`, and throwaway test databases. New page:
   `concepts/database-urls-and-dev-databases`. Done: page created with schemes
   verified against the URL-handling code and the built binary, and the pages
   that use the terms now link to it at first use.
9. **`ptah-ls` is undocumented.** The repository ships an annotation language
   server binary (`cmd/ptah-ls`, `internal/ptahls`) that is built and verified
   during releases (`docs/release_process.md`), yet no reader-facing page
   mentions it. Proposed home: an editor-support section in
   `reference/go-annotations`, with an install note in `start/install`.
   (Found by this audit; not in the pre-implementation design.) Done: the
   editor-support section is on `reference/go-annotations`, and
   `start/install` lists `ptah-ls` as an optional tool with an install note.
10. **Maintenance how-to.** `migrations edit`, `rebase`, `rm`, and `repair`
    have command rows (except `repair`, see item 1) and exit-code rows but no
    how-to home. New page: `versioned/maintain-history`. Done: page created
    with every verb (including the dirty-state repair and `--resume-from`
    flows) run against the built binary.

## 10. Target navigation and page map

Ordering principle: reading order for a new user first, lookup surfaces last.
The hierarchy is two levels — a top-level group holds subgroups, a subgroup
holds pages — so that no list of siblings runs much past eight. The second
level exists because four flat groups ran past that: `Model your schema` at 17
entries, `Atlas compatibility` and `Reference` at 11 each, and `Versioned
migrations` at 9. Group rationale follows the tree. Current → new mappings are
in the disposition columns of sections 1–2.

The tree below is the sidebar `docs/site/src/sidebar.mjs` declares. Each line
carries the label the reader sees and the slug behind it; a label of `Overview`
marks a section index whose page title would otherwise repeat the group above
it.

```text
Home (index.mdx, splash; renders no sidebar)
Start
  Install Ptah                        start/install
  Quick start                         start/quick-start (navigation: one decision, two children)
  Quick start: versioned migrations   start/quick-start-migrations
  Quick start: declarative changes    start/quick-start-declarative
  Choose a workflow                   start/choose-a-workflow
  Adopt an existing database          start/adopt-an-existing-database
Workflows
  Versioned migrations
    Overview                          versioned/overview
    Generate migrations               versioned/generate
    Apply migrations                  versioned/apply
    Roll back migrations              versioned/rollback
    Integrity and safety              versioned/integrity-and-safety
    Maintain migration history        versioned/maintain-history
    Import from another tool          versioned/import
    Checkpoints                       versioned/checkpoints
  Direct schema changes
    Overview                          direct/overview
    Inspect a database                direct/inspect
    Compare and drift                 direct/compare-and-drift
    Plan and approve changes          direct/plan-and-approve
    Apply directly                    direct/apply
  Test and CI
    Test migrations and schemas       testing/migrations-and-schema
    CI                                testing/ci
  Load data
    Reference data                    versioned/reference-data
    Seed data                         operate/seed-data
  Distribute and operate
    OCI registry artifacts            operate/oci-registry
    Troubleshooting                   operate/troubleshooting
Schema
  Sources
    Work with a desired schema        schema/work-with-a-source
    SQL schema                        schema/sql
    YAML schema                       schema/yaml
    HCL schema                        schema/hcl
    DBML                              schema/dbml
    Go annotations                    schema/go-annotations
    ORM and external loaders          schema/orm-and-external
    Composite desired schema          schema/composite
  Analysis and documentation
    Validate and format schema files  schema/validate-and-format
    Visualize the schema              schema/visualize
    Generate schema documentation     schema/document
    Serve a live schema view          schema/serve
    Count schema objects              schema/stats
    Trace view column lineage         schema/lineage
    Report schema security findings   schema/security
  Contract exports
    API schema export                 schema/export
    Protobuf schema export            schema/protobuf
Databases
  Database support matrix             databases/support-matrix
  PostgreSQL                          databases/postgresql
  SQLite                              databases/sqlite
  SQL Server                          databases/sqlserver
Integrations
  Go integration
    Public Go API                     extend/public-api
    Reusable components               extend/components
    Query builder                     extend/query-builder
  AI and agents
    AI agents over MCP                operate/ai-agents
    Ptah Assist and your own model    operate/ai-assist
Atlas compatibility
  Overview                            atlas/overview
  Adopting an Atlas project           atlas/adoption
  Commands and configuration
    Atlas migrate commands            atlas/migrate-commands
    Atlas schema commands             atlas/schema-commands
    Atlas project config              atlas/project-config
  Differences and evidence            (collapsed)
    Feature matrix                    atlas/feature-matrix
    Comparison                        atlas/comparison
    Retained divergences              atlas/retained-divergences
    Conformance                       atlas/conformance
    Atlas docs coverage               atlas/docs-coverage
    License boundary                  atlas/license-boundary
Concepts and reference
  Concepts
    Desired schema and schema sources  concepts/desired-schema-and-sources
    The migration directory           concepts/migration-directory
    Database URLs and dev databases   concepts/database-urls-and-dev-databases
    Dialects and capabilities         concepts/dialects-and-capabilities
  Command reference
    Native commands                   reference/native-commands
    Atlas-compatible commands         reference/atlas-commands
    Database test commands            reference/test-cases
  Format reference
    Configuration                     reference/configuration
    Go annotation reference           reference/go-annotations
    HCL schema reference              reference/hcl-schema
    YAML Schema Reference             reference/yaml-schema
  Rules and diagnostics
    Capabilities                      reference/capabilities
    Lint rules                        reference/lint-rules
    Exit codes                        reference/exit-codes
    Glossary                          reference/glossary
```

Retired from the sidebar: `documentation-map` (redirect to home), the
`Examples` group (content merged where the decision is made), and the
`Use Ptah` and `Operate` labels. The second level retires no page and no
route: every entry the flat sidebar carried has a place in the nested one.

A group heading is a `<summary>` and can never be a link, so a section index is
an ordinary leaf inside its group. A subgroup earns one only when that page has
something to say beyond listing its children: `Versioned migrations`,
`Direct schema changes` and `Sources` open with one, and the other thirteen
subgroups do not. `Atlas compatibility` and `Databases` carry theirs as a
top-level leaf for the same reason.

The shape, measured by importing `src/sidebar.mjs` rather than by counting the
block above: 7 top-level groups, 16 subgroups, 75 leaf entries, maximum depth 2,
and one collapsed subgroup (`Atlas compatibility > Differences and evidence`).
The largest sibling list anywhere is 8, reached twice — `Workflows > Versioned
migrations` and `Schema > Sources` — so the rule that sent the sidebar to two
levels holds everywhere in the result, not only in the four groups that broke it.

Three of those properties are reading rules that no checker holds, and they are
written here so a later change is judged against them rather than against
whichever gate happens to stay green:

- **The two-level cap.** Starlight's `items` schema is a lazy union that
  includes itself, so a third level renders and passes every check in
  `docs/site/scripts/`.
- **The seven-or-eight sibling rule.** Nothing counts siblings.
- **A duplicated entry.** `check-page-health.mjs` reads the sidebar in both
  directions, so it catches a page named by no entry and an entry naming no
  page, but a page named twice satisfies both directions. The tell is in the
  numbers it prints: entries should run exactly one below pages, the one being
  `index.mdx`, which renders no sidebar. `76 pages, 75 sidebar entries` is
  correct; `76 pages, 76 sidebar entries` is a duplicate.

Rationale by group (each maps to the top-level question a reader faces, in
order):

1. **Start** — "What is this and how do I try it?" Install, a runnable quick
   start, the one decision that shapes everything else (versioned vs direct vs
   hybrid), and the brownfield entry.
2. **Workflows** — "How do I change the database?" The two peer subgroups
   `Versioned migrations` and `Direct schema changes` mirror the
   choose-a-workflow decision, each opening with its own overview. The
   versioned subgroup decomposes along the lifecycle (generate → apply →
   rollback → integrity → maintain → import). `Test and CI`, `Load data` and
   `Distribute and operate` follow, because they are what a reader does once
   either route reaches the server; reference data and seed data sit together
   because they are the declarative and imperative halves of one question.
3. **Schema** — "How do I tell Ptah what the schema should be, and what can I
   get back out of it?" `Sources` holds the peer formats feeding one pipeline,
   `Analysis and documentation` holds what Ptah reports about a schema, and
   `Contract exports` holds the two exports another system consumes.
4. **Databases** — "What about my engine?" Support matrix plus per-engine
   guides where behavior materially differs. It stays flat until a fourth
   guide takes it past the sibling rule.
5. **Integrations** — "How do I drive Ptah from something that is not the
   CLI?" The Go API and the agent surfaces, discoverable without sitting on
   the path a new reader walks.
6. **Atlas compatibility** — "I'm coming from Atlas / need parity evidence."
   The overview and the adoption path are leaves; the command spellings and
   the project config are one subgroup; the evidence is a second, collapsed,
   because it belongs outside the learning path.
7. **Concepts and reference** — "What exactly does X mean / accept?" Last,
   because they serve returning readers, and split by what is being looked up:
   a concept, a command, a file format, or a rule.

Page count grew from 37 at the audited commit to 76 routes the site publishes
today, 75 of them named by a sidebar entry and one of them the splash page. The
growth is splits plus the verified missing content in section 9. Tiering keeps
scope honest: SQLite and SQL Server launched as compact engine pages because
their backing material justified it, while MySQL/MariaDB launched as a
`databases/support-matrix` section and splits out when content justifies it.

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
  not copied. The peer group is native work, not a compatibility surface:
  measured on the binary built from this tree, `ptah schema apply` is a
  registered native subcommand and its help reads "Apply a desired schema
  directly to a database", alongside native `compare`, `drift`, `plan`,
  `approve` and `inspect`. It is named "Direct schema changes" rather than
  "Declarative schema changes" because that is what separates it from the
  versioned group: `ptah migrations generate` reads the same desired schema and
  records the difference as files instead of running it, so "declarative" names
  where a change came from rather than how it lands. Section 7 carries the rule
  and `direct/overview` states it for the reader.
- Atlas's page-per-command reference is rejected; Ptah keeps two consolidated
  command reference pages (native, Atlas-compatible) with per-command anchors.
- No Atlas prose, examples, diagrams, assets, taxonomy, or page sequence is
  copied; the structure above is derived from Ptah's own command tree and
  reader journeys.

## 12. Corrections to the pre-implementation design

The design proposal attached to #804 was drafted against `master` at
`3fdc3c8`. This audit re-verified it at `f5c59b5` and corrects the record:

- **#805 landed in between.** The Atlas-compatible `migrate test` and
  `schema test` are now working forwards to the native test
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
  frontmatter, no TODO-style markers, and a sidebar entry in
  `src/sidebar.mjs` — a `slug:` or an internal `link:`, either one counting as
  coverage. The same gate reads the other direction, so an entry naming no page
  and a link naming no route are findings too. Every added or moved page must
  enter the sidebar in the same PR; orphan pages are impossible.
- `check-route-retirement.mjs`: every route in
  `scripts/data/published-routes.json` is either a live page or the source of a
  redirect. Retiring a URL without a redirect is a finding, and so is deleting
  the redirect later; a new page joins the ledger through `--write` in the same
  PR.
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
- `check-style.mjs` reaches further than the rest: site pages, `docs/*.md`
  outside `docs/site/`, `examples/**`, `integration/*.md` and the repository
  READMEs, 138 files in all. Measured by planting a British spelling in each
  and reading the exit code: the root `README.md`, `docs/roadmap-post-ga.md`
  and `docs/conformance.md` are governed; `docs/STYLE_GUIDE.md` is exempt by
  name because it is the rule source, and this inventory is outside the walk
  because the walk skips `docs/site/`.
- Section 7's canonical names are a reading rule, not a gate. `check-style.mjs`
  matches words, not phrases, so nothing catches a retired synonym: measured by
  restoring `### Declarative schema changes` to `index.mdx` and running
  `check:style`, `check:links`, `check:page-health`, `check:route-retirement`,
  `check:redirects` and `check:core-doc-links` — all six exit 0. A phrase rule
  is addable, and is deferred rather than rejected: added today it would fail
  on the root `README.md` opening sentence, its workflow table row, and the
  verbatim quote of that sentence in `docs/roadmap-post-ga.md`, all of which
  belong to the README rewrite that owns the file. It becomes a zero-finding
  addition once that rewrite carries the rename.
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
