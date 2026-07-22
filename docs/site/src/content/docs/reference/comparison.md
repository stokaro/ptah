---
title: Comparison
description: Ptah native commands, Atlas-compatible commands, feature parity, config precedence, and safety behavior.
---

## Product positioning

Ptah is an independent MIT-licensed implementation. It does not use Atlas source
code; see [License boundary](../../operate/license-boundary/) for the repository
and test-asset boundary.

Atlas has both open and commercial/cloud feature sets. The current Atlas
[feature availability](https://atlasgo.io/features) page lists database
inspection, schema diffing, versioned migrations, and declarative migrations as
open CLI features. The same page lists the migration linting CLI feature as Pro
while also listing a basic Open lint-rule set. Checkpoints, visualization,
interactive migrations, testing, deployment rollout, database security as code,
and declarative data management are listed as Pro features.

## Command parity

| Task | Native Ptah | `ptah atlas` | Atlas OSS |
| --- | --- | --- | --- |
| Apply migrations | `ptah migrations up` | `ptah atlas migrate apply` | `atlas migrate apply` |
| Roll back migrations | `ptah migrations down` | `ptah atlas migrate down` | `atlas migrate down` |
| Migration status | `ptah migrations status` | `ptah atlas migrate status` | `atlas migrate status` |
| Hash migrations | `ptah migrations hash` | `ptah atlas migrate hash` | `atlas migrate hash` |
| Validate migrations | `ptah migrations validate` | `ptah atlas migrate validate` | `atlas migrate validate` |
| Lint migrations | `ptah migrations lint` | `ptah atlas migrate lint` | Current Atlas docs list the migration linting CLI feature as Pro and a basic lint-rule set as Open. |
| Create an empty migration | `ptah migrations create` | `ptah atlas migrate new` | `atlas migrate new` |
| Repair revision state | `ptah migrations repair` | `ptah atlas migrate set` | `atlas migrate set` |
| Inspect schema | `ptah db read` | `ptah atlas schema inspect` | `atlas schema inspect` |
| Diff schema | `ptah schema compare` | `ptah atlas schema diff` | `atlas schema diff` |
| Format schema files | Atlas-compatible command only | `ptah atlas schema fmt` | `atlas schema fmt` |
| Clean schema objects | `ptah db drop-all` | `ptah atlas schema clean` | `atlas schema clean` |

Some Atlas command paths are intentionally registered before complete runtime
behavior exists, and some accepted Atlas flags still fail explicitly rather than
being silently ignored. The gap register below links that work to concrete
tracking issues.

The separate `ptah-compat` binary exposes the Atlas-compatible command tree at
process root for drop-in script migration. It is not repeated in this matrix
because its command semantics are the same Atlas-compatible surface shown in the
`ptah atlas` column.

For a page-by-page crosswalk against the official Atlas documentation, see
[Atlas docs coverage](../atlas-docs-coverage/).

## Detailed product comparison

| Area | Ptah | Atlas OSS | Atlas Commercial / Cloud | Evidence |
| --- | --- | --- | --- | --- |
| License and implementation | MIT-licensed independent implementation. Ptah compatibility code is written in this repository and does not import or vendor Atlas source. | Atlas is an independent upstream product. Ptah treats its public command names, flags, file formats, and observable behavior as compatibility inputs. | Same Atlas product family plus licensed Pro and Cloud capabilities. | [License boundary](../../operate/license-boundary/), [Atlas feature availability](https://atlasgo.io/features) |
| Command compatibility | Native command tree plus Atlas-compatible paths under `ptah atlas <command> ...`. Some paths and flags are still tracked gaps. | Open CLI feature surface includes inspection, schema diffing, versioned migrations, and declarative migrations. | Pro and Cloud add capabilities that are not OSS drop-in targets, such as checkpoints, rollout, testing, and registry-backed workflows. | [Atlas CLI reference](https://atlasgo.io/cli-reference), [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#514`](https://github.com/stokaro/ptah/issues/514) |
| Schema inspection | `ptah db read` remains Ptah's native schema-read command. `ptah atlas schema inspect` inspects supported live databases and emits Atlas-shaped output without Ptah status banners: HCL by default, SQL with `--format sql` or `--format '{{ sql . }}'`, JSON with `--format json` or `--format '{{ json . }}'`, and custom Go-template output using `.MarshalHCL`, `sql`, `json`, `base64url`, and `mermaid`. Split/write templates, include/exclude filters, file-backed inspection, and dev-database inference remain gaps. | `atlas schema inspect` is documented as an open CLI feature for inspecting a database schema with HCL, SQL, JSON, and template output forms. | Commercial database drivers broaden the set of inspectable engines. | [Atlas CLI reference](https://atlasgo.io/cli-reference), [Capabilities](../capabilities/), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Schema diff, apply, formatting, and cleanup | `ptah schema compare` covers Ptah's Go-annotations-to-live-DB comparison path. `ptah atlas schema apply` now diffs a live database against local `file://` `.hcl`, `.yaml`, `.yml`, or `.sql` desired schema files, reads `env.url`, `env.src`, and `env.dev` from `atlas.hcl` when `--env` is passed, prints planned SQL, supports Atlas-style `--format` templates over planned changes, and applies after interactive confirmation or explicit `--auto-approve`; `--dry-run` prints without applying, and `--tx-mode` supports `file`, `all`, and `none` for the generated plan. `ptah atlas schema diff` implements local schema-file diffs, prints migration SQL, and supports Atlas-style `--format` templates with `sql` and `.MarshalSQL`. For both declarative paths, database desired-state URLs, migration directories, `env://` URL sources, include/exclude filters, Atlas dev-database simulation, web output, and lock flags remain gaps. `ptah atlas schema fmt` formats local `.hcl` files with HCL canonical layout. `ptah atlas schema clean` forwards to the destructive native `ptah db drop-all` cleanup path; `--auto-approve` is supported only as an explicit confirmation bypass, not through environment defaults. | Atlas OSS documents schema diffing, declarative migrations, HCL formatting, and schema cleanup as open CLI features. | Cloud/Pro workflows add registry-backed plans, approvals, and deployment tracking. | [Atlas feature availability](https://atlasgo.io/features), [pre-planning schema migrations](https://atlasgo.io/declarative/plan), [Atlas CLI reference](https://atlasgo.io/cli-reference), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Versioned migrations | `ptah migrations up`, `down`, `status`, `hash`, `validate`, `create`, `repair`, and Atlas-compatible counterparts cover local migration workflows. `ptah atlas migrate apply` executes Atlas-format migration directories with Atlas revision-table metadata by default and supports positional `amount`, `--to-version`, `--baseline`, `--allow-dirty`, `--tx-mode`, `--exec-order`, `--revisions-schema`, `--lock-timeout`, `--lock-name`, `--dry-run`, and Go-template `--format` output over a Ptah apply result that mirrors Atlas's public apply-template fields; `--lock-name` changes the session-level advisory lock name for databases that support migration locks. `ptah atlas migrate validate` verifies `ptah.sum` or `atlas.sum` and, with `--dev-url`, cleans the dev database and replays the migration directory to validate SQL execution. `ptah atlas migrate diff` validates an existing `atlas.sum`, replays a local Atlas migration directory on a directly connectable dev database, diffs it against local schema files, writes an Atlas single-file migration, updates `atlas.sum`, supports `--lock-timeout` for Ptah's local migration-directory lock, supports Atlas-style `--format` templates with `sql` and `.MarshalSQL` for generated migration SQL, and uses `--schema` to scope local desired schema files plus the replayed dev database state; database desired-state URLs, `env://`, and Docker dev databases remain gaps. `ptah atlas migrate down` forwards to Ptah's pre-planned down-file rollback path, maps Atlas-compatible flags whose behavior matches native Ptah behavior, and fails explicitly for Atlas dynamic down-planning and output-format behavior that is not implemented yet. `ptah atlas migrate import` imports local `file://` directories from Atlas-supported formats into a separate Atlas single-file directory and writes `atlas.sum`, but rejects Flyway repeatable migrations until Ptah can execute Atlas R-suffixed imported migrations. | Atlas OSS includes versioned migrations and documents `atlas migrate apply`, `atlas migrate diff`, `atlas migrate down`, and `atlas migrate import` for applying, generating, reverting, and importing local migration directories. | Atlas Registry and deployment reporting add remote migration-directory storage, tagging, history, and environment promotion workflows. Pro adds approval workflows for protected down plans. | [Atlas feature availability](https://atlasgo.io/features), [Atlas migration apply](https://atlasgo.io/versioned/apply), [Atlas down migrations](https://atlasgo.io/versioned/down), [Import from other migration tools](https://atlasgo.io/versioned/import), [Atlas Cloud deployment docs](https://atlasgo.io/cloud/deployment), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Migration linting | Ptah ships first-party migration linting and the `ptah atlas migrate lint` compatibility path. `--latest N` limits the run to the latest N migration versions; `--dev-url` infers the lint dialect and treats directly connectable dev databases as scratch databases by cleaning and replaying migrations to validate SQL execution. Docker dev databases, Atlas web reports, Git branch changeset detection, and Atlas Go-template lint output remain gaps. | Current Atlas docs mark the official migration linting CLI feature as Pro while the feature availability page also lists a basic Open lint-rule set. Atlas documents `--latest N` and dev-database simulation for changeset linting. | Pro migration linting includes Atlas analyzers, policy workflows, enforced checks, and browser reports. | [Atlas feature availability](https://atlasgo.io/features), [Atlas migration linting docs](https://atlasgo.io/versioned/lint), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Cloud and registry features | Ptah has no Atlas Cloud dependency and no Atlas Registry implementation. | Not part of the open drop-in target surface unless a workflow is explicitly available without Cloud credentials. | Atlas Cloud provides registry, deployment reporting, cloud CLI commands, UI, Pro seats, pipelines, and schema monitoring. | [Atlas Registry](https://atlasgo.io/cloud/features/registry), [Atlas Cloud deployment docs](https://atlasgo.io/cloud/deployment), [Atlas pricing](https://atlasgo.io/cloud/pricing) |
| Supported databases | Ptah has first-party support for PostgreSQL, SQLite, MySQL/MariaDB, SQL Server subsets, and capability-gated PostgreSQL-compatible or specialty targets. | Atlas docs list PostgreSQL, MySQL, MariaDB, SQLite, TiDB, and LibSQL as Open drivers. | Atlas Pro adds SQL Server, ClickHouse, Redshift, Oracle, Spanner, Snowflake, Databricks, CockroachDB, Azure HorizonDB, YugabyteDB, Aurora DSQL, Azure Fabric, and related drivers. | [Capabilities](../capabilities/), [Atlas feature availability](https://atlasgo.io/features) |
| HCL and config | Ptah parses strict HCL schema and Atlas project config subsets, including literal `env.src` schema file sources for `ptah atlas schema apply --env`. Unsupported constructs fail explicitly rather than being silently ignored. | Atlas OSS supports SQL, HCL schema, external schema, remote/template directories, and related data sources listed as Open. | Pro data sources include composite schema and blob directory features. | [HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md), [Atlas project config](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md), [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511) |
| Conformance status | Ptah uses the separate `ptah-atlas-conformance` repository as measured evidence against Atlas fixtures and behavior. The regression budget and full-conformance gates are intentionally separate: budget green means no unexpected regression, while full-conformance can remain red for known Atlas OSS gaps such as dynamic down planning. | Atlas fixtures and CLI behavior provide the comparison target for OSS-compatible behavior. | Commercial/cloud-only behavior is separated from the OSS drop-in target and tracked as documentation scope. | [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md), [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md), [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167) |

## Feature parity evidence

| Area | Ptah status | Evidence |
| --- | --- | --- |
| Offline Atlas fixture ingestion | The imported Atlas fixture corpus and CLI probes are tracked in the conformance repository. Treat a red full-conformance gate as product work, not as a broken regression gate; the regression budget records which known gaps are currently tolerated. | [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Live database round trips | Current live smoke corpus is green: 10 observations, 0 non-OK in the linked report. This is evidence for the covered scenarios, not proof of every Atlas OSS runtime path. | [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md) |
| Atlas CE differential checks | Current Atlas CE differential corpus is green: 5 observations, 0 non-OK in the linked report. This is evidence for the covered scenarios, not proof of every Atlas OSS schema object. | [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md) |
| HCL schema files | Strict supported subset. Unsupported constructs fail explicitly instead of being ignored. PostgreSQL `include` columns are preserved for indexes, primary keys, and unique constraints across HCL parse/render, SQL parse/render, schema diff, and database introspection paths. | [HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md) |
| Atlas project config | Strict supported subset. Literal `env.src` values can feed `ptah atlas schema apply --env`. Unsupported constructs fail explicitly instead of being ignored. | [Atlas project config](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md) |
| Native Go annotations | First-party Ptah workflow. | [Go schema workflow](../../workflows/go-schema/) |

## Gap register

| Gap | Type | Current boundary | Tracking |
| --- | --- | --- | --- |
| Atlas-compatible command runtime placeholders | Product behavior | No registered Atlas-compatible path in the current focused #510 set is left as a pure runtime placeholder. `ptah atlas version`, `ptah atlas license`, `ptah atlas schema fmt`, `ptah atlas schema diff`, `ptah atlas schema apply`, `ptah atlas migrate diff`, and `ptah atlas migrate import` now execute Ptah-owned behavior, with command-specific gaps tracked separately. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Atlas-compatible down semantics | Product behavior | `ptah atlas migrate down` is an Atlas OSS command path and recognizes the documented Atlas-style flag names. Flags mapped to native behavior include `--url`, `--dir`, `--to-version`, `--dry-run`, `--revisions-schema`, and `--lock-timeout`. Unsupported Atlas-only behavior currently fails explicitly, including dynamic planning or safety inputs such as `--dev-url`, `--to-tag`, `--skip-checks`, and `--plan`, plus Atlas Go template output formatting via `--format`. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Atlas-compatible Flyway repeatable import execution | Product behavior | `ptah atlas migrate import` rejects Flyway `R__...sql` repeatable migrations because Ptah currently treats Atlas R-suffixed migrations as non-executable repeatable files. Importing them successfully would let a later apply skip schema objects silently. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Atlas-compatible flag semantics | Product behavior | Accepted flags whose behavior is still incomplete include `schema inspect --dev-url` beyond dialect validation, `schema inspect --exclude`, `schema inspect --include`, `schema inspect --format` for split/write templates, `schema apply --schema`, `schema apply --exclude`, `schema apply --include`, `schema diff --schema`, `schema diff --exclude`, `schema diff --include`, `schema diff --web`, `migrate down --dev-url`, `migrate down --to-tag`, `migrate down --format`, `migrate down --skip-checks`, and `migrate down --plan`. `ptah atlas schema apply --to` and `ptah atlas schema diff --from/--to` now support local schema files only; database URLs, migration directories, and `env://` URL sources remain follow-up gaps. `ptah atlas schema apply --env` reads literal local `env.src` desired schema sources from `atlas.hcl`, but does not evaluate dynamic HCL data-source expressions yet. `ptah atlas schema inspect --format` now supports HCL, SQL, JSON, custom templates, and Mermaid helper output, but not Atlas split/write file templates. `ptah atlas schema diff --format` now supports Atlas-style SQL/custom output with `sql` and `.MarshalSQL` for local schema-file diffs. `ptah atlas schema apply --dev-url` validates dialect compatibility but does not run Atlas dev-database simulation yet. `ptah atlas schema apply --tx-mode` supports `file`, `all`, and `none` for generated local-schema plans. `ptah atlas migrate validate --dev-url` now validates SQL execution by cleaning the dev database and replaying the migration directory. `ptah atlas migrate lint --dev-url` now infers dialect and cleans and replays migrations on directly connectable dev databases; Docker dev databases, web reports, Git branch changeset detection, and Atlas Go-template lint output remain gaps. `ptah atlas migrate diff --to` now supports local schema files only; database desired-state URLs, `env://` sources, and Docker dev databases remain follow-up gaps; `--schema` scopes local desired schema files and the replayed dev database state; `--lock-timeout` bounds Ptah's local migration-directory lock for checksum/write safety, and `--format` renders generated migration SQL with `sql` and `.MarshalSQL`. `ptah atlas migrate apply` now supports positional `amount`, `--to-version`, `--baseline`, `--allow-dirty`, `--tx-mode`, `--exec-order`, `--revisions-schema`, `--lock-timeout`, `--lock-name`, `--dry-run`, and Go-template `--format` output against Atlas-format migration directories and Atlas revision metadata. `--tx-mode=all` is limited to dialects with transactional DDL support and conflicts with file-level no-transaction directives. This is not the full Atlas flag gap: unregistered Atlas flags also need a full audit and either implementation or explicit out-of-scope classification. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| HCL schema and Atlas project config subset audit | Product behavior and coverage | Current imported fixtures pass, and there are no concrete unsupported Atlas OSS schema/config constructs listed in the current conformance reports. Complete schema/config parity is not claimed until the remaining Atlas OSS surface is audited; newly discovered unsupported constructs should become focused implementation issues. | [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511) |
| Live and differential corpus breadth | Conformance coverage | The live and Atlas CE differential reports are green for the current smoke corpus only. More fixtures are needed before using those checks as broad Atlas OSS parity evidence. | [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167) |

A green docs build only proves the documentation site builds and internal links
resolve. It is not parity evidence. Use the conformance reports for measured
behavior and the gap register above for known product, coverage, and
documentation gaps.

## Config precedence

| Source | Wins over |
| --- | --- |
| CLI flags | Everything else |
| Environment variables | Config files and defaults |
| `atlas.hcl` environment | `ptah.yaml` and defaults |
| `ptah.yaml` environment | Defaults |
| Built-in defaults | Nothing |

## Safety and exit behavior

| Behavior | Ptah contract |
| --- | --- |
| Unknown or unsupported config | Fails instead of guessing. |
| Migration directory hash drift | `migrations validate` exits non-zero. |
| Pending migrations in status | `migrations status --exit-code` exits `1`. |
| Rollback | Requires explicit `--target`; use `--confirm` for non-interactive runs. |
| Destructive migration plans | Should be gated in CI; use the GitHub Action or explicit review. |

Reference: [Exit codes](../exit-codes/).
