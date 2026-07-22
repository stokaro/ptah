---
title: Atlas docs coverage
description: Current Atlas documentation crosswalk for Ptah compatibility, implementation, conformance, and follow-up work.
---

This page maps current Atlas documentation areas to Ptah documentation,
implementation status, conformance coverage, and follow-up issues. It is a
documentation coverage matrix, not a full parity claim.

Research date: July 22, 2026.

Official Atlas sources reviewed:

- [Atlas documentation home](https://atlasgo.io/docs)
- [Feature compatibility](https://atlasgo.io/features)
- [CLI reference](https://atlasgo.io/cli-reference)
- [Schema inspection](https://atlasgo.io/inspect)
- [Declarative schema apply](https://atlasgo.io/declarative/apply)
- [Declarative schema diff](https://atlasgo.io/declarative/diff)
- [Versioned migrations introduction](https://atlasgo.io/versioned/intro)
- [Versioned migration apply](https://atlasgo.io/versioned/apply)
- [Versioned migration lint](https://atlasgo.io/versioned/lint)
- [Down migrations](https://atlasgo.io/versioned/down)
- [Import existing databases or migrations](https://atlasgo.io/versioned/import)
- [Pre-execution checks](https://atlasgo.io/versioned/checks)
- [Migration directory checkpoints](https://atlasgo.io/versioned/checkpoint)
- [Pre-apply drift detection](https://atlasgo.io/versioned/drift-detection)
- [Atlas HCL syntax](https://atlasgo.io/atlas-schema/hcl)
- [Atlas project configuration](https://atlasgo.io/atlas-schema/projects)
- [Dev database](https://atlasgo.io/concepts/dev-database)
- [Atlas Registry](https://atlasgo.io/cloud/features/registry)
- [Atlas Cloud deployment reporting](https://atlasgo.io/cloud/deployment)
- [Schema testing](https://atlasgo.io/testing/schema)
- [Migration testing](https://atlasgo.io/testing/migrate)
- [Migration plan testing](https://atlasgo.io/testing/plan)

Availability classifications below are based on those official pages,
especially the Atlas [feature compatibility](https://atlasgo.io/features) page
when it separates Open, Pro, and Cloud behavior.

## Status Terms

| Status | Meaning |
| --- | --- |
| Documented | Ptah docs explain the supported behavior and link to exact reference material. |
| Partial | Ptah implements or documents part of the Atlas area, but gaps remain. |
| Gap | The area needs implementation, conformance, or documentation work before parity can be claimed. |
| Out of scope | The area is Atlas Pro, Cloud, registry, account, UI, or commercial behavior rather than an Atlas OSS drop-in target. |
| Measured | `ptah-atlas-conformance` has probes for this area. |
| Unmeasured | The behavior may exist, but current conformance reports do not prove it. |

## Coverage Matrix

| Atlas docs area | Atlas availability | Ptah documentation | Ptah implementation status | Conformance status | Follow-up |
| --- | --- | --- | --- | --- | --- |
| Top-level docs structure and getting started | Open docs | [Start](../../getting-started/), [Documentation map](../../documentation-map/) | Documented for Ptah workflows. Not a one-to-one Atlas docs clone. | Unmeasured; docs structure is not a runtime behavior. | [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) for full docs revision. |
| Installation and CLI entry points | Open docs | [Install Ptah](../../install/), [Commands](../commands/), [Atlas-compatible CLI](../../workflows/atlas-cli/) | Documented. `ptah atlas ...` is the Atlas-compatible namespace inside the full Ptah CLI. | Partially measured by command-resolution probes. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) for remaining CLI semantics. |
| CLI command and flag reference | Open for OSS commands; Pro/Cloud commands excluded from OSS target | [Commands](../commands/), [Atlas-compatible CLI](../../workflows/atlas-cli/), [Exit codes](../exit-codes/) | Partial. Core command paths are documented, but full Atlas flag semantics are still being audited and implemented. | Measured for selected command paths and flags only. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Schema inspection | Open | [Commands](../commands/), [Capabilities](../capabilities/), [Comparison](../comparison/) | Partial. `ptah db read` remains the native Ptah schema-read path. `ptah atlas schema inspect` now emits Atlas-shaped output without Ptah status banners: HCL by default, SQL with `--format sql` or `--format '{{ sql . }}'`, JSON with `--format json` or `--format '{{ json . }}'`, custom templates using the supported inspect helpers, basic HCL/SQL split-write file exports, and OSS `--exclude` resource filters including the Atlas-documented `*[type=extension].version` field selector. Other field-level exclude selectors, file-backed inspection, advanced split/write configuration, and dev-database inference remain tracked gaps. `--include` is Atlas Pro-only and outside Ptah's OSS drop-in target. | Partially measured by live SQLite HCL/SQL/JSON/custom-template/split-write/exclude/compat probes and CLI flag probes. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Declarative schema apply | Open | [Comparison](../comparison/), [Atlas-compatible CLI](../../workflows/atlas-cli/) | Partial. `ptah atlas schema apply` reads a live database, diffs it against local `file://` `.hcl`, `.yaml`, `.yml`, or `.sql` desired schema files, can take defaults from evaluated local `atlas.hcl` env expressions including `env.url`, `env.src`, `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff` policy, prints planned SQL, supports `--dry-run`, applies after interactive confirmation or explicit `--auto-approve`, supports Atlas transaction modes `file`, `all`, and `none` for the generated plan, supports `--exclude` and disabled `schema.mode` resource filters for the local-file comparison, and can use PostgreSQL concurrent index creation when `--tx-mode none` is set. Database desired-state URLs, migration directories, `env://`, include filters, Atlas dev-database simulation, and lock flags remain incomplete. | Partially measured with local schema files and live SQLite apply/no-op/dry-run/transaction-mode/exclude/config-driven format/schema-mode coverage. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Declarative schema diff | Open | [Comparison](../comparison/), [Atlas-compatible CLI](../../workflows/atlas-cli/) | Partial. `ptah schema compare` covers Ptah's native Go/live-DB comparison path. `ptah atlas schema diff` supports local `file://` schema-file diffs for `.hcl`, `.yaml`, `.yml`, and `.sql` sources, Atlas-style SQL/custom output formatting with `--format`, `sql`, and `.MarshalSQL`, `--exclude` and disabled `schema.mode` resource filters over the local inputs, and evaluated `atlas.hcl` defaults for `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.diff`, and supported `diff` policy. Database URLs, migration directories, `env://`, dev-database simulation, export, web output, and include filters remain incomplete. | Partially measured with local schema-file default, custom-template, no-op-template, invalid-template, exclude, and config-driven skip-drop probes. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Desired-state sources | Open sources include SQL, HCL, and external schema integrations; some data sources are Pro | [Schema files](../../workflows/schema-files/), [HCL schema example](../../examples/atlas-hcl/) | Partial. Ptah supports YAML, Go annotations, supported HCL schema files, SQL schema files, live DB introspection, and SQL parsing/rendering paths. Atlas external schemas and registry-backed sources are not full parity today. | Partially measured for imported Atlas fixtures. | [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511). |
| Atlas HCL schema syntax | Open for core HCL schema; advanced objects and product-gated areas vary by feature matrix | [Schema files](../../workflows/schema-files/), [HCL schema example](../../examples/atlas-hcl/), root [HCL schema reference](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md) | Partial. Ptah parses a strict supported subset and fails explicitly for unsupported constructs. Current support includes core tables, columns, indexes, constraints, enums, schemas, selected generated/identity forms, and recently added PostgreSQL include columns. | Measured for current imported fixtures; not complete Atlas HCL coverage. | [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511). |
| Atlas project config (`atlas.hcl`) | Open for local env config; Cloud/registry constructs are out of scope | [Configuration](../configuration/), root [Atlas project config reference](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md) | Partial. Ptah reads a documented subset into project config IR, including local env settings, `schema.src`, `schema.mode`, `format.schema.inspect/apply/diff`, `format.migrate.apply/diff`, supported `diff.skip.drop_table` and `diff.concurrent_index.create` policy, local variable defaults, locals, `getenv`, `file`, `fileset`, `format`, `jsonencode`, `data.hcl_schema.<name>.url`, and migration-lint changeset selectors such as `lint.latest` and `lint.git`, and rejects unsupported constructs. Cloud, registry, variable override flags, data sources beyond the local subset, lint policy engines, unsupported format blocks, unsupported diff policy fields, and remote directory behavior are not implemented. | Partially measured with parser, direct command, and live SQLite command tests for the supported local subset. | [`stokaro/ptah#583`](https://github.com/stokaro/ptah/issues/583), [`stokaro/ptah#581`](https://github.com/stokaro/ptah/issues/581). |
| Dev database | Core concept for Atlas diff/apply/lint planning; Docker/dev blocks include Pro-only baseline forms in current Atlas docs | [Configuration](../configuration/), [Comparison](../comparison/) | Partial. Ptah has shadow/dev database concepts for migration generation and project config IR. `ptah atlas migrate validate --dev-url` and `ptah atlas migrate lint --dev-url` clean and replay directly connectable dev databases; Atlas-style `--dev-url` behavior remains incomplete for several other commands and Docker dev databases. | Partially measured for migrate validate, migrate lint, and selected migrate diff/schema paths. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Versioned migrations overview | Open | [Migrations](../../workflows/migrations/), [Atlas migrations example](../../examples/atlas-migrations/), [Comparison](../comparison/) | Documented for Ptah native workflow and Atlas-compatible command names. Runtime parity still depends on command-specific rows below. | Partially measured. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Migration apply | Open | [Migrations](../../workflows/migrations/), [Atlas-compatible CLI](../../workflows/atlas-cli/), [Configuration](../configuration/) | Partial. `ptah migrations up` remains the native Ptah path. `ptah atlas migrate apply` executes Atlas-format migration directories with Atlas revision-table metadata by default, reads `env.url`, `migration`, and `format.migrate.apply` from `atlas.hcl`, and supports positional `amount`, `--to-version`, `--baseline`, `--allow-dirty`, `--tx-mode`, `--exec-order`, `--revisions-schema`, `--lock-timeout`, `--lock-name`, `--dry-run`, and Go-template `--format` output over a Ptah apply result that mirrors Atlas's public apply-template fields. `--lock-name` changes the session-level advisory lock name for databases that support migration locks. | Measured for selected migration-directory and live SQLite amount, target-version, baseline, dry-run baseline, lock-name acceptance, JSON format, custom template, config-driven format, dry-run format, no-op format, invalid-template preflight, redacted URL, and failed-apply format cases. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Migration down / rollback | Open | [Migrations](../../workflows/migrations/), [Atlas-compatible CLI](../../workflows/atlas-cli/), [Comparison](../comparison/) | Partial. Ptah rolls back through pre-planned down files. Atlas dynamic down planning, `--dev-url`, `--to-tag`, `--skip-checks`, `--plan`, and Go-template output formatting remain explicit gaps. | Partially measured. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Migration diff generation | Open | [Migrations](../../workflows/migrations/), [Atlas-compatible CLI](../../workflows/atlas-cli/), [Comparison](../comparison/) | Partial. Native Ptah can generate migrations from schema differences. `ptah atlas migrate diff` now validates an existing `atlas.sum`, replays a local Atlas migration directory on a directly connectable dev database, compares it to local schema files, writes an Atlas single-file migration, updates `atlas.sum`, reads `env.schema.src`, `env.dev`, `migration.dir`, `format.migrate.diff`, and supported non-concurrent `diff` policy from `atlas.hcl`, supports `--lock-timeout` for Ptah's local migration-directory lock, supports Atlas-style `--format` templates with `sql` and `.MarshalSQL` for the generated migration SQL, and supports `--schema` scoping for local desired schema files plus the replayed dev database state. Database desired-state URLs, `env://`, Docker dev databases, and concurrent index migration-file metadata remain incomplete. | Partially measured with local SQLite dev DB, local schema-file, schema-filter, custom-format, config-driven format/env defaults, invalid-format, and lock-timeout coverage. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Migration linting | Mixed in current Atlas docs: feature page lists migration linting CLI as Pro while also listing a basic Open lint-rule set | [CI](../../workflows/ci/), [Migrations](../../workflows/migrations/), [Comparison](../comparison/) | Partial. Ptah ships native linting, SARIF, inline suppression, severity config, and `ptah atlas migrate lint`; `--latest`, `--git-base`, `--git-dir`, and matching `atlas.hcl` defaults select the linted changeset; `--dev-url` infers lint dialect and treats directly connectable dev databases as scratch databases by cleaning and replaying migrations. Docker dev databases, web reports, and Atlas Go-template lint output remain gaps. | Partially measured with static lint, Git changeset selection, config-driven latest selection, and live SQLite dev-database replay coverage. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Migration directory integrity, hash, and validation | Open versioned workflow concept | [Migrations](../../workflows/migrations/), [Atlas migrations example](../../examples/atlas-migrations/), [Exit codes](../exit-codes/) | Documented. Ptah supports `ptah.sum`, Atlas-compatible `atlas.sum`, hash, validate, and `migrate validate --dev-url` SQL replay paths. Remaining parity depends on exact Atlas edge cases. | Measured for selected directory fixtures and live SQLite dev-database replay. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Migration import | Open for local migration-directory formats | [Atlas-compatible CLI](../../workflows/atlas-cli/), [Comparison](../comparison/) | Partial. Ptah imports local `file://` directories into an Atlas single-file directory and writes `atlas.sum`; Flyway repeatable migrations fail explicitly until Ptah can execute Atlas R-suffixed migrations. | Partially measured. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510). |
| Manual migrations and troubleshooting | Open docs | [Migrations](../../workflows/migrations/), [Troubleshooting](../../operate/troubleshooting/), [Exit codes](../exit-codes/) | Documented for Ptah-native behavior. Atlas-specific troubleshooting strings and repair flows are not fully mirrored. | Partially measured. | [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) for docs polish; [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) for runtime semantics. |
| Drift detection | Feature page lists drift detection as Pro | [CI](../../workflows/ci/), [Comparison](../comparison/) | Ptah has native `ptah schema drift`; Atlas Cloud/Pro drift monitoring is out of scope. | Ptah-native behavior is tested in repo; Atlas Cloud parity is not a target. | None for Cloud parity; [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) for docs depth. |
| Checkpoints | Feature page lists checkpoints as Pro | [Comparison](../comparison/), [Conformance](../../operate/conformance/) | Out of scope for Atlas OSS drop-in unless an Open workflow is identified later. | Not measured. | No Ptah implementation issue unless the OSS target changes. |
| Pre-migration checks and policy workflows | Feature page lists pre-migration checks as Pro | [CI](../../workflows/ci/), [Comparison](../comparison/) | Out of scope for Atlas Pro policy parity. Ptah has native lint/safety gates, not Atlas Pro checks. | Not measured as Atlas Pro behavior. | No Atlas OSS issue unless an Open check surface is identified. |
| Testing framework | Feature page lists testing framework as Pro | [Comparison](../comparison/), [Conformance](../../operate/conformance/) | Out of scope for Atlas OSS drop-in. Ptah has its own tests and conformance repo, not Atlas's Pro schema, migrate, or plan testing framework. | Not measured as Atlas Pro behavior. | None for OSS parity. |
| Supported databases | Open for PostgreSQL, MySQL, MariaDB, SQLite, TiDB, LibSQL in current Atlas feature matrix; many other drivers are Pro | [Capabilities](../capabilities/), [Comparison](../comparison/) | Partial but intentionally not identical. Ptah supports PostgreSQL, SQLite, MySQL/MariaDB, SQL Server subsets, and capability-gated PostgreSQL-compatible or specialty targets. Object-level support varies by dialect. | Partially measured by local, live, and conformance tests. | [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) for fuller object-level docs; implementation gaps should be filed from concrete findings. |
| Database object kinds | Core object kinds open for common drivers; advanced PostgreSQL objects such as partitions, views, functions, sequences, extensions, and RLS are listed as Pro examples in Atlas docs | [Capabilities](../capabilities/), [Schema files](../../workflows/schema-files/), root [HCL schema reference](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md) | Partial and not product-identical. Ptah supports some objects Atlas lists as Pro-gated, but HCL/config parity is still a subset until audited. | Partially measured. | [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511). |
| Atlas Registry | Cloud | [License boundary](../../operate/license-boundary/), [Comparison](../comparison/) | Out of scope. Ptah has no Atlas Cloud dependency and no registry implementation. | Not measured. | None for OSS parity. |
| Atlas Cloud deployment reporting | Cloud | [License boundary](../../operate/license-boundary/), [Comparison](../comparison/) | Out of scope. Ptah can be used in CI, but it does not report deployments to Atlas Cloud. | Not measured. | None for OSS parity. |
| Cloud-only workflows and account commands | Cloud/Pro | [License boundary](../../operate/license-boundary/), [Comparison](../comparison/) | Out of scope. Login, registry, UI, promotion, monitoring, and Cloud APIs are not Atlas OSS drop-in targets. | Not measured. | None for OSS parity. |
| CI integrations | Mixed: local CLI usage is open; Atlas Cloud deployment and lint reporting can require Pro/Cloud | [CI](../../workflows/ci/), [Conformance](../../operate/conformance/) | Documented for Ptah-native CI and conformance interpretation. Atlas's official integrations are not cloned one by one. | Ptah CI is measured by repository workflows; Atlas integration parity is unmeasured. | [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498). |
| Conformance evidence | Atlas docs do not define Ptah conformance; this is Ptah-owned evidence | [Conformance](../../operate/conformance/), [Comparison](../comparison/) | Documented. Regression budget and full-conformance gates are intentionally separate. | Measured in `ptah-atlas-conformance`, with current limits documented there. | [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167). |
| License and implementation boundary | Atlas source is a separate upstream project; Ptah compatibility must stay license-clean | [License boundary](../../operate/license-boundary/), [Comparison](../comparison/) | Documented. Ptah does not import, vendor, port, or derive implementation code from Atlas. Public interfaces and separately held test assets are compatibility inputs. | Not a runtime conformance area. | Keep this page updated when conformance assets change. |

## Follow-Up Issue Coverage

The fresh docs pass did not expose a product or conformance gap that lacks a
tracking issue. Current follow-up coverage is:

| Gap family | Tracking issue |
| --- | --- |
| Atlas command runtime and flag semantics | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| HCL schema and Atlas project config parity | [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511) |
| Full Ptah documentation revision | [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) |
| Live and differential conformance breadth | [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167) |

When a future Atlas docs audit finds a concrete unsupported OSS behavior not
covered by those issues, file a focused implementation or conformance issue
before claiming the area as covered.

## How To Use This Matrix

Use this page before changing Atlas-compatible behavior or documentation:

1. Find the Atlas docs area and official source link.
2. Check whether Ptah behavior is documented, partial, a gap, or out of scope.
3. If the row is partial or a gap, update the linked issue or create a focused
   follow-up issue before claiming support.
4. Update conformance only when the behavior can be measured by command,
   fixture, live database, or Atlas CE differential probes.

Do not turn a green docs build into a product parity claim. Product parity needs
current implementation evidence, conformance evidence, and a closed gap row.
