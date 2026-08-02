---
title: Atlas docs coverage
description: Current Atlas documentation crosswalk for Ptah compatibility, implementation, conformance, and follow-up work.
---

This page maps current Atlas documentation areas to Ptah documentation,
implementation status, conformance coverage, and follow-up issues. It is a
documentation coverage matrix, not a full parity claim.

Research date: July 28, 2026.

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

## Status terms

| Status | Meaning |
| --- | --- |
| Documented | Ptah docs explain the supported behavior and link to exact reference material. |
| Partial | Ptah implements or documents part of the Atlas area, but gaps remain. |
| Gap | The area needs implementation, conformance, or documentation work before parity can be claimed. |
| Out of scope | The area is Atlas Pro, Cloud, registry, account, UI, or commercial behavior rather than an Atlas OSS drop-in target. |
| Measured | `ptah-atlas-conformance` has probes for this area. |
| Unmeasured | The behavior may exist, but current conformance reports do not prove it. |

## Coverage matrix

Each area below records how Atlas documents it, where Ptah documents it, the
implementation status, the conformance status, and the follow-up. This table is
the index; the sections carry the detail.

| Atlas docs area | Ptah status |
| --- | --- |
| [Top-level docs structure and getting started](#top-level-docs-structure-and-getting-started) | Documented |
| [Installation and CLI entry points](#installation-and-cli-entry-points) | Documented |
| [CLI command and flag reference](#cli-command-and-flag-reference) | Partial |
| [Schema inspection](#schema-inspection) | Partial |
| [Declarative schema apply](#declarative-schema-apply) | Partial |
| [Declarative schema diff](#declarative-schema-diff) | Partial |
| [Desired-state sources](#desired-state-sources) | Partial |
| [Atlas HCL schema syntax](#atlas-hcl-schema-syntax) | Partial |
| [Atlas project config (`atlas.hcl`)](#atlas-project-config-atlashcl) | Partial |
| [Dev database](#dev-database) | Partial |
| [Versioned migrations overview](#versioned-migrations-overview) | Documented |
| [Migration apply](#migration-apply) | Partial |
| [Migration down and rollback](#migration-down-and-rollback) | Partial |
| [Migration diff generation](#migration-diff-generation) | Partial |
| [Migration linting](#migration-linting) | Partial |
| [Migration directory integrity, hash, and validation](#migration-directory-integrity-hash-and-validation) | Documented |
| [Migration import](#migration-import) | Partial |
| [Manual migrations and troubleshooting](#manual-migrations-and-troubleshooting) | Documented |
| [Drift detection](#drift-detection) | Native |
| [Checkpoints](#checkpoints) | Native |
| [Pre-migration checks and policy workflows](#pre-migration-checks-and-policy-workflows) | Partial |
| [Testing framework](#testing-framework) | Native |
| [Declarative reference data](#declarative-reference-data) | Native |
| [Supported databases](#supported-databases) | Partial |
| [Database object kinds](#database-object-kinds) | Partial |
| [Atlas Registry](#atlas-registry) | Out of scope |
| [Atlas Cloud deployment reporting](#atlas-cloud-deployment-reporting) | Out of scope |
| [Cloud-only workflows and account commands](#cloud-only-workflows-and-account-commands) | Out of scope |
| [CI integrations](#ci-integrations) | Documented |
| [Conformance evidence](#conformance-evidence) | Documented |
| [License and implementation boundary](#license-and-implementation-boundary) | Documented |

### Top-level docs structure and getting started

**Atlas availability.** Open docs

**Ptah documentation.** [Quick start](../../start/quick-start/), [Choose a workflow](../../start/choose-a-workflow/)

**Implementation status.** Documented for Ptah workflows. Not a one-to-one Atlas docs clone.

**Conformance status.** Unmeasured; docs structure is not a runtime behavior.

**Follow-up.** [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) for full docs revision.


### Installation and CLI entry points

**Atlas availability.** Open docs

**Ptah documentation.** [Install Ptah](../../start/install/), [Native commands](../../reference/native-commands/), [Atlas compatibility overview](../overview/)

**Implementation status.** Documented. Atlas-compatible invocations run the separate `ptah-compat` drop-in binary.

**Conformance status.** Partially measured by command-resolution probes.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) for remaining CLI semantics.


### CLI command and flag reference

**Atlas availability.** Open for OSS commands; Pro/Cloud commands excluded from OSS target

**Ptah documentation.** [Native commands](../../reference/native-commands/), [Atlas-compatible commands](../../reference/atlas-commands/), [Exit codes](../../reference/exit-codes/)

**Implementation status.** Partial. Core command paths are documented, but full Atlas flag semantics are still being audited and implemented.

**Conformance status.** Measured for selected command paths and flags only.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510).


### Schema inspection

**Atlas availability.** Open

**Ptah documentation.** [Atlas-compatible commands](../../reference/atlas-commands/), [Capabilities](../../reference/capabilities/), [Comparison](../comparison/)

**Implementation status.** Partial. `ptah db read` remains the native Ptah schema-read path.

`ptah-compat schema inspect` now emits Atlas-shaped output without Ptah status banners: HCL by default, SQL with `--format sql` or `--format '{{ sql . }}'`, JSON with `--format json` or `--format '{{ json . }}'`, custom templates using the supported inspect helpers, HCL/SQL split-write file exports with the documented Atlas split strategies (per object by default, `split "schema"`, `split "type"`, optional file-extension argument), and OSS `--exclude` resource filters including the Atlas-documented `*[type=extension].version` field selector with schema-qualified globs.

Local schema files, migration directories, and `env://` references are inspected through required `--dev-url` dev-database evaluation (reset, materialize, introspect). Other field-level exclude selectors fail explicitly; exporter blocks remain tracked gaps.

`ptah-compat schema inspect --include` positively selects which top-level resources the output keeps, through the same selector engine as `schema apply` and `schema diff`: `--schema` names the schema universe, `--include` picks resources inside it, `--exclude` subtracts. The pinned Atlas CE binary does not register the flag on this command and rejects it as an unknown flag, so this is a Pro-surface spelling Ptah implements openly rather than a CE parity target.

**Conformance status.** Partially measured by live SQLite HCL/SQL/JSON/custom-template/split-write/exclude/compat probes and CLI flag probes.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#640`](https://github.com/stokaro/ptah/issues/640).


### Declarative schema apply

**Atlas availability.** Open

**Ptah documentation.** [Comparison](../comparison/), [Atlas schema commands](../schema-commands/)

**Implementation status.** Partial.

`ptah-compat schema apply` reads a live database, diffs it against local
`file://` `.hcl`, `.yaml`, `.yml`, or `.sql` desired schema files, prints
planned SQL, and applies after interactive confirmation or explicit
`--auto-approve`. It also:

- can take defaults from evaluated local `atlas.hcl` env expressions including `env.url`, `env.src`, `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff` policy
- supports `--dry-run`
- supports Atlas transaction modes `file`, `all`, and `none` for the generated plan
- supports `--exclude` and disabled `schema.mode` resource filters for the local-file comparison
- can use PostgreSQL concurrent index creation when `--tx-mode none` is set
- supports `--edit` to open the planned SQL in `$VISUAL`/`$EDITOR` before approval so the edited SQL is what gets applied

`--plan file://<path>` executes a pre-approved local plan file saved by `schema plan`, and `--lock-timeout` bounds waiting for the session advisory lock that serializes concurrent applies against one target, with an explicit unlocked-with-note decision on dialects without advisory locks.

`--to` also accepts one directly connectable database URL, one migration directory (`file://` directory containing `atlas.sum`) replayed on the required `--dev-url` dev database, or one `env://` reference resolved through the evaluated `atlas.hcl` env; unsupported schemes such as `atlas://` fail before the target database is contacted.

Before a non-dry-run apply, `--dev-url` rehearses the exact ordered plan on the reset dev database with the target's current schema recreated first; a failed rehearsal refuses the apply with the target unchanged.

`--schema` and `--include` positively scope both comparison sides with union semantics, exclusion subtraction, cross-scope dependency diagnostics, and synced output for empty selections.

**Conformance status.** Partially measured with local schema files and live SQLite apply/no-op/dry-run/transaction-mode/exclude/config-driven format/schema-mode coverage, plus CLI-surface flag probes.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#640`](https://github.com/stokaro/ptah/issues/640).


### Declarative schema diff

**Atlas availability.** Open

**Ptah documentation.** [Comparison](../comparison/), [Atlas schema commands](../schema-commands/)

**Implementation status.** Partial. `ptah schema compare` covers Ptah's native Go/live-DB comparison path.

`ptah-compat schema diff` supports local `file://` schema-file diffs for `.hcl`, `.yaml`, `.yml`, and `.sql` sources, Atlas-style SQL/custom output formatting with `--format`, `sql`, and `.MarshalSQL`, `--exclude` and disabled `schema.mode` resource filters over the local inputs, and evaluated `atlas.hcl` defaults for `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.diff`, and supported `diff` policy.

`--from` and `--to` also accept one directly connectable database URL, one migration directory replayed on the required `--dev-url` dev database, or one `env://` reference resolved through the evaluated `atlas.hcl` env, with the dialect pinned by `--dev-url` first and then by database URLs.

`--schema` and `--include` positively scope both diff sides with the same selection semantics as `schema apply`. Dev-database simulation and export remain incomplete. The pinned Atlas CE flag surface does not register `schema diff --web`, so Ptah rejects it as unknown.

**Conformance status.** Partially measured with local schema-file default, custom-template, no-op-template, invalid-template, exclude, config-driven skip-drop probes, and CLI-surface flag probes.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#640`](https://github.com/stokaro/ptah/issues/640).


### Desired-state sources

**Atlas availability.** The Atlas docs describe SQL, HCL, and external schema integrations, with some data sources Pro. Measured 2026-08-01: the pinned Atlas CE v1.2.0 binary rejects `data "external_schema"` with "not supported by the community version", so the external-schema data source is not an Open capability

**Ptah documentation.** [Composite desired schema](../../schema/composite/), [ORM and external loaders](../../schema/orm-and-external/), [OCI registry artifacts](../../operate/oci-registry/), [HCL schema](../../schema/hcl/)

**Implementation status.** Partial. Native Ptah supports YAML, Go annotations, supported HCL schema files, SQL schema files, live DB introspection, external programs that emit SQL, HCL, or YAML, and canonical desired-schema artifacts in a bring-your-own OCI registry. The same `ptah.yaml external_schema` block supplies native render, compare, drift, and migration planning.

The native OCI source is available to `schema compare` and `drift` through `--schema-file`; it is not Atlas Registry parity or an `atlas://` source for Atlas-compatible commands. Atlas HCL `data "external_schema"` is implemented for both binaries, gated behind `--allow-external-schema` (native) or `PTAH_ALLOW_EXTERNAL_SCHEMA=1` (`ptah-compat`); registry-backed sources and the remaining Atlas data sources stay outside the supported compatibility subset.

**Conformance status.** Native external programs are measured by a deterministic 20-observation SQL/HCL/YAML workflow through render, compare, drift, plan, generate, apply, live SQLite facts, and convergence. A separate zero-gap tier exercises pinned GORM and SQLAlchemy providers. The native OCI round trip remains covered by Ptah's own command and integration tests rather than Atlas conformance.

**Follow-up.** [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511), [`stokaro/ptah#664`](https://github.com/stokaro/ptah/issues/664), [`stokaro/ptah#669`](https://github.com/stokaro/ptah/issues/669).


### Atlas HCL schema syntax

**Atlas availability.** Open for core HCL schema; advanced objects and product-gated areas vary by feature matrix

**Ptah documentation.** [HCL schema](../../schema/hcl/), site [HCL schema reference](../../reference/hcl-schema/)

**Implementation status.** Partial. Ptah parses a strict supported subset and fails explicitly for unsupported constructs. Current support includes core tables, columns, indexes, constraints, enums, schemas, selected generated/identity forms, and recently added PostgreSQL include columns.

**Conformance status.** Measured for current imported fixtures; not complete Atlas HCL coverage.

**Follow-up.** [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511).


### Atlas project config (`atlas.hcl`)

**Atlas availability.** Open for local env config; Cloud/registry constructs are out of scope

**Ptah documentation.** [Configuration](../../reference/configuration/), site [Atlas project config reference](../project-config/)

**Implementation status.** Partial.

Ptah reads a documented subset into project config IR, including local env settings, `schema.src`, `schema.mode`, `format.schema.inspect/apply/diff`, `format.migrate.apply/diff/lint/status`, supported `diff.skip.drop_table` and `diff.concurrent_index.create` policy, supported migration-lint analyzer severity policy for `destructive`, `concurrent_index`, `data_depend`, `incompatible`, and `nestedtx`, local variable defaults, typed variables (`string`, `number`, `bool`, `list(string)`) with `sensitive` support, string/list variable overrides through repeated `--var name=value` with conversion to the declared type, locals, `getenv`, `file`, `fileset`, `format`, `jsonencode`, `data.hcl_schema.<name>.url`, and migration-lint changeset selectors such as `lint.latest` and `lint.git`, and rejects unsupported constructs.

Cloud, registry, data sources beyond the local subset, variable `validation` blocks, other variable type constraints such as `object(...)`, Atlas check-level lint policy, custom lint rules, unsupported lint analyzer options, unsupported format blocks, unsupported diff policy fields, and remote directory behavior are not implemented.

**Conformance status.** Partially measured with parser, direct command, compatibility-wrapper, and live SQLite command tests for the supported local subset.

**Follow-up.** [`stokaro/ptah#582`](https://github.com/stokaro/ptah/issues/582), [`stokaro/ptah#583`](https://github.com/stokaro/ptah/issues/583), [`stokaro/ptah#581`](https://github.com/stokaro/ptah/issues/581), [`stokaro/ptah#619`](https://github.com/stokaro/ptah/issues/619).


### Dev database

**Atlas availability.** Core concept for Atlas diff/apply/lint planning; Docker/dev blocks include Pro-only baseline forms in current Atlas docs

**Ptah documentation.** [Configuration](../../reference/configuration/), [Comparison](../comparison/)

**Implementation status.** Partial. Ptah has shadow/dev database concepts for migration generation and project config IR. `ptah-compat migrate validate --dev-url` and `ptah-compat migrate lint --dev-url` clean and replay directly connectable dev databases; Atlas-style `--dev-url` behavior remains incomplete for several other commands and Docker dev databases.

**Conformance status.** Partially measured for migrate validate, migrate lint, and selected migrate diff/schema paths.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510).


### Versioned migrations overview

**Atlas availability.** Open

**Ptah documentation.** [Versioned migrations](../../versioned/overview/), [Atlas migrate commands](../migrate-commands/), [Comparison](../comparison/)

**Implementation status.** Documented for Ptah native workflow and Atlas-compatible command names. Runtime parity still depends on command-specific rows below.

**Conformance status.** Partially measured.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510).


### Migration apply

**Atlas availability.** Open

**Ptah documentation.** [Apply migrations](../../versioned/apply/), [Atlas migrate commands](../migrate-commands/), [Configuration](../../reference/configuration/)

**Implementation status.** Partial. `ptah migrations up` remains the native Ptah path.

`ptah-compat migrate apply` executes Atlas-format migration directories with Atlas revision-table metadata by default, reads `env.url`, `migration`, and `format.migrate.apply` from `atlas.hcl`, and supports positional `amount`, `--baseline`, `--allow-dirty`, `--tx-mode`, `--exec-order`, `--revisions-schema`, `--lock-timeout`, `--dry-run`, and Go-template `--format` output over a Ptah apply result that mirrors Atlas's public apply-template fields.

External Atlas OSS directory formats (`golang-migrate`, `goose`, `flyway`, `liquibase`, `dbmate`) are read and converted in memory to Atlas single-file, up-only migrations and applied directly, reusing the format-loading layer shared with `ptah-compat migrate import`; unknown formats and Flyway repeatable migrations still fail before the target database is opened.

Directory URL `?format=` overrides `migration.format` whether the URL comes from project config or CLI. The pinned Atlas CE flag surface does not register `migrate apply --to-version` or `--lock-name`, so Ptah rejects them as unknown.

**Conformance status.** Measured for selected migration-directory and live SQLite amount, baseline, `LINEAR_SKIP` state semantics, dry-run baseline, JSON format, custom template, config-driven format, per-format up-only external-format execution (goose, dbmate, liquibase, golang-migrate, flyway), CLI and project URL-format precedence, unknown-format pre-connect rejection, no-op format, invalid-template preflight, redacted URL, failed-apply format cases, and CLI-surface rejection of non-CE flags.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#640`](https://github.com/stokaro/ptah/issues/640), [`stokaro/ptah#741`](https://github.com/stokaro/ptah/issues/741), [`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742).


### Migration down and rollback

**Atlas availability.** Open

**Ptah documentation.** [Roll back migrations](../../versioned/rollback/), [Atlas migrate commands](../migrate-commands/), [Comparison](../comparison/)

**Implementation status.** Partial. Ptah rolls back through pre-planned down files. `ptah-compat migrate down --dev-url` replays and verifies the rollback plan on the dev database before touching the target (native `ptah migrations down --shadow-db`), and `--format` renders an Atlas Go-template report over `.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, and `.Error`.

The registry-bound `--to-tag`, `--skip-checks`, and `--plan` flags are recorded waivers that fail loudly with their rationale; Atlas's registry-approved dynamic down planning itself stays out of scope.

**Conformance status.** Partially measured.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#758`](https://github.com/stokaro/ptah/issues/758).


### Migration diff generation

**Atlas availability.** Open

**Ptah documentation.** [Generate migrations](../../versioned/generate/), [Atlas migrate commands](../migrate-commands/), [Comparison](../comparison/)

**Implementation status.** Partial. Native Ptah can generate migrations from schema differences.

`ptah-compat migrate diff` now validates an existing `atlas.sum`, replays a
local Atlas migration directory on a directly connectable dev database, and
writes Atlas-style migration files. It:

- compares it to local schema files, one directly connectable database URL, one local Atlas migration directory, or one `env://` reference
- updates `atlas.sum` only after every file was written
- reads `env.schema.src`, `env.dev`, `migration.dir`, `format.migrate.diff`, and supported `diff` policy from `atlas.hcl` including `diff.concurrent_index.create` with `-- atlas:txmode none` file tagging and transactional/concurrent file splitting
- supports the Atlas-hidden `--dry-run` flag to print generated SQL without writing a migration file or `atlas.sum`
- supports `--lock-timeout` for Ptah's local migration-directory lock
- supports Atlas-style `--format` templates with `sql` and `.MarshalSQL` for the generated migration SQL
- supports `--schema` scoping for the resolved desired state plus the replayed dev database state
- supports `--edit` to open the generated migration in `$VISUAL`/`$EDITOR` before `atlas.sum` is finalized

`--qualifier` applies Atlas's single-schema custom qualifier to every object in the generated statements on PostgreSQL, CockroachDB, YugabyteDB, MySQL, and MariaDB dev databases, failing explicitly before any file or checksum write for invalid values, unsupported dialects, multi-schema plans, and not-yet-qualifiable statement kinds. Docker dev databases remain incomplete.

**Conformance status.** Partially measured with local SQLite dev DB, local schema-file, schema-filter, custom-format, config-driven format/env defaults, dry-run, invalid-format, lock-timeout, qualifier, and txmode-split coverage, CLI-surface flag probes, and a real-PostgreSQL end-to-end test for database desired-state scoping, concurrent-index metadata, and qualifier artifacts, plus real MySQL and MariaDB source-preservation and convergence tests.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#618`](https://github.com/stokaro/ptah/issues/618), [`stokaro/ptah#640`](https://github.com/stokaro/ptah/issues/640), [`stokaro/ptah#842`](https://github.com/stokaro/ptah/issues/842).


### Migration linting

**Atlas availability.** Mixed in current Atlas docs: feature page lists migration linting CLI as Pro while also listing a basic Open lint-rule set

**Ptah documentation.** [CI](../../testing/ci/), [Integrity and safety](../../versioned/integrity-and-safety/), [Comparison](../comparison/)

**Implementation status.** Partial.

Ptah ships native linting, SARIF, inline suppression, severity config, and `ptah-compat migrate lint`; `--dir-format` defaults to `atlas`, `--latest`, `--git-base`, `--git-dir`, and matching `atlas.hcl` defaults select the linted changeset; `--dev-url` infers lint dialect and treats directly connectable dev databases as scratch databases by cleaning and replaying migrations; `--format`, `format.migrate.lint`, and Atlas `lint { log = "…" }` render Atlas-style Go templates over `.Env`, `.Steps`, and `.Files`, and the no-template default reproduces Atlas's migration-analysis text report; supported `atlas.hcl` analyzer policy maps severity for matching Ptah lint rule families.

Atlas check-level policy, custom rules, force/allow-list analyzer options, Docker dev databases, web reports, and external migration-tool `--dir-format` execution remain gaps.

**Conformance status.** Partially measured with static lint, explicit Atlas dir-format latest selection, Git changeset selection, config-driven latest selection, policy-driven severity, compatibility-wrapper env policy, live SQLite dev-database replay, and Atlas Go-template output coverage.

**Follow-up.** [`stokaro/ptah#582`](https://github.com/stokaro/ptah/issues/582), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#622`](https://github.com/stokaro/ptah/issues/622).


### Migration directory integrity, hash, and validation

**Atlas availability.** Open versioned workflow concept

**Ptah documentation.** [Integrity and safety](../../versioned/integrity-and-safety/), [Atlas migrate commands](../migrate-commands/), [Exit codes](../../reference/exit-codes/)

**Implementation status.** Documented. Ptah supports `ptah.sum`, Atlas-compatible `atlas.sum`, hash, validate, and `migrate validate --dev-url` SQL replay paths. `ptah-compat migrate hash` and `validate` register Atlas `--dir-format` with default `atlas`; external migration-tool formats fail explicitly outside import. Remaining parity depends on exact Atlas edge cases.

**Conformance status.** Measured for selected directory fixtures, Atlas-default hash output, and live SQLite dev-database replay.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#622`](https://github.com/stokaro/ptah/issues/622).


### Migration import

**Atlas availability.** Open for local migration-directory formats

**Ptah documentation.** [Atlas migrate commands](../migrate-commands/), [Comparison](../comparison/)

**Implementation status.** Partial. Ptah imports local `file://` directories into an Atlas single-file directory and writes `atlas.sum`; Flyway repeatable migrations fail explicitly until Ptah can execute Atlas R-suffixed migrations.

**Conformance status.** Partially measured.

**Follow-up.** [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510).


### Manual migrations and troubleshooting

**Atlas availability.** Open docs

**Ptah documentation.** [Generate migrations](../../versioned/generate/), [Troubleshooting](../../operate/troubleshooting/), [Exit codes](../../reference/exit-codes/)

**Implementation status.** Documented for Ptah-native behavior. Atlas-specific troubleshooting strings and repair flows are not fully mirrored.

**Conformance status.** Partially measured.

**Follow-up.** [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) for docs polish; [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) for runtime semantics.


### Drift detection

**Atlas availability.** Feature page lists drift detection as Pro

**Ptah documentation.** [CI](../../testing/ci/), [Comparison](../comparison/)

**Implementation status.** Ptah has native `ptah schema drift`; Atlas Cloud/Pro drift monitoring is out of scope.

**Conformance status.** Ptah-native behavior is tested in repo; Atlas Cloud parity is not a target.

**Follow-up.** None for Cloud parity; [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) for docs depth.


### Checkpoints

**Atlas availability.** Feature page lists checkpoints as Pro

**Ptah documentation.** [Comparison](../comparison/), [Conformance](../conformance/)

**Implementation status.** Implemented natively and free. `ptah migrations checkpoint` squashes a directory's history into a cumulative-schema checkpoint that fresh databases bootstrap from, and `ptah-compat migrate checkpoint` forwards to it on the Atlas-compatible surface — a workflow Atlas keeps in its Pro build.

Checkpoint output covers both conventions. `--dir-format=atlas` — the default on the compat surface — writes Atlas's single up-only `<version>_<name>.sql` carrying the `-- atlas:checkpoint` directive on its first line and refreshes `atlas.sum`; `--dir-format=ptah` writes the reversible `.checkpoint.up.sql` / `.checkpoint.down.sql` pair and refreshes `ptah.sum`. `--dir-format=auto` is refused, because writing under it would have to guess the convention and the integrity file. The read side honors Atlas's `-- atlas:checkpoint` directive whoever wrote it: checkpoint directories bootstrap fresh databases from the latest checkpoint and are silently skipped on databases that already applied pre-checkpoint history, matching measured Atlas behavior.

**Conformance status.** Measured by native command and Atlas-compatibility tests, not as a community-version unsupported boundary.

**Follow-up.** [`stokaro/ptah#660`](https://github.com/stokaro/ptah/issues/660), [`stokaro/ptah#758`](https://github.com/stokaro/ptah/issues/758).


### Pre-migration checks and policy workflows

**Atlas availability.** Feature page lists pre-migration checks as Pro

**Ptah documentation.** [CI](../../testing/ci/), [Comparison](../comparison/)

**Implementation status.** Partial. The local assertion half is implemented in both spellings: the native `-- +ptah check` directive and Atlas txtar `checks.sql` / `checks/*.sql` sections, including file-level `atlas:assert oneof`. They are enforced as pre-migration gates rather than executed as plain SQL. The Atlas Cloud approval-policy half stays out of scope.

**Conformance status.** Measured against a licensed Atlas build (v1.2.4): a failing txtar assertion aborts the apply before any body statement on both binaries, and no revision row is recorded. Ptah also covers Atlas's documented named check files and one-of grouping.

**Follow-up.** [`stokaro/ptah#956`](https://github.com/stokaro/ptah/issues/956) closed the txtar `checks.sql` gap; no Atlas OSS issue unless a further Open check surface is identified.


### Testing framework

**Atlas availability.** Feature page lists testing framework as Pro

**Ptah documentation.** [Comparison](../comparison/), [Conformance](../conformance/)

**Implementation status.** Implemented natively and free. `ptah migrations test` and `ptah schema test` run declarative test cases against a throwaway database — a workflow Atlas keeps in its Pro build.

The Atlas-compatible `ptah-compat migrate test` and `ptah-compat schema test` verbs forward to the native runners with Atlas-shaped flags (`--dir`/`-u --url`, `--dev-url`, `--run`, project flags) and the native exit-code contract; Ptah-native YAML/Go test files are the executable payload, and Atlas `.test.hcl` ingestion remains a separate gap.

**Conformance status.** Measured by native command and Atlas-compatibility tests, not as a community-version unsupported boundary.

**Follow-up.** [`stokaro/ptah#659`](https://github.com/stokaro/ptah/issues/659), [`stokaro/ptah#758`](https://github.com/stokaro/ptah/issues/758).


### Declarative reference data

**Atlas availability.** Feature page lists declarative data management as Pro

**Ptah documentation.** [Comparison](../comparison/), [Reference data](../../versioned/reference-data/)

**Implementation status.** Implemented natively and free. `ptah migrations data` diffs declarative reference rows against a live table and writes a reversible data migration (`INSERT`/`UPDATE`/`DELETE`) with an exact inverse `down` — a workflow Atlas keeps in its Pro build and Atlas CE cannot inspect declaratively.

**Conformance status.** Measured by native command and round-trip reversibility tests, not as a community-version unsupported boundary.

**Follow-up.** [`stokaro/ptah#663`](https://github.com/stokaro/ptah/issues/663).


### Supported databases

**Atlas availability.** Open for PostgreSQL, MySQL, MariaDB, SQLite, TiDB, LibSQL in current Atlas feature matrix; many other drivers are Pro

**Ptah documentation.** [Capabilities](../../reference/capabilities/), [Comparison](../comparison/)

**Implementation status.** Partial but intentionally not identical. Ptah supports PostgreSQL, SQLite, MySQL/MariaDB, SQL Server subsets, and capability-gated PostgreSQL-compatible or specialty targets. Object-level support varies by dialect.

**Conformance status.** Partially measured by local, live, and conformance tests.

**Follow-up.** [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498) for fuller object-level docs; implementation gaps should be filed from concrete findings.


### Database object kinds

**Atlas availability.** Core object kinds open for common drivers; advanced PostgreSQL objects such as partitions, views, functions, sequences, extensions, and RLS are listed as Pro examples in Atlas docs

**Ptah documentation.** [Capabilities](../../reference/capabilities/), [HCL schema](../../schema/hcl/), site [HCL schema reference](../../reference/hcl-schema/)

**Implementation status.** Partial and not product-identical. Ptah supports some objects Atlas lists as Pro-gated, but HCL/config parity is still a subset until audited.

**Conformance status.** Partially measured.

**Follow-up.** [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511).


### Atlas Registry

**Atlas availability.** Cloud

**Ptah documentation.** [OCI registry artifacts](../../operate/oci-registry/), [License boundary](../license-boundary/), [Comparison](../comparison/)

**Implementation status.** Atlas Registry remains out of scope: Ptah has no Atlas Cloud dependency, account model, `atlas://` resolver, hosted UI, or Atlas deployment API. Ptah independently provides native `ptah migrations push/pull`, `ptah schema push/pull`, and `ptah oci referrers` commands for bring-your-own OCI registries, plus direct native consumers and best-effort deployment-report referrers.

The referrers command lists descriptor metadata but does not pull report payloads. The Atlas-compatible `migrate push` and `schema push` paths still mirror the Atlas CE unsupported boundary.

**Conformance status.** Atlas-compatible push stubs remain measured by CLI-surface conformance. Native OCI behavior is tested in Ptah and is not evidence of Atlas Cloud parity.

**Follow-up.** [`stokaro/ptah#638`](https://github.com/stokaro/ptah/issues/638), [`stokaro/ptah#664`](https://github.com/stokaro/ptah/issues/664).


### Atlas Cloud deployment reporting

**Atlas availability.** Cloud

**Ptah documentation.** [License boundary](../license-boundary/), [Comparison](../comparison/)

**Implementation status.** Out of scope. Ptah can be used in CI, but it does not report deployments to Atlas Cloud.

**Conformance status.** Not measured.

**Follow-up.** None for OSS parity.


### Cloud-only workflows and account commands

**Atlas availability.** Cloud/Pro

**Ptah documentation.** [License boundary](../license-boundary/), [Comparison](../comparison/)

**Implementation status.** Out of scope. Login, registry, UI, promotion, monitoring, and Cloud APIs are not Atlas OSS drop-in targets.

**Conformance status.** Not measured.

**Follow-up.** None for OSS parity.


### CI integrations

**Atlas availability.** Mixed: local CLI usage is open; Atlas Cloud deployment and lint reporting can require Pro/Cloud

**Ptah documentation.** [CI](../../testing/ci/), [Conformance](../conformance/)

**Implementation status.** Documented for Ptah-native CI and conformance interpretation. Atlas's official integrations are not cloned one by one.

**Conformance status.** Ptah CI is measured by repository workflows; Atlas integration parity is unmeasured.

**Follow-up.** [`stokaro/ptah#498`](https://github.com/stokaro/ptah/issues/498).


### Conformance evidence

**Atlas availability.** Atlas docs do not define Ptah conformance; this is Ptah-owned evidence

**Ptah documentation.** [Conformance](../conformance/), [Comparison](../comparison/)

**Implementation status.** Documented. Regression budget and full-conformance gates are intentionally separate.

**Conformance status.** Measured in `ptah-atlas-conformance`, with current limits documented there.

**Follow-up.** [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167).


### License and implementation boundary

**Atlas availability.** Atlas source is a separate upstream project; Ptah compatibility must stay license-clean

**Ptah documentation.** [License boundary](../license-boundary/), [Comparison](../comparison/)

**Implementation status.** Documented. Ptah does not import, vendor, port, or derive implementation code from Atlas. Public interfaces and separately held test assets are compatibility inputs.

**Conformance status.** Not a runtime conformance area.

**Follow-up.** Keep this page updated when conformance assets change.

## Follow-up issue coverage

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

## How to use this matrix

Use this page before changing Atlas-compatible behavior or documentation:

1. Find the Atlas docs area and official source link.
2. Check whether Ptah behavior is documented, partial, a gap, or out of scope.
3. If the row is partial or a gap, update the linked issue or create a focused
   follow-up issue before claiming support.
4. Update conformance only when the behavior can be measured by command,
   fixture, live database, or Atlas CE differential probes.

Do not turn a green docs build into a product parity claim. Product parity needs
current implementation evidence, conformance evidence, and a closed gap row.
