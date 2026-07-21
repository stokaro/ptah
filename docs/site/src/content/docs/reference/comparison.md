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

| Task | Native Ptah | `ptah atlas` | `ptah-compat` / renamed `atlas` | Atlas OSS |
| --- | --- | --- | --- | --- |
| Apply migrations | `ptah migrations up` | `ptah atlas migrate apply` | `ptah-compat migrate apply` / `atlas migrate apply` | `atlas migrate apply` |
| Roll back migrations | `ptah migrations down` | `ptah atlas migrate down` | `ptah-compat migrate down` / `atlas migrate down` | `atlas migrate down` |
| Migration status | `ptah migrations status` | `ptah atlas migrate status` | `ptah-compat migrate status` / `atlas migrate status` | `atlas migrate status` |
| Hash migrations | `ptah migrations hash` | `ptah atlas migrate hash` | `ptah-compat migrate hash` / `atlas migrate hash` | `atlas migrate hash` |
| Validate migrations | `ptah migrations validate` | `ptah atlas migrate validate` | `ptah-compat migrate validate` / `atlas migrate validate` | `atlas migrate validate` |
| Lint migrations | `ptah migrations lint` | `ptah atlas migrate lint` | `ptah-compat migrate lint` / `atlas migrate lint` | Current Atlas docs list the migration linting CLI feature as Pro and a basic lint-rule set as Open. |
| Create an empty migration | `ptah migrations create` | `ptah atlas migrate new` | `ptah-compat migrate new` / `atlas migrate new` | `atlas migrate new` |
| Repair revision state | `ptah migrations repair` | `ptah atlas migrate set` | `ptah-compat migrate set` / `atlas migrate set` | `atlas migrate set` |
| Inspect schema | `ptah db read` | `ptah atlas schema inspect` | `ptah-compat schema inspect` / `atlas schema inspect` | `atlas schema inspect` |
| Diff schema | `ptah schema compare` | `ptah atlas schema diff` | `ptah-compat schema diff` / `atlas schema diff` | `atlas schema diff` |

Some Atlas command paths are intentionally registered before complete runtime
behavior exists, and some accepted Atlas flags still fail explicitly rather than
being silently ignored. The gap register below links that work to concrete
tracking issues.

## Detailed product comparison

| Area | Ptah | Atlas OSS | Atlas Commercial / Cloud | Evidence |
| --- | --- | --- | --- | --- |
| License and implementation | MIT-licensed independent implementation. Ptah compatibility code is written in this repository and does not import or vendor Atlas source. | Atlas is an independent upstream product. Ptah treats its public command names, flags, file formats, and observable behavior as compatibility inputs. | Same Atlas product family plus licensed Pro and Cloud capabilities. | [License boundary](../../operate/license-boundary/), [Atlas feature availability](https://atlasgo.io/features) |
| Command compatibility | Native command tree plus Atlas-compatible paths under `ptah atlas <command> ...`. The separate `ptah-compat` binary exposes the same Atlas-compatible command tree at process root and can be copied or symlinked as `atlas`. Some paths and flags are still tracked gaps. | Open CLI feature surface includes inspection, schema diffing, versioned migrations, and declarative migrations. | Pro and Cloud add capabilities that are not OSS drop-in targets, such as checkpoints, rollout, testing, and registry-backed workflows. | [Atlas CLI reference](https://atlasgo.io/cli-reference), [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah#514`](https://github.com/stokaro/ptah/issues/514) |
| Schema inspection | `ptah db read` and `ptah atlas schema inspect` inspect supported live databases into Ptah schema IR/output formats. Atlas flags such as `--format`, `--exclude`, and `--dev-url` are tracked gaps. | `atlas schema inspect` is documented as an open CLI feature for inspecting a database schema. | Commercial database drivers broaden the set of inspectable engines. | [Atlas CLI reference](https://atlasgo.io/cli-reference), [Capabilities](../capabilities/), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Schema diff and apply | `ptah schema compare` and `ptah atlas schema diff` cover Ptah's current schema diff path. `ptah atlas schema apply` is registered but still a runtime placeholder. | Atlas OSS documents schema diffing and declarative migrations as open CLI features. | Cloud/Pro workflows add registry-backed plans, approvals, and deployment tracking. | [Atlas feature availability](https://atlasgo.io/features), [pre-planning schema migrations](https://atlasgo.io/declarative/plan), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Versioned migrations | `ptah migrations up`, `down`, `status`, `hash`, `validate`, `create`, `repair`, and Atlas-compatible counterparts cover local migration workflows. `ptah atlas migrate down` forwards to Ptah's pre-planned down-file rollback path, maps Atlas-compatible flags whose behavior matches native Ptah behavior, and fails explicitly for Atlas dynamic down-planning and output-format behavior that is not implemented yet. `ptah atlas migrate import` and `ptah atlas migrate diff` remain runtime placeholders. | Atlas OSS includes versioned migrations and documents `atlas migrate down` for reverting applied migrations. | Atlas Registry and deployment reporting add remote migration-directory storage, tagging, history, and environment promotion workflows. Pro adds approval workflows for protected down plans. | [Atlas feature availability](https://atlasgo.io/features), [Atlas down migrations](https://atlasgo.io/versioned/down), [Atlas Cloud deployment docs](https://atlasgo.io/cloud/deployment), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Migration linting | Ptah ships first-party migration linting and the `ptah atlas migrate lint` compatibility path, with some Atlas flags still incomplete. | Current Atlas docs mark the official migration linting CLI feature as Pro while the feature availability page also lists a basic Open lint-rule set. | Pro migration linting includes Atlas analyzers, policy workflows, enforced checks, and browser reports. | [Atlas feature availability](https://atlasgo.io/features), [Atlas migration linting docs](https://atlasgo.io/versioned/lint), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Cloud and registry features | Ptah has no Atlas Cloud dependency and no Atlas Registry implementation. | Not part of the open drop-in target surface unless a workflow is explicitly available without Cloud credentials. | Atlas Cloud provides registry, deployment reporting, cloud CLI commands, UI, Pro seats, pipelines, and schema monitoring. | [Atlas Registry](https://atlasgo.io/cloud/features/registry), [Atlas Cloud deployment docs](https://atlasgo.io/cloud/deployment), [Atlas pricing](https://atlasgo.io/cloud/pricing) |
| Supported databases | Ptah has first-party support for PostgreSQL, SQLite, MySQL/MariaDB, SQL Server subsets, and capability-gated PostgreSQL-compatible or specialty targets. | Atlas docs list PostgreSQL, MySQL, MariaDB, SQLite, TiDB, and LibSQL as Open drivers. | Atlas Pro adds SQL Server, ClickHouse, Redshift, Oracle, Spanner, Snowflake, Databricks, CockroachDB, Azure HorizonDB, YugabyteDB, Aurora DSQL, Azure Fabric, and related drivers. | [Capabilities](../capabilities/), [Atlas feature availability](https://atlasgo.io/features) |
| HCL and config | Ptah parses strict Atlas schema HCL and project config subsets. Unsupported constructs fail explicitly rather than being silently ignored. | Atlas OSS supports SQL, HCL schema, external schema, remote/template directories, and related data sources listed as Open. | Pro data sources include composite schema and blob directory features. | [Atlas HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md), [Atlas project config](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md), [Atlas feature availability](https://atlasgo.io/features), [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511) |
| Conformance status | Ptah uses the separate `ptah-atlas-conformance` repository as measured evidence against Atlas fixtures and behavior. The regression budget and full-conformance gates are intentionally separate: budget green means no unexpected regression, while full-conformance can remain red for known Atlas OSS gaps such as dynamic down planning. | Atlas fixtures and CLI behavior provide the comparison target for OSS-compatible behavior. | Commercial/cloud-only behavior is separated from the OSS drop-in target and tracked as documentation scope. | [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md), [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md), [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510), [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167) |

## Feature parity evidence

| Area | Ptah status | Evidence |
| --- | --- | --- |
| Offline Atlas fixture ingestion | The imported Atlas fixture corpus and CLI probes are tracked in the conformance repository. Treat a red full-conformance gate as product work, not as a broken regression gate; the regression budget records which known gaps are currently tolerated. | [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md), [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Live database round trips | Current live smoke corpus is green: 10 observations, 0 non-OK in the linked report. This is evidence for the covered scenarios, not proof of every Atlas OSS runtime path. | [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md) |
| Atlas CE differential checks | Current Atlas CE differential corpus is green: 5 observations, 0 non-OK in the linked report. This is evidence for the covered scenarios, not proof of every Atlas OSS schema object. | [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md) |
| Atlas HCL schema files | Strict supported subset. Unsupported constructs fail explicitly instead of being ignored. | [Atlas HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md) |
| Atlas project config | Strict supported subset. Unsupported constructs fail explicitly instead of being ignored. | [Atlas project config](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md) |
| Native Go annotations | First-party Ptah workflow. | [Go schema workflow](../../workflows/go-schema/) |

## Gap register

| Gap | Type | Current boundary | Tracking |
| --- | --- | --- | --- |
| Atlas-compatible command runtime placeholders | Product behavior | These registered paths currently report that runtime behavior is not implemented yet: `ptah atlas schema apply`, `ptah atlas schema fmt`, `ptah atlas migrate diff`, and `ptah atlas migrate import`. `ptah atlas version` and `ptah atlas license` now execute Ptah's build-info and license-clean notice behavior. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Atlas-compatible binary-level entry point | Product packaging | `ptah-compat` exposes Atlas-compatible commands at process root for script migration and can present itself as `atlas` when installed under that executable name. This changes command shape, not the remaining runtime gaps listed for #510. | [`stokaro/ptah#514`](https://github.com/stokaro/ptah/issues/514) |
| Atlas-compatible down semantics | Product behavior | `ptah atlas migrate down` is an Atlas OSS command path and recognizes the documented Atlas-style flag names. Flags mapped to native behavior include `--url`, `--dir`, `--to-version`, `--dry-run`, `--revisions-schema`, and `--lock-timeout`. Unsupported Atlas-only behavior currently fails explicitly, including dynamic planning or safety inputs such as `--dev-url`, `--to-tag`, `--skip-checks`, and `--plan`, plus Atlas Go template output formatting via `--format`. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Atlas-compatible flag semantics | Product behavior | Accepted flags whose behavior is still incomplete include `schema inspect --dev-url`, `schema inspect --exclude`, `schema inspect --format`, `schema diff --from`, `schema diff --to`, `schema diff --dev-url`, `schema diff --format`, `schema clean --auto-approve`, `migrate apply --tx-mode`, `migrate down --dev-url`, `migrate down --to-tag`, `migrate down --format`, `migrate down --skip-checks`, `migrate down --plan`, `migrate lint --dev-url`, `migrate lint --latest`, and `migrate validate --dev-url`. This is not the full Atlas flag gap: unregistered Atlas flags also need a full audit and either implementation or explicit out-of-scope classification. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Atlas HCL schema and project config subset audit | Product behavior and coverage | Current imported fixtures pass, and there are no concrete unsupported Atlas OSS schema/config constructs listed in the current conformance reports. Complete schema/config parity is not claimed until the remaining Atlas OSS surface is audited; newly discovered unsupported constructs should become focused implementation issues. | [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511) |
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
