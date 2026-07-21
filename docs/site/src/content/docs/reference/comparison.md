---
title: Comparison
description: Ptah native commands, Atlas-compatible commands, feature parity, config precedence, and safety behavior.
---

## Command parity

| Task | Native Ptah | Atlas-compatible Ptah | Atlas OSS |
| --- | --- | --- | --- |
| Apply migrations | `ptah migrations up` | `ptah atlas migrate apply` | `atlas migrate apply` |
| Roll back migrations | `ptah migrations down` | Ptah extension path; not tracked as an Atlas OSS drop-in target by current conformance. | Not in the current OSS target corpus |
| Migration status | `ptah migrations status` | `ptah atlas migrate status` | `atlas migrate status` |
| Hash migrations | `ptah migrations hash` | `ptah atlas migrate hash` | `atlas migrate hash` |
| Validate migrations | `ptah migrations validate` | `ptah atlas migrate validate` | `atlas migrate validate` |
| Lint migrations | `ptah migrations lint` | `ptah atlas migrate lint` | `atlas migrate lint` |
| Create an empty migration | `ptah migrations create` | `ptah atlas migrate new` | `atlas migrate new` |
| Repair revision state | `ptah migrations repair` | `ptah atlas migrate set` | `atlas migrate set` |
| Inspect schema | `ptah db read` | `ptah atlas schema inspect` | `atlas schema inspect` |
| Diff schema | `ptah schema compare` | `ptah atlas schema diff` | `atlas schema diff` |

Some Atlas command paths are intentionally registered before complete runtime
behavior exists, and some accepted Atlas flags still fail explicitly rather than
being silently ignored. The gap register below links that work to concrete
tracking issues.

## Feature parity evidence

| Area | Ptah status | Evidence |
| --- | --- | --- |
| Offline Atlas fixture ingestion | Current imported Atlas fixture corpus is green: 160 fixtures, 636 observations, 0 non-OK in the linked report. | [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md) |
| Live database round trips | Current live smoke corpus is green: 10 observations, 0 non-OK in the linked report. This is evidence for the covered scenarios, not proof of every Atlas OSS runtime path. | [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md) |
| Atlas CE differential checks | Current Atlas CE differential corpus is green: 5 observations, 0 non-OK in the linked report. This is evidence for the covered scenarios, not proof of every Atlas OSS schema object. | [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md) |
| Atlas HCL schema files | Strict supported subset. Unsupported constructs fail explicitly instead of being ignored. | [Atlas HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md) |
| Atlas project config | Strict supported subset. Unsupported constructs fail explicitly instead of being ignored. | [Atlas project config](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md) |
| Native Go annotations | First-party Ptah workflow. | [Go schema workflow](../../workflows/go-schema/) |

## Gap register

| Gap | Type | Current boundary | Tracking |
| --- | --- | --- | --- |
| Atlas-compatible command runtime placeholders | Product behavior | These registered paths currently report that runtime behavior is not implemented yet: `ptah atlas schema apply`, `ptah atlas schema fmt`, `ptah atlas migrate diff`, `ptah atlas migrate import`, `ptah atlas version`, and `ptah atlas license`. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Atlas-compatible flag semantics | Product behavior | Accepted flags whose behavior is still incomplete: `schema inspect --dev-url`, `schema inspect --exclude`, `schema inspect --format`, `schema diff --from`, `schema diff --to`, `schema diff --dev-url`, `schema diff --format`, `schema clean --auto-approve`, `migrate apply --tx-mode`, `migrate lint --dev-url`, `migrate lint --latest`, and `migrate validate --dev-url`. | [`stokaro/ptah#510`](https://github.com/stokaro/ptah/issues/510) |
| Atlas HCL schema and project config subset audit | Product behavior and coverage | Current imported fixtures pass, and there are no concrete unsupported Atlas OSS schema/config constructs listed in the current conformance reports. Complete schema/config parity is not claimed until the remaining Atlas OSS surface is audited; newly discovered unsupported constructs should become focused implementation issues. | [`stokaro/ptah#511`](https://github.com/stokaro/ptah/issues/511) |
| Live and differential corpus breadth | Conformance coverage | The live and Atlas CE differential reports are green for the current smoke corpus only. More fixtures are needed before using those checks as broad Atlas OSS parity evidence. | [`stokaro/ptah-atlas-conformance#167`](https://github.com/stokaro/ptah-atlas-conformance/issues/167) |
| Atlas OSS vs Atlas Commercial scope | Documentation | Cloud, registry, Pro, and commercial-only Atlas commands are not OSS drop-in targets, but the public comparison needs a clearer split. | [`stokaro/ptah#497`](https://github.com/stokaro/ptah/issues/497) |

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
