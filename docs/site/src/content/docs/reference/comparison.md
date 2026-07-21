---
title: Comparison
description: Ptah native commands, Atlas-compatible commands, feature parity, config precedence, and safety behavior.
---

## Command parity

| Task | Native Ptah | Atlas-compatible Ptah | Atlas OSS |
| --- | --- | --- | --- |
| Apply migrations | `ptah migrations up` | `ptah atlas migrate apply` | `atlas migrate apply` |
| Roll back migrations | `ptah migrations down` | `ptah atlas migrate down` | `atlas migrate down` |
| Migration status | `ptah migrations status` | `ptah atlas migrate status` | `atlas migrate status` |
| Hash migrations | `ptah migrations hash` | `ptah atlas migrate hash` | `atlas migrate hash` |
| Validate migrations | `ptah migrations validate` | `ptah atlas migrate validate` | `atlas migrate validate` |
| Lint migrations | `ptah migrations lint` | `ptah atlas migrate lint` | `atlas migrate lint` |
| Inspect schema | `ptah db read` | `ptah atlas schema inspect` | `atlas schema inspect` |
| Diff schema | `ptah schema compare` | `ptah atlas schema diff` | `atlas schema diff` |

## Feature parity

| Area | Ptah status | Evidence |
| --- | --- | --- |
| Offline Atlas fixture ingestion | Currently green in the conformance repo. | [Conformance](../operate/conformance/) |
| Live database round trips | Has known gaps. | [Conformance](../operate/conformance/) |
| Atlas CE differential checks | Has known gaps. | [Conformance](../operate/conformance/) |
| Atlas HCL schema files | Supported subset. | [Atlas HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md) |
| Atlas project config | Supported subset. | [Atlas project config](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md) |
| Native Go annotations | First-party Ptah workflow. | [Go schema workflow](../workflows/go-schema/) |

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

Reference: [Exit codes](./exit-codes/).
