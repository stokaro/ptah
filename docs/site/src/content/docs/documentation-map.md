---
title: Documentation map
description: Which Ptah document to read for each task.
---

Use this page when you know what you need to do, but not where the relevant Ptah documentation lives.

| Situation | Read first | Then read |
| --- | --- | --- |
| I need to install Ptah | [Install Ptah](../install/) | [Quick start](../getting-started/) |
| I want to try Ptah locally | [Quick start](../getting-started/) | [Go model example](../examples/go-model/) |
| My Go app owns the schema | [Go schema workflow](../workflows/go-schema/) | [Migrations](../workflows/migrations/) |
| My schema lives in YAML | [Schema files](../workflows/schema-files/) | [YAML schema reference](https://github.com/stokaro/ptah/blob/master/docs/yaml_schema.md) |
| My schema lives in Atlas HCL | [Schema files](../workflows/schema-files/) | [Atlas HCL schema reference](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md) |
| I need Atlas-style commands | [Atlas-compatible CLI](../workflows/atlas-cli/) | [Comparison](../reference/comparison/) |
| I need to run Ptah in CI | [CI](../workflows/ci/) | [Exit codes](../reference/exit-codes/) |
| I need dialect behavior | [Capabilities](../reference/capabilities/) | Dialect-specific reference markdown such as `docs/sqlite.md` and `docs/sqlserver.md` |
| I need the public Go API | [`docs/public_api.md`](https://github.com/stokaro/ptah/blob/master/docs/public_api.md) | Stable packages, snapshots, and public API guard scripts |
| I need diagrams | [Schema visualization example](../examples/schema-viz/) | [`examples/viz`](https://github.com/stokaro/ptah/tree/master/examples/viz) |
| A command failed | [Troubleshooting](../operate/troubleshooting/) | The relevant command reference page |
| I need Atlas parity evidence | [Conformance](../operate/conformance/) | [`ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance) |
| I need license assurance | [License boundary](../operate/license-boundary/) | Conformance repository provenance notes |

## Documentation layers

| Layer | Purpose |
| --- | --- |
| `docs/site` | Human-facing documentation site and task-oriented guides. |
| `docs/*.md` | Detailed source references for commands, config, dialects, and design. |
| `examples/*` | Runnable local examples and generated artifacts. |
| `ptah-atlas-conformance` | External Atlas compatibility evidence and gap reports. |

When a task is covered by both the site and a source reference, use the site for the workflow and the source reference for exact flags, schema shapes, or implementation details.

## Maintenance rule

When Ptah behavior changes, update both layers that readers will hit:

- the task page in `docs/site/src/content/docs/`;
- the exact source reference in `docs/*.md`, `examples/*`, package docs, or
  conformance reports.

Do not update only the nearest README when a command path, flag, config key,
generated SQL shape, public API, or Atlas parity claim changes.
