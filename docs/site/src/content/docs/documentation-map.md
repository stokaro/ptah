---
title: Documentation map
description: Which Ptah document to read for each task.
---

Use this page when you know what you need to do, but not where the relevant Ptah documentation lives.

| Situation | Read first | Then read |
| --- | --- | --- |
| I need to install Ptah | [Install Ptah](../start/install/) | [Quick start](../start/quick-start/) |
| I want to try Ptah locally | [Quick start](../start/quick-start/) | [Go annotations](../schema/go-annotations/) |
| I have not picked between versioned and direct changes | [Choose a workflow](../start/choose-a-workflow/) | [Migrations](../workflows/migrations/) or [Atlas-compatible CLI](../workflows/atlas-cli/) |
| I have a live database built outside Ptah | [Adopt an existing database](../start/adopt-an-existing-database/) | [Migrations](../workflows/migrations/) |
| My Go app owns the schema | [Go annotations](../schema/go-annotations/) | [Migrations](../workflows/migrations/) |
| My schema lives in YAML | [YAML schema](../schema/yaml/) | [YAML schema reference](../reference/yaml-schema/) |
| My schema lives in HCL | [HCL schema](../schema/hcl/) | [HCL schema reference](../reference/hcl-schema/) |
| My schema lives in SQL files | [SQL schema](../schema/sql/) | [Composite desired schema](../schema/composite/) |
| My ORM owns the schema | [ORM and external loaders](../schema/orm-and-external/) | [Composite desired schema](../schema/composite/) |
| I want to publish or consume migrations and schemas through OCI | [OCI registry artifacts](../workflows/oci-registry/) | [Migrations](../workflows/migrations/) and [Commands](../reference/commands/) |
| I need Atlas-style commands | [Atlas-compatible CLI](../workflows/atlas-cli/) | [Comparison](../reference/comparison/) |
| I want to embed Ptah in another Go tool | [Reusable components](../reference/reusable-components/) | [Public Go API](../reference/public-api/) |
| I need to run Ptah in CI | [CI](../workflows/ci/) | [Exit codes](../reference/exit-codes/) |
| I need to test migrations or a desired schema | [Testing](../workflows/testing/) | [Database test commands](../reference/testing/) |
| I need dialect behavior | [Capabilities](../reference/capabilities/) | [Dialect notes](../reference/dialect-notes/) |
| I need Atlas docs coverage | [Atlas docs coverage](../reference/atlas-docs-coverage/) | [Comparison](../reference/comparison/) and [Conformance](../operate/conformance/) |
| I need the public Go API | [Public Go API](../reference/public-api/) | Stable packages, snapshots, and public API guard scripts |
| I need diagrams | [Visualize the schema](../schema/visualize/) | [`examples/viz`](https://github.com/stokaro/ptah/tree/master/examples/viz) |
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
