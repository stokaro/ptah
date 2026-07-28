---
title: Database test commands
description: Exact flags, steps, assertions, reports, and isolation behavior for migration and schema tests.
---

Ptah has two native declarative test commands:

```bash
ptah migrations test [flags]
ptah schema test [flags]
```

Both load `*.yaml` and `*.yml` files from `--dir`, execute cases in file order,
render a report, and return a non-zero status when any case fails.

## Command flags

| Flag | Migrations test | Schema test | Default |
| --- | --- | --- | --- |
| `--dir` | Yes | Yes | `./tests` |
| `--migrations-dir` | Yes | No | `./migrations` |
| `--root-dir` | Used by `apply_schema` | Desired schema applied before cases | `./models` |
| `--seed-dir` | Default for seed steps | Default for seed steps | No default |
| `--dir-format` | `auto`, `ptah`, or `atlas` | No | `ptah` |
| `--db-url` | Optional explicit throwaway database | Optional explicit throwaway database | Ephemeral SQLite |
| `--run` | Go regular expression matched against case names | Go regular expression matched against case names | All cases |
| `--report` | `text`, `json`, or `html` | `text`, `json`, or `html` | `text` |

## YAML model

Each file contains `cases`, each case contains `name` and `steps`, and each step
sets exactly one action:

| Action | Value | Scope |
| --- | --- | --- |
| `migrate_to` | Non-negative integer version, `latest`, or `0` | Migration tests |
| `apply_schema` | `true` | Additively converge objects declared under `--root-dir` using the live target |
| `seed` | Mapping with required `env`; optional `dir` overrides `--seed-dir` | Both |
| `exec` | SQL string | Both |
| `assert` | Mapping with `query` and one condition | Both |

Assertion conditions are `row_count` (a non-negative integer), `scalar`, and
`error_contains`. Unknown fields, unnamed cases, empty step lists, and steps
with zero or multiple actions are errors. Every seed step must either set `dir`
or receive a run-level `--seed-dir`.

## Isolation

Without `--db-url`, every case gets a separate ephemeral SQLite database. With
`--db-url`, all cases share the explicit caller-owned database and the caller
must isolate and clean it. The explicit target must be disposable because test
steps mutate schema and data. Reusing it is supported for idempotent cases, but
concurrent runners are not serialized and arbitrary SQL is not rolled back.

`apply_schema` preserves unrelated objects, uses live dialect capabilities and
identifier semantics, and can repair supported drift. Roles and grants are
rejected because their effects can escape the database-local test lifecycle.

## Exit contract

| Exit | Meaning |
| --- | --- |
| `0` | Every case passed. |
| `1` | The runner completed and at least one case failed. |
| `2` | Usage, input, connection, interruption, setup, or report failure. |

For examples and CI guidance, see
[Test migrations and schemas](../../testing/migrations-and-schema/). For the
embeddable runner contract, see [Public Go API](../public-api/).
