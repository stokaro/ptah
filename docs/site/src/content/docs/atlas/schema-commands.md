---
title: Atlas schema commands
description: Inspect, diff, apply, plan, format, clean, and test schemas with the Atlas-style ptah atlas schema verbs.
---

You want Atlas-style declarative schema work — inspect a live database, diff
schema files, apply or plan desired-schema changes — through Ptah. This page
covers the `ptah atlas schema` verbs with runnable examples. The surfaces and
flag translation rules are on the
[Atlas compatibility overview](../overview/).

## Command behavior

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas schema inspect` | Inspects a live database and writes Atlas-shaped HCL, SQL, JSON, or custom-template output. |
| `ptah atlas schema apply` | Diffs a desired-state source (schema files, a database URL, a migration directory, or an `env://` reference) against a live database and applies the planned SQL after confirmation. |
| `ptah atlas schema plan` | Saves the declarative plan as a fingerprinted local plan file for a later `schema apply --plan`. |
| `ptah atlas schema diff` | Diffs two desired-state sources (schema files, database URLs, migration directories, or `env://` references) and prints migration SQL. |
| `ptah atlas schema fmt` | Formats local `.hcl` files using HCL canonical layout. |
| `ptah atlas schema clean` | Plans and applies destructive cleanup of user-owned schema objects. |
| `ptah atlas schema test [paths]` | Forwards to `ptah schema test` with Ptah-native YAML test cases. |
| `ptah atlas schema push` | Atlas CE boundary stub; the native `ptah schema push` to any OCI registry is the open replacement. |

Per-verb status detail — Atlas differences, waivers, and the inputs that fail
explicitly — is on [Atlas-compatible commands](../../reference/atlas-commands/).

## Inspect a live database

`ptah atlas schema inspect` accepts a live database `--url` and writes
machine-oriented schema output without native Ptah status banners. The default
format is Atlas-compatible HCL.

```bash
ptah atlas schema inspect --url "$DATABASE_URL" > schema.hcl
ptah atlas schema inspect --url "$DATABASE_URL" --format sql > schema.sql
ptah atlas schema inspect --url "$DATABASE_URL" --format json > schema.json
```

`--schema` / `-s` narrows inspection when the underlying database reader supports
schema scoping. `--dev-url` validates dialect compatibility only today; Ptah
does not yet run Atlas dev-database inference for inspection. `--format`
accepts Atlas-style Go templates with `.MarshalHCL`, `hcl`, `sql`, `json`,
`base64url`, `mermaid`, `split`, and `write`. Basic split-write exports are
supported for HCL and SQL output:

```bash
ptah atlas schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ hcl . | split | write "schema" }}'

ptah atlas schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ sql . | split | write "schema" }}'
```

`--exclude` accepts repeated or comma-separated
Atlas-style glob patterns, including `[type=...]` selectors, and removes
matching resources from HCL, SQL, JSON, and custom-template output. Field-level
exclude selector support includes the Atlas-documented
`*[type=extension].version` form. Other field-level selectors fail explicitly
until Ptah models those fields as independently filterable resources.
Schema-qualified function and enum filters remain limited by Ptah's current
introspection model, which does not retain schema names for those resource types
yet. `--include` is not part of the pinned Atlas CE inspect flag surface.
File-backed inspection, exporter blocks, and advanced split/write configuration
remain explicit gaps.

## Apply a desired schema

`ptah atlas schema apply` accepts a live database `--url` and a `--to`
desired state: one or more local schema file URLs, one directly connectable
database URL whose live schema becomes the desired state, one migration
directory (a `file://` directory containing `atlas.sum`) replayed on the
required `--dev-url` dev database, or one `env://<attribute>` reference
(`src`, `schema.src`, `url`, `dev`, `migration.dir`) resolved through the
evaluated `atlas.hcl` env. All `--to` values must be one source kind, and
unsupported schemes such as `atlas://` fail before the target database is
contacted. With `--env`, Ptah can read `env.url`, `env.src`,
`env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`,
`format.schema.apply`, and supported `diff` policy from the selected
`atlas.hcl` environment, including local variable defaults, locals, `getenv`,
`file`, `fileset`, `format`, `jsonencode`, and `data.hcl_schema.<name>.url`
references. Explicit CLI flags still take precedence. Ptah reads the current
database schema, diffs it against the desired local schema files, prints the
planned SQL, and applies it after interactive confirmation. Use `--dry-run` to
print the plan without applying it, or `--auto-approve` to skip the prompt
explicitly. Use `--tx-mode=file` or `--tx-mode=all` to execute the generated
plan in one transaction, or `--tx-mode=none` to execute statements without
transaction wrapping. With `--edit`, the planned SQL opens in `$VISUAL` or
`$EDITOR` before the plan is shown and approved, and the edited SQL is what
gets applied.

For Atlas script compatibility, `schema apply` also accepts the hidden
`--file/-f` alias for local HCL or SQL paths and maps it to the same
local desired-schema loading path as `--to`. `--file` and `--to` are mutually
exclusive.

```bash
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
```

Expected output includes:

```text
Planned schema changes:
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL UNIQUE,
  "name" TEXT
);
```

An `atlas.hcl` environment can carry the same inputs:

```hcl
data "hcl_schema" "app" {
  paths = fileset("schema/*.hcl")
}

env "local" {
  url = getenv("DATABASE_URL")
  dev = getenv("DEV_DATABASE_URL")
  schema {
    src = data.hcl_schema.app.url
    mode {
      funcs = false
    }
  }
  format {
    schema {
      apply = "{{ sql . \"  \" }}"
    }
  }
}
```

```bash
ptah atlas schema apply --env local --dry-run
```

`--dev-url` must match the target database dialect. For migration-directory
`--to` sources it names the dev database the directory is replayed on and is
required. Before a non-dry-run apply touches the target, the generated plan is
rehearsed on the dev database: Ptah resets the dev database, recreates the
target's introspected current schema on it, and executes the exact ordered
plan statements — including SQL edited through `--edit` — under the same
transaction mode as the target apply. A failed rehearsal refuses the apply and
leaves the target unchanged. The dev database must not be the target itself
or a database-URL `--to` desired state (it is reset destructively), must be
directly connectable (no `docker://`), and must use the same schema scope as
the target on scope-parameterized dialects such as SQL Server.

`--lock-timeout` bounds how long the apply waits for the session advisory lock
(`ptah_schema_apply`) that serializes concurrent schema applies against one
target database. The lock is acquired before target inspection and planning,
held through simulation, confirmation, and execution, and released on every
exit path including cancellation. An empty value waits indefinitely; an
elapsed timeout fails the apply before the target is inspected. PostgreSQL
(`pg_advisory_lock`), MySQL and MariaDB (`GET_LOCK`), and SQL Server
(`sp_getapplock`) use real database locks. SQLite, ClickHouse, CockroachDB,
YugabyteDB, and Spanner have no advisory-lock semantics: the apply proceeds
without a lock, and an explicitly passed `--lock-timeout` prints a note on
stderr.

`--exclude` accepts repeated or comma-separated Atlas-style glob patterns,
including `[type=...]` selectors. Ptah applies the filter to both the current
live schema and the desired local schema files before planning, so excluded
objects are ignored rather than dropped.

Disabled `schema.mode` values are mapped to the same resource-exclusion system
for object kinds represented in Ptah's schema IR. `diff.skip.drop_table = true`
removes table drops from supported local plans. For non-dry-run PostgreSQL
`schema apply` plans that actually emit `CREATE INDEX CONCURRENTLY`,
`diff.concurrent_index.create = true` requires `--tx-mode none`;
`diff.concurrent_index.drop` and `diff.skip.drop_schema` fail explicitly.

### Scope the comparison with `--schema` and `--include`

`--schema/-s` and `--include` positively select what both comparison sides
see, on `schema apply` and `schema diff` alike:

- `--schema` names define the schema universe. Repeated and comma-separated
  values union deterministically. On PostgreSQL-family targets the names are
  schema namespaces and unqualified objects belong to the connection's default
  schema (`public`). On MySQL and MariaDB a schema is a database, and because
  a Ptah connection is bound to one database, only the connected database's
  name selects anything. SQLite has the single schema `main`.
- `--include` picks top-level resources inside that universe with Atlas-style
  glob selectors, optionally constrained with `[type=...]`. Selectable types:
  `table`, `view`, `materialized_view`, `function`, `enum`, `extension`,
  `sequence`, `domain`, `composite_type`, `range`, and `role`. Repeated and
  comma-separated selectors union deterministically. Children of a selected
  table — columns, indexes, constraints, triggers, policies, grants, and seed
  data — ride along with it, and support objects the selection depends on
  (enums and other types used by kept columns, sequences owned by kept
  tables, roles named by kept grants, owning schemas) are retained.
  Child-resource selectors such as `[type=column]` or `[type=index]`, field
  selectors, and unknown resource types are rejected loudly because Ptah
  cannot project a partial parent faithfully.
- `--exclude` and disabled `schema.mode` values subtract from the positive
  selection afterward. The composition order is fixed: schema universe first,
  include selection inside it, exclusion last.

The same projection is applied to the current database state and the desired
schema, so out-of-scope objects are invisible to the comparison and are never
created, modified, or dropped. A selected object that depends on an object the
selection dropped — a foreign key to an unselected table, a function calling
an unselected function, a view or trigger body referencing an unselected
relation, a column using an excluded enum — refuses the plan with an explicit
cross-scope diagnostic instead of emitting incomplete SQL:

```text
error: apply --schema/--include to desired schema: the --schema/--include selection drops objects that selected objects depend on:
  - table "scope_groups" depends on table "scope_users" via a foreign key, but "scope_users" is not selected
add the missing objects to the selection or exclude the dependent objects
```

A selection that matches nothing is not an error: the comparison sees two
empty projections and reports a synced schema. Malformed selectors fail during
validation, before any database is contacted.

`--format` accepts Atlas-style Go templates over the planned apply changes. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run \
  --format '{{ sql . "  " }}'
```

## Save and execute plan files

`ptah atlas schema plan` is the open local replacement for Atlas's Pro
registry-gated plan workflow. It computes the same declarative plan `schema
apply` would generate — from the `--from` target database to the local `--to`
schema files — and saves it as a local JSON plan file (`format_version` 1) that
records the ordered SQL statements with per-statement safety severity, the
dialect, and the SHA-256 fingerprints of the source and desired schema states.
`schema apply --plan file://<path>` then executes exactly the reviewed
statements after verifying the live database still matches the plan's source
fingerprint; a drifted database refuses with a stale-plan error instead of
running reviewed SQL against unreviewed state.

```bash
# Compute and save the plan for review (or --save for ./<name>.plan.json).
ptah atlas schema plan \
  --from "$DATABASE_URL" \
  --to file://schema.sql \
  --output add-orders.plan.json

# Later, execute exactly the reviewed plan; drift refuses loudly.
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --plan file://add-orders.plan.json \
  --auto-approve
```

Expected output of the plan step ends with:

```text
Plan saved to file://add-orders.plan.json
```

If the target database changed after the plan was saved, apply refuses with a
stale-plan error naming both fingerprints:

```text
error: pre-planned migration is stale: the target database schema does not match the plan's source fingerprint (plan sha256:..., database sha256:...); the database changed since the plan was computed, so re-run `schema plan` against the current database and review the fresh plan
```

## Diff schema files

`ptah atlas schema diff` accepts a desired-state source on each side: one or
more local `--from`/`--to` schema file URLs, one directly connectable database
URL whose live schema is introspected, one migration directory (a `file://`
directory containing `atlas.sum`) replayed on the required `--dev-url` dev
database, or one `env://<attribute>` reference (`src`, `schema.src`, `url`,
`dev`, `migration.dir`) resolved through the evaluated `atlas.hcl` env. All
URLs of one flag must be one source kind. The SQL dialect is pinned by
`--dev-url` first, then by `--from` and `--to` database URLs; local schema
files alone still require `--dev-url` for dialect selection. With `--env`,
Ptah can read `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`,
`format.schema.diff`, and supported `diff` policy from `atlas.hcl`. The
current implementation does not execute Atlas's dev-database simulation; the
dev URL selects the dialect and hosts migration-directory replays.

```bash
ptah atlas schema diff \
  -f file://old.hcl \
  --to file://schema.hcl \
  --dev-url "sqlite://dev.db"
```

With an `old.hcl` declaring only a `users` table and a `schema.hcl` adding a
`posts` table, expected output includes:

```text
CREATE TABLE "main"."posts" (
  "id" INTEGER PRIMARY KEY,
  "user_id" INTEGER NOT NULL
);
```

`--format` accepts Atlas-style Go templates over Ptah's local diff report. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah atlas schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "sqlite://dev.db" \
  --format '{{ sql . "  " }}'
```

Unsupported schemes — `atlas://` registry URLs, `docker://` as a desired
state, and anything Ptah cannot connect to directly — fail during validation,
before any database is contacted. A migration-directory source without
`--dev-url` fails the same way. Non-Atlas-CE flags such as `--tx-mode` are rejected as unknown. `--exclude` and
disabled `schema.mode` values filter both local `--from` and `--to` schema files
before diffing, and `--schema`/`--include` positively scope both sides with the
same selection semantics, composition order, and cross-scope dependency
diagnostics as `schema apply` (see
[Scope the comparison with `--schema` and `--include`](#scope-the-comparison-with---schema-and---include)). A diff whose change needs a dialect-specific rebuild plan — for
example adding a column to a SQLite table — fails with an explicit error
instead of emitting SQL the dialect cannot run in place.

## Format schema files

`ptah atlas schema fmt` rewrites local `.hcl` files into HCL canonical layout:

```bash
ptah atlas schema fmt schema.hcl
```

## Clean a database

`ptah atlas schema clean` plans and applies destructive cleanup of user-owned
schema objects. Preview first:

```bash
ptah atlas schema clean --url "$DATABASE_URL" --dry-run
```

Against a SQLite database containing one `users` table, expected output
includes:

```text
Planned cleanup changes: 1
- DROP TABLE IF EXISTS "users"
[DRY RUN] No changes were applied.
```

:::danger
Without `--dry-run`, cleanup drops the listed objects after confirmation
(`--auto-approve` skips the prompt). There is no undo.
:::

## Format template fields

| Command | Format data fields |
| --- | --- |
| `ptah atlas schema inspect --format` | `.Realm`, `.Schema`, `.MarshalHCL`, `.MarshalSQL`, `.MarshalJSON`, plus `hcl`, `sql`, `json`, `base64url`, `mermaid`, `split`, and `write` template helpers. |
| `ptah atlas schema apply --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for the planned SQL statements. |
| `ptah atlas schema diff --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for generated migration SQL. |
| `ptah atlas schema clean --format` | `.Env.Driver`, `.Env.URL`, `.DryRun`, `.Applied`, `.Objects`, and `.Changes`. |

The shared report shape and URL redaction rules are described on the
[Atlas compatibility overview](../overview/#format-reports-and-redaction).

## Next steps

- Managing migration directories on this surface:
  [Atlas migrate commands](../migrate-commands/).
- Doing direct changes with a native-first flow:
  [Apply schema changes directly](../../direct/apply/).
- Checking the supported `atlas.hcl` inputs these commands read:
  [Atlas project config](../project-config/).
