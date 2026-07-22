---
title: Atlas-compatible CLI
description: Use Atlas-style commands through ptah atlas.
---

Atlas-compatible command paths live under `ptah atlas <command> ...` inside the
native Ptah CLI tree.

The separate `ptah-compat` binary is a binary-level drop-in replacement for
scripts that need Atlas-style root commands, including scripts that call an
executable named `atlas`.

Ptah does not add root-level Atlas spellings such as `ptah migrate apply` or
`ptah schema inspect` to the native `ptah` binary. Those paths are intentionally
invalid because the native Ptah command tree is being designed separately before
GA. Use `ptah-compat` or a copied/symlinked executable named `atlas` when
existing scripts expect Atlas-style root commands.

## Translation model

Implemented Atlas-compatible commands either execute dedicated Atlas-shaped
behavior or translate Atlas-style flags into the closest native Ptah command
model. Unsupported flags fail clearly instead of being ignored.

| Atlas flag style | Native Ptah concept |
| --- | --- |
| `--url` | `--db-url` |
| `--dir` | `--migrations-dir` |
| `atlas.hcl` env | Project config IR, including `schema apply --env` defaults |
| Atlas revision table mode | Ptah revision format and table settings |

## Migration commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas migrate apply` | Atlas-format apply path equivalent to `ptah migrations up` |
| `ptah atlas migrate down` | Forwards to `ptah migrations down`; maps compatible Atlas flags and fails explicitly for dynamic down-planning and output-format flags that native Ptah does not implement yet. |
| `ptah atlas migrate status` | `ptah migrations status` |
| `ptah atlas migrate hash` | `ptah migrations hash` |
| `ptah atlas migrate validate` | Verifies `ptah.sum` or `atlas.sum`; with `--dev-url`, cleans and replays migrations on the dev database to validate SQL execution. |
| `ptah atlas migrate lint` | `ptah migrations lint`; supports Atlas-style `--latest N`, infers lint dialect from `--dev-url`, and cleans and replays migrations on directly connectable dev databases to validate SQL execution. |
| `ptah atlas migrate new` | `ptah migrations create` |
| `ptah atlas migrate set` | `ptah migrations repair` |
| `ptah atlas migrate diff` | Replays local Atlas migrations on `--dev-url`, diffs against local schema files, writes an Atlas single-file migration, and updates `atlas.sum`. |
| `ptah atlas migrate import` | Imports local `file://` migration directories from Atlas-supported formats into a separate Atlas single-file directory and writes `atlas.sum`. |

## Utility commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas version` | Prints Ptah build information. |
| `ptah atlas license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |

## Schema commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas schema inspect` | Inspects a live database and writes Atlas-shaped HCL by default, SQL with `--format sql` / `--format '{{ sql . }}'`, JSON with `--format json` / `--format '{{ json . }}'`, custom Go-template output, or basic `hcl`/`sql` split-write exports. The OSS `--exclude` flag filters inspected resources. |
| `ptah atlas schema apply` | Applies local desired schema files to a live database through Ptah schema diff and migration execution; supports `--env` project defaults and Atlas-style `--format` templates over the planned changes. |
| `ptah atlas schema diff` | Local `file://` schema-file diff for `.hcl`, `.yaml`, `.yml`, and `.sql` sources. |
| `ptah atlas schema fmt` | Formats local `.hcl` files using HCL canonical layout. |

`ptah atlas schema inspect` accepts a live database `--url` and writes
machine-oriented schema output without native Ptah status banners. The default
format is Atlas-compatible HCL.

```bash
ptah atlas schema inspect --url "$DATABASE_URL" > schema.hcl
ptah atlas schema inspect --url "$DATABASE_URL" --format sql > schema.sql
ptah atlas schema inspect --url "$DATABASE_URL" --format json > schema.json
```

`--schema` narrows inspection when the underlying database reader supports
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
yet. `--include` is an Atlas Pro feature and is outside Ptah's OSS drop-in
target. File-backed inspection, exporter blocks, and advanced split/write
configuration remain explicit gaps.

`ptah atlas schema apply` accepts one or more local `--to` schema file URLs and
a live database `--url`. With `--env`, Ptah can read `env.url`, `env.src`, and
`env.dev` from the selected `atlas.hcl` environment; explicit CLI flags still
take precedence. Ptah reads the current database schema, diffs it against the
desired local schema files, prints the planned SQL, and applies it after
interactive confirmation. Use `--dry-run` to print the plan without applying it,
or `--auto-approve` to skip the prompt explicitly. Use `--tx-mode=file` or
`--tx-mode=all` to execute the generated plan in one transaction, or
`--tx-mode=none` to execute statements without transaction wrapping.

```bash
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
```

```hcl
env "local" {
  url = "sqlite://app.db"
  src = "schema.sql"
  dev = "sqlite://dev.db"
}
```

```bash
ptah atlas schema apply --env local --dry-run
```

`--dev-url` is accepted for dialect validation only in this path today. It must
match the target database dialect; Ptah does not yet execute Atlas's
dev-database simulation for declarative apply.

`--format` accepts Atlas-style Go templates over the planned apply changes. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run \
  --format '{{ sql . "  " }}'
```

`ptah atlas schema diff` accepts one or more `--from` and `--to` local schema
file URLs and requires `--dev-url` so Ptah can choose the SQL dialect. The
current implementation does not execute Atlas's dev-database simulation; it
uses the dev URL for dialect selection only.

```bash
ptah atlas schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "postgres://localhost/dev"
```

`--format` accepts Atlas-style Go templates over Ptah's local diff report. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah atlas schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "postgres://localhost/dev" \
  --format '{{ sql . "  " }}'
```

Remote database URLs, migration directory URLs, `env://` project attributes,
include/exclude filters, Atlas Cloud web output, transaction-mode flags, and
lock flags fail explicitly until their semantics are implemented.

## Migration Apply

`ptah atlas migrate apply` reads a local Atlas migration directory and records
runtime history in Atlas revision-table format by default. The optional
positional `amount` applies only the first N pending migrations. Use
`--to-version` to apply up to a specific migration version, and `--baseline` to
mark earlier migration files as applied without executing their SQL bodies
before applying the remaining pending migrations.

```bash
ptah atlas migrate apply 2 \
  --url "$DATABASE_URL" \
  --dir file://migrations

ptah atlas migrate apply \
  --url "$DATABASE_URL" \
  --dir file://migrations \
  --to-version 20260722093000
```

Supported Atlas apply flags include `--dry-run`, `--tx-mode`, `--exec-order`,
`--allow-dirty`, `--baseline`, `--revisions-schema`, `--lock-timeout`, and
`--lock-name`. `--lock-name` changes the session-level advisory lock name used
by databases that support migration locks. `--format` executes a Go template
against a Ptah apply result that mirrors Atlas's public apply-template fields:
`Pending`, `Applied`, `Current`, `Target`, `Start`, `End`, `Driver`, `URL`, and
`Dir`; `{{ json . }}` emits the same result as JSON with database credentials
redacted.

## Migration Diff

`ptah atlas migrate diff` accepts a local `--dir` migration directory, one or
more local `--to` schema files, and a directly connectable `--dev-url`. Ptah
drops all tables in the dev database, replays the migration directory into it,
compares that state to the desired schema files, and writes an Atlas-style
single `.sql` migration plus `atlas.sum` when changes exist. Use a disposable
dev database. If `atlas.sum` already exists, Ptah validates it before replaying
migrations and fails on checksum drift instead of silently rehashing edited
files.

```bash
ptah atlas migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db"
```

Use `--lock-timeout` to bound waiting for Ptah's local migration-directory lock
while the command validates checksums and writes the new migration. The default
migration-file format matches Atlas's two-space SQL indentation template. Use
`--format` to render the generated migration SQL through Atlas-style Go
templates with `sql` and `.MarshalSQL`, for example to disable indentation:

```bash
ptah atlas migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db" \
  --format '{{ sql . "" }}'
```

`--schema` accepts repeated or comma-separated schema names and narrows the
replayed dev database state plus local desired schema files before the diff is
planned. Database desired-state URLs, `env://` project attributes, and Docker
dev databases fail explicitly until their semantics are implemented.

## Migration Validate

`ptah atlas migrate validate` verifies the migration directory against
`atlas.sum` or `ptah.sum`. When `--dev-url` is set, Ptah first checks integrity
and then treats the dev database as scratch space: it drops user tables and
replays the migration directory to validate SQL execution semantics. If
integrity drift is found, Ptah reports the drift and does not connect to the dev
database.

```bash
ptah atlas migrate validate \
  --dir file://migrations \
  --dir-format atlas \
  --dev-url "sqlite://dev.db"
```

## Example

```bash
ptah atlas migrate apply \
  --url "$DATABASE_URL" \
  --dir ./migrations

ptah atlas schema inspect --url "$DATABASE_URL"
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
ptah atlas schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "postgres://localhost/dev"
ptah atlas migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db"
ptah atlas schema fmt schema.hcl
ptah atlas migrate import \
  --from "file://flyway?format=flyway" \
  --to "file://migrations"

ptah atlas migrate lint \
  --dir ./migrations \
  --dev-url "sqlite://dev.db" \
  --latest 1
```

`migrate lint --dev-url` treats the dev database as scratch space: it drops user
tables, replays the migration directory, and then runs static lint
reporting. Docker `--dev-url` values remain an explicit gap; use a directly
connectable database URL.

For existing scripts that already call `atlas`, install or copy the
`ptah-compat` drop-in replacement under that executable name:

```bash
install_dir="$(go env GOPATH)/bin"
ln -sf "$(command -v ptah-compat)" "$install_dir/atlas"
atlas migrate apply --url "$DATABASE_URL" --dir ./migrations
```

Ptah translates or implements supported Atlas-style flags. Unsupported Atlas
flags should fail clearly instead of being ignored.

`ptah atlas migrate import` is intentionally fail-closed: use a destination
directory different from the source directory, and start with a destination that
does not already contain `.sql` migration files or `atlas.sum`.
Flyway repeatable migrations currently fail explicitly because Ptah does not yet
execute Atlas R-suffixed imported migrations.

## Check before migration

```bash
ptah atlas migrate hash --dir ./migrations
ptah atlas migrate validate --dir ./migrations --dev-url "sqlite://dev.db"
ptah atlas migrate status --url "$DATABASE_URL" --dir ./migrations
```

When converting scripts, keep the `atlas` namespace in the Ptah command:

| Do | Do not |
| --- | --- |
| `ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations` | `ptah migrate apply --url "$DATABASE_URL" --dir ./migrations` |
| `ptah atlas schema inspect --url "$DATABASE_URL"` | `ptah schema inspect --url "$DATABASE_URL"` |

When replacing an existing Atlas binary in scripts, use the `ptah-compat`
drop-in replacement instead of adding root-level Atlas spellings to `ptah`:

```bash
atlas schema apply --url "$DATABASE_URL" --to file://schema.sql --dry-run
atlas schema inspect --url "$DATABASE_URL"
```

## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the comparison gap register support that claim. Use
[Conformance](../../operate/conformance/) for current evidence and
[Comparison](../../reference/comparison/) for tracked product, coverage, and
documentation gaps.
