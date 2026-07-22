---
title: Atlas-compatible CLI
description: Use Atlas-style commands through ptah atlas or the ptah-compat binary.
---

Atlas-compatible command paths are available in two forms:

- `ptah atlas <command> ...` inside the native Ptah CLI tree.
- `ptah-compat <command> ...` as a binary-level Atlas-compatible entry point.

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
| `atlas.hcl` env | Project config IR |
| Atlas revision table mode | Ptah revision format and table settings |

## Migration commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas migrate apply` | Atlas-format apply path equivalent to `ptah migrations up` |
| `ptah atlas migrate down` | Forwards to `ptah migrations down`; maps compatible Atlas flags and fails explicitly for dynamic down-planning and output-format flags that native Ptah does not implement yet. |
| `ptah atlas migrate status` | `ptah migrations status` |
| `ptah atlas migrate hash` | `ptah migrations hash` |
| `ptah atlas migrate validate` | `ptah migrations validate` |
| `ptah atlas migrate lint` | `ptah migrations lint`; supports Atlas-style `--latest N` for latest-version linting. |
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
| `ptah atlas schema inspect` | Inspects a live database and writes Atlas-shaped HCL by default, SQL with `--format sql` / `--format '{{ sql . }}'`, JSON with `--format json` / `--format '{{ json . }}'`, or custom Go-template output. |
| `ptah atlas schema apply` | Applies local desired schema files to a live database through Ptah schema diff and migration execution. |
| `ptah atlas schema diff` | Local `file://` schema-file diff for `.hcl`, `.yaml`, `.yml`, and `.sql` sources. |
| `ptah atlas schema fmt` | Formats local `.hcl` files using HCL canonical layout. |

`ptah atlas schema inspect` accepts a live database `--url` and writes
machine-oriented schema output without native Ptah status banners. The default
format is Atlas HCL.

```bash
ptah atlas schema inspect --url "$DATABASE_URL" > schema.hcl
ptah atlas schema inspect --url "$DATABASE_URL" --format sql > schema.sql
ptah atlas schema inspect --url "$DATABASE_URL" --format json > schema.json
```

`--schema` narrows inspection when the underlying database reader supports
schema scoping. `--dev-url` validates dialect compatibility only today; Ptah
does not yet run Atlas dev-database inference for inspection. `--format`
accepts Atlas-style Go templates with `.MarshalHCL`, `sql`, `json`,
`base64url`, and `mermaid`. Split/write templates, include/exclude filters, and
file-backed inspection remain explicit gaps.

`ptah atlas schema apply` accepts one or more local `--to` schema file URLs and
a live database `--url`. Ptah reads the current database schema, diffs it
against the desired local schema files, prints the planned SQL, and applies it
after interactive confirmation. Use `--dry-run` to print the plan without
applying it, or `--auto-approve` to skip the prompt explicitly. Use
`--tx-mode=file` or `--tx-mode=all` to execute the generated plan in one
transaction, or `--tx-mode=none` to execute statements without transaction
wrapping.

```bash
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
```

`--dev-url` is accepted for dialect validation only in this path today. It must
match the target database dialect; Ptah does not yet execute Atlas's
dev-database simulation for declarative apply.

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
while the command validates checksums and writes the new migration. Database
desired-state URLs, `env://` project attributes, schema filters, Docker dev
databases, and `--format` templates fail explicitly until their semantics are
implemented.

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

ptah atlas migrate lint --dir ./migrations --latest 1
```

For binary-level drop-in usage:

```bash
ptah-compat migrate apply \
  --url "$DATABASE_URL" \
  --dir ./migrations

ptah-compat schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
ptah-compat schema inspect --url "$DATABASE_URL"
ptah-compat schema fmt schema.hcl
ptah-compat migrate import \
  --from "file://goose?format=goose" \
  --to "file://migrations"
```

For existing scripts that already call `atlas`, install or copy `ptah-compat`
under that executable name:

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
ptah atlas migrate validate --dir ./migrations
ptah atlas migrate status --url "$DATABASE_URL" --dir ./migrations
```

When converting scripts, keep the `atlas` namespace in the Ptah command:

| Do | Do not |
| --- | --- |
| `ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations` | `ptah migrate apply --url "$DATABASE_URL" --dir ./migrations` |
| `ptah atlas schema inspect --url "$DATABASE_URL"` | `ptah schema inspect --url "$DATABASE_URL"` |

When replacing an existing Atlas binary in scripts, use the compatibility binary
instead of adding root-level Atlas spellings to `ptah`:

```bash
ptah-compat migrate apply --url "$DATABASE_URL" --dir ./migrations

# Or install/copy ptah-compat as "atlas" for existing scripts.
atlas schema apply --url "$DATABASE_URL" --to file://schema.sql --dry-run
atlas schema inspect --url "$DATABASE_URL"
```

## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the comparison gap register support that claim. Use
[Conformance](../../operate/conformance/) for current evidence and
[Comparison](../../reference/comparison/) for tracked product, coverage, and
documentation gaps.
