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

Implemented Atlas-compatible commands translate Atlas-style flags into Ptah's
native command model and then delegate to native behavior. Unsupported flags
fail clearly instead of being ignored.

| Atlas flag style | Native Ptah concept |
| --- | --- |
| `--url` | `--db-url` |
| `--dir` | `--migrations-dir` |
| `atlas.hcl` env | Project config IR |
| Atlas revision table mode | Ptah revision format and table settings |

## Migration commands

| Atlas-compatible command | Native Ptah command |
| --- | --- |
| `ptah atlas migrate apply` | `ptah migrations up` |
| `ptah atlas migrate down` | Forwards to `ptah migrations down`; maps compatible Atlas flags and fails explicitly for dynamic down-planning and output-format flags that native Ptah does not implement yet. |
| `ptah atlas migrate status` | `ptah migrations status` |
| `ptah atlas migrate hash` | `ptah migrations hash` |
| `ptah atlas migrate validate` | `ptah migrations validate` |
| `ptah atlas migrate lint` | `ptah migrations lint`; supports Atlas-style `--latest N` for latest-version linting. |
| `ptah atlas migrate new` | `ptah migrations create` |
| `ptah atlas migrate set` | `ptah migrations repair` |
| `ptah atlas migrate diff` | Command path registered; runtime behavior is not implemented yet. |
| `ptah atlas migrate import` | Imports local `file://` migration directories from Atlas-supported formats into a separate Atlas single-file directory and writes `atlas.sum`. |

## Utility commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas version` | Prints Ptah build information. |
| `ptah atlas license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |

## Schema commands

| Atlas-compatible command | Native Ptah command |
| --- | --- |
| `ptah atlas schema inspect` | `ptah db read` |
| `ptah atlas schema diff` | Local `file://` schema-file diff for `.hcl`, `.yaml`, `.yml`, and `.sql` sources. |
| `ptah atlas schema fmt` | Formats local `.hcl` files using HCL canonical layout. |

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

Remote database URLs, migration directory URLs, `env://` project attributes,
include/exclude filters, Atlas Cloud web output, and `--format` templates fail
explicitly until their semantics are implemented.

## Example

```bash
ptah atlas migrate apply \
  --url "$DATABASE_URL" \
  --dir ./migrations

ptah atlas schema inspect --url "$DATABASE_URL"
ptah atlas schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "postgres://localhost/dev"
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

Ptah translates implemented Atlas-style flags and then delegates to native behavior. Unsupported Atlas flags should fail clearly instead of being ignored.

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

When replacing an existing Atlas binary in scripts, use the compatibility
binary instead:

| Do | Do not |
| --- | --- |
| `ptah-compat migrate apply --url "$DATABASE_URL" --dir ./migrations` | `ptah migrate apply --url "$DATABASE_URL" --dir ./migrations` |
| `atlas schema inspect --url "$DATABASE_URL"` where `atlas` is `ptah-compat` renamed or symlinked | `ptah schema inspect --url "$DATABASE_URL"` |

## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the comparison gap register support that claim. Use
[Conformance](../../operate/conformance/) for current evidence and
[Comparison](../../reference/comparison/) for tracked product, coverage, and
documentation gaps.
