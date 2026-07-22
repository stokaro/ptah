---
title: Commands
description: Native and Atlas-compatible Ptah command surfaces.
---

Ptah has two command surfaces:

- Native Ptah commands, owned by Ptah.
- Atlas-compatible commands, reserved under `ptah atlas <command> ...` in the
  native binary and under `ptah-compat <command> ...` in the compatibility
  binary.

Reference: [native CLI command tree](https://github.com/stokaro/ptah/blob/master/docs/native_cli.md).

## Native commands

| Command | Purpose |
| --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. |
| `ptah schema render` | Render desired schema SQL from Go, YAML, or Atlas HCL inputs. |
| `ptah schema annotations` | Export Ptah Go annotation metadata. |
| `ptah schema compare` | Compare desired schema with a live database. |
| `ptah schema drift` | Check live database drift against desired schema. |
| `ptah schema export` | Export one schema source format to another. |
| `ptah viz` | Render desired schema diagrams as Mermaid, DOT, or SVG. |
| `ptah db read` | Read schema from a live database. |
| `ptah db drop-all` | Drop all schema objects in a live database. |
| `ptah migrations plan` | Print migration SQL from desired/live schema differences. |
| `ptah migrations generate` | Generate migration files from desired/live schema differences. |
| `ptah migrations create` | Create empty migration files for manual SQL. |
| `ptah migrations up` | Run pending migrations. |
| `ptah migrations down` | Roll back migrations. |
| `ptah migrations status` | Show migration status. |
| `ptah migrations hash` | Write or update migration-directory integrity. |
| `ptah migrations validate` | Validate migration-directory integrity. |
| `ptah migrations lint` | Lint migration files. |
| `ptah sql lint` | Lint standalone SQL files. |
| `ptah seed` | Apply environment-scoped SQL seed files. |
| `ptah version` | Print Ptah build information. |

## Atlas-compatible commands

Use `ptah atlas ...` in the native `ptah` binary. Root-level Atlas aliases are
intentionally absent from that binary.

```bash
ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations
ptah atlas migrate status --url "$DATABASE_URL" --dir ./migrations
ptah atlas schema apply --url "$DATABASE_URL" --to file://schema.sql --dry-run
ptah atlas schema inspect --url "$DATABASE_URL"
```

Use `ptah-compat ...` when a script needs Atlas-style root commands:

```bash
ptah-compat schema apply --url "$DATABASE_URL" --to file://schema.sql --dry-run
ptah-compat migrate apply --url "$DATABASE_URL" --dir ./migrations
ptah-compat migrate status --url "$DATABASE_URL" --dir ./migrations
ptah-compat schema inspect --url "$DATABASE_URL"
```

If `ptah-compat` is copied or symlinked as `atlas`, usage and help paths are
rendered as `atlas <command> ...` where Cobra can derive them from the
executable name.

| Command | Current status |
| --- | --- |
| `ptah atlas version` | Prints Ptah build information. |
| `ptah atlas license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |
| `ptah atlas migrate apply` | Forwards to `ptah migrations up`. |
| `ptah atlas migrate status` | Forwards to `ptah migrations status`. |
| `ptah atlas migrate hash` | Forwards to `ptah migrations hash`. |
| `ptah atlas migrate validate` | Forwards to `ptah migrations validate`. |
| `ptah atlas migrate lint` | Forwards to `ptah migrations lint`; maps `--latest N` to native latest-version linting. |
| `ptah atlas migrate new` | Forwards to `ptah migrations create`. |
| `ptah atlas migrate set` | Forwards to `ptah migrations repair`. |
| `ptah atlas migrate down` | Forwards to `ptah migrations down`; maps compatible Atlas flags and fails explicitly for dynamic down-planning and output-format flags that native Ptah does not implement yet. |
| `ptah atlas migrate diff` | Validates an existing `atlas.sum`, replays a local Atlas migration directory on `--dev-url`, diffs it against local `.hcl`, `.yaml`, `.yml`, or `.sql` `--to` schema files, writes a new Atlas single-file migration, and updates `atlas.sum`. Database desired-state URLs, `env://`, schema filters, lock flags, Docker dev databases, and `--format` templates remain explicit gaps. |
| `ptah atlas migrate import` | Imports local `file://` migration directories from `atlas`, `golang-migrate`, `goose`, `flyway`, `liquibase`, or `dbmate` format into a separate Atlas single-file directory and writes `atlas.sum`. Flyway repeatable migrations fail explicitly until Ptah can execute Atlas R-suffixed imported migrations. |
| `ptah atlas schema inspect` | Forwards to `ptah db read`. |
| `ptah atlas schema apply` | Diffs a live database against local `file://` `.hcl`, `.yaml`, `.yml`, or `.sql` desired schema files, prints the planned SQL, and applies it after interactive confirmation or explicit `--auto-approve`. `--dry-run` prints the plan without applying. Database desired-state URLs, migration directories, `env://`, include/exclude filters, custom format templates, lock flags, and Atlas dev-database simulation remain explicit gaps. |
| `ptah atlas schema diff` | Diffs local `file://` schema files with `.hcl`, `.yaml`, `.yml`, or `.sql` extensions and prints migration SQL. Database URLs, migration directories, `env://`, include/exclude filters, web output, and custom format templates remain explicit gaps. |
| `ptah atlas schema fmt` | Formats local `.hcl` files using HCL canonical layout. |

Run `ptah <command> --help` or `ptah atlas <command> --help` for exact flags in
the version you are using. Run `ptah-compat <command> --help` for the same
Atlas-compatible command tree at process root.
