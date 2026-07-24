---
title: Commands
description: Native and Atlas-compatible Ptah command surfaces.
---

Ptah has two command surfaces:

- Native Ptah commands, owned by Ptah.
- Atlas-compatible commands, reserved under `ptah atlas <command> ...` in the
  native binary.

The separate `ptah-compat` binary is a drop-in replacement for scripts that need
Atlas-style root commands, including scripts that call an executable named
`atlas`.

This page is the published command reference. Use `ptah <command> --help` for
the exact flag set in an installed binary.

## Native commands

| Command | Purpose |
| --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. |
| `ptah schema render` | Render desired schema SQL from Go, YAML, or HCL schema inputs. |
| `ptah schema annotations` | Export Ptah Go annotation metadata. |
| `ptah schema compare` | Compare desired schema with a live database. |
| `ptah schema drift` | Check live database drift against desired schema. |
| `ptah schema export` | Export a schema to HCL, an OpenAPI 3.0 component schema, or a GraphQL SDL. |
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
| `ptah migrations validate` | Validate migration-directory integrity and, optionally, SQL execution by cleaning and replaying migrations on `--dev-url`. |
| `ptah migrations lint` | Lint migration files and, with `--dev-url`, clean and replay migrations on a directly connectable dev database before static reporting. |
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

If `ptah-compat` is copied or symlinked as `atlas`, usage and help paths are
rendered as `atlas <command> ...` where Cobra can derive them from the
executable name.

| Command | Current status |
| --- | --- |
| `ptah atlas version` | Prints Ptah build information. |
| `ptah atlas license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |
| `ptah atlas migrate apply` | Applies Atlas-format migration directories with Atlas-compatible apply flags. With `--env`, reads `env.url`, `migration`, and `format.migrate.apply` from `atlas.hcl`. |
| `ptah atlas migrate status` | Reports Atlas-format migration status with Atlas revision-table metadata and Atlas-format migration directories by default; supports `--dir-format atlas`, `--revisions-schema`, and Atlas Go-template `--format` output over `.Env`, `.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, and `.Status`. |
| `ptah atlas migrate hash` | Forwards to `ptah migrations hash` with Atlas `--dir-format` defaulting to `atlas`, so the compatibility path writes `atlas.sum` by default. |
| `ptah atlas migrate validate` | Verifies `ptah.sum` or `atlas.sum`; with `--dev-url`, cleans the dev database and replays the migration directory to validate SQL execution. |
| `ptah atlas migrate lint` | Runs Ptah migration linting with Atlas `--dir-format` defaulting to `atlas`; maps `--latest N` and `--git-base/--git-dir` to native changeset linting, infers lint dialect from `--dev-url`, cleans and replays migrations on directly connectable dev databases, and supports Atlas Go-template `--format` output over `.Env`, `.Steps`, and `.Files`. Docker dev databases and web reports remain explicit gaps. |
| `ptah atlas migrate new` | Creates an Atlas single-file skeleton migration and updates `atlas.sum` by default; supports `--dir-format atlas`. |
| `ptah atlas migrate set <revision>` | Sets or rewrites the Atlas-format revision row for the positional revision by forwarding to `ptah migrations repair` with Atlas revision-table metadata and Atlas-format migration directories by default. With `--env`, reads `env.url`, `migration.dir`, and `migration.revisions_schema` from `atlas.hcl`; explicit `--dir`, `--url`, and `--revisions-schema` flags keep CLI precedence. |
| `ptah atlas migrate down` | Forwards to `ptah migrations down`; maps compatible Atlas flags and fails explicitly for dynamic down-planning and output-format flags that native Ptah does not implement yet. |
| `ptah atlas migrate diff` | Validates an existing `atlas.sum`, replays a local Atlas migration directory on `--dev-url`, diffs it against local `.hcl`, `.yaml`, `.yml`, or `.sql` `--to` schema files, writes a new Atlas single-file migration, updates `atlas.sum`, and supports `--lock-timeout` for Ptah's local migration-directory lock. The Atlas-hidden `--dry-run` flag prints the generated SQL instead of writing a migration file or `atlas.sum`. With `--env`, reads `env.schema.src`, `env.dev`, `migration.dir`, `format.migrate.diff`, and supported non-concurrent `diff` policy from `atlas.hcl`. Generated SQL uses Atlas-style two-space indentation by default; `--format` renders it with `sql` and `.MarshalSQL` templates. `--schema/-s` narrows both the replayed dev database state and local desired schema files to selected schema names. Database desired-state URLs, `env://`, Docker dev databases, and concurrent index migration-file metadata remain explicit gaps. |
| `ptah atlas migrate import` | Imports local `file://` migration directories from `atlas`, `golang-migrate`, `goose`, `flyway`, `liquibase`, or `dbmate` format into a separate Atlas single-file directory and writes `atlas.sum`. Flyway repeatable migrations fail explicitly until Ptah can execute Atlas R-suffixed imported migrations. |
| `ptah atlas schema inspect` | Inspects a live database and writes Atlas-compatible schema output without Ptah status banners. The default output is HCL; SQL output is supported with `--format sql` or `--format '{{ sql . }}'`; JSON and custom templates are supported through `--format json`, `{{ json . }}`, `{{ .MarshalHCL }}`, `{{ hcl . }}`, `{{ sql . }}`, and `{{ mermaid . }}`. Basic `{{ hcl . | split | write "schema" }}` and `{{ sql . | split | write "schema" }}` exports are supported. `--schema/-s` narrows inspection when supported by the database reader. The OSS `--exclude` flag filters inspected resources with Atlas-style globs and `[type=...]` selectors, including the Atlas-documented `*[type=extension].version` field selector. `--include` is Atlas Pro-only and outside Ptah's OSS drop-in target. Other field-level exclude selectors, file-backed inspection, advanced split/write configuration, and dev-database inference remain explicit gaps. |
| `ptah atlas schema apply` | Diffs a live database against local `file://` `.hcl`, `.yaml`, `.yml`, or `.sql` desired schema files, can read `env.url`, `env.src`, `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff` policy from `atlas.hcl` with `--env`, including local variable defaults, locals, `getenv`, `file`, `fileset`, `format`, `jsonencode`, and `data.hcl_schema.<name>.url` references, prints the planned SQL, and applies it after interactive confirmation or explicit `--auto-approve`. `--dry-run` prints the plan without applying. `--tx-mode=file` and `--tx-mode=all` execute the generated plan in one transaction; `--tx-mode=none` executes statements without transaction wrapping. `--format` supports Atlas-style templates over planned changes with `sql` and `.MarshalSQL`. `--schema/-s` is parsed for Atlas CLI compatibility but remains limited to future database-URL desired-state support; Atlas's hidden deprecated `--file/-f` alias is accepted for local HCL or SQL paths and mapped to the desired schema input. `--exclude` and disabled `schema.mode` values filter matching resources out of the current and desired local-file comparison before planning. Database desired-state URLs, migration directories, `env://` URL sources, include filters, lock flags, and Atlas dev-database simulation remain explicit gaps. |
| `ptah atlas schema diff` | Diffs local `file://` schema files with `.hcl`, `.yaml`, `.yml`, or `.sql` extensions, prints migration SQL, supports `--from/-f`, supports Atlas-style `--format` templates with `sql` and `.MarshalSQL`, and applies `--exclude` plus disabled `schema.mode` resource filters to both local inputs before diffing. `--schema/-s` is parsed for Atlas CLI compatibility but remains limited to future database-URL schema diff support. With `--env`, reads `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.diff`, and supported `diff` policy from `atlas.hcl`. Database URLs, migration directories, `env://`, include filters, and web output remain explicit gaps. |
| `ptah atlas schema fmt` | Formats local `.hcl` files using HCL canonical layout. |

Run `ptah <command> --help` or `ptah atlas <command> --help` for exact flags in
the version you are using.
