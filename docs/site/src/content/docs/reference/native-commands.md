---
title: Native commands
description: Complete verb table for the native ptah command tree.
---

This page lists every native `ptah` verb with its purpose. Native commands use
Ptah-owned spellings; Atlas aliases are intentionally absent from the `ptah`
binary. The Atlas-compatible surface — the separate `ptah-compat` drop-in
binary — has its own page: [Atlas-compatible commands](../atlas-commands/).
Use `ptah <command> --help` for the exact flag set in an installed binary.

## Desired schema: `ptah schema`

| Command | Purpose |
| --- | --- |
| `ptah schema render` | Render desired schema SQL from Go, YAML, HCL, SQL, or external-command inputs. |
| `ptah schema annotations` | Export Ptah Go annotation metadata as a JSON Schema document. |
| `ptah schema compare` | Compare desired schema with a live database. |
| `ptah schema drift` | Check live database drift against desired schema. |
| `ptah schema apply` | Apply a desired schema directly to a database, with an advisory lock, optional dev-database rehearsal, and interactive approval (`--auto-approve` for scripts). |
| `ptah schema plan` | Save the declarative apply plan as a fingerprinted local plan file; `ptah schema apply --plan` executes it only while the target still matches the recorded fingerprint. |
| `ptah schema inspect` | Inspect a live database, a schema file, or an Atlas-format migration directory as machine-clean HCL, SQL, or JSON; `--out-dir`/`--split` export files. |
| `ptah schema diff` | Diff two arbitrary schema states (files, database URLs, or migration directories) into migration SQL or JSON. |
| `ptah schema fmt` | Format HCL schema files canonically; `--check` is a no-write CI gate. |
| `ptah schema export` | Export a schema to HCL, an OpenAPI 3.0 component schema, or a GraphQL SDL. |
| `ptah schema push` | Publish a lossless canonical desired schema to an OCI registry. |
| `ptah schema pull` | Pull a canonical desired schema from an OCI registry. |
| `ptah schema test` | Apply a desired schema (from Go annotations) to a throwaway database and run declarative seed/SQL/assert YAML cases against it. |

## Migration lifecycle: `ptah migrations`

| Command | Purpose |
| --- | --- |
| `ptah migrations plan` | Print migration SQL from desired/live schema differences. |
| `ptah migrations generate` | Generate migration files from desired/live schema differences; `--replay` derives the current state by replaying the directory on a `--dev-url` database instead of introspecting `--db-url`. |
| `ptah migrations create` | Create empty migration files for manual SQL; `--edit` opens them in `$VISUAL`/`$EDITOR` (or `--editor`) and refreshes `atlas.sum` for Atlas-format directories. |
| `ptah migrations import` | Convert a golang-migrate, Goose, Flyway, Liquibase, or dbmate migration directory into Ptah's native format, auto-detecting the source tool unless `--from` is set. |
| `ptah migrations baseline` | Record existing migrations as applied in the revision table without executing their SQL; `--shadow-db` verifies the baselined history reproduces the target schema. |
| `ptah migrations set` | Move the revision boundary to an arbitrary version in both directions without executing SQL: everything through `--version` is recorded applied, rows above it are removed. |
| `ptah migrations up` | Run pending migrations; `--limit N` applies only the first N, and `--allow-dirty` is the explicit recovery escape hatch past a dirty revision row. |
| `ptah migrations down` | Roll back migrations. |
| `ptah migrations status` | Show migration status. |
| `ptah migrations repair` | Repair migration revision metadata after a dirty or partial migration state; `--resume-from` executes the remaining up statements before marking the version applied. |
| `ptah migrations hash` | Write or update migration-directory integrity. |
| `ptah migrations validate` | Validate migration-directory integrity and, optionally, SQL execution by cleaning and replaying migrations on `--dev-url`. |
| `ptah migrations lint` | Lint migration files and, with `--dev-url`, clean and replay migrations on a directly connectable dev database before static reporting. |
| `ptah migrations test` | Run declarative YAML cases with migrate/apply-schema/seed/SQL/assert steps against a throwaway database, exiting non-zero on any failure. |
| `ptah migrations edit` | Edit a migration's SQL (via `$EDITOR` or `--up-file`/`--down-file`) and rewrite the integrity file, refusing already-applied migrations unless `--force`. |
| `ptah migrations rebase` | Move a migration to the end of history by re-timestamping it, and rewrite the integrity file, refusing already-applied migrations unless `--force`. |
| `ptah migrations rm` | Delete a migration's up/down pair and rewrite the integrity file, refusing already-applied migrations unless `--force`. |
| `ptah migrations checkpoint` | Squash migration history into a cumulative-schema checkpoint pair by replaying the directory on a `--shadow-db`. |
| `ptah migrations data` | Generate a reversible data migration from declarative reference-data drift against a live database. |
| `ptah migrations push` | Publish a migration directory to an OCI registry. |
| `ptah migrations pull` | Pull and reconstruct a migration directory from an OCI registry. |

## Live databases: `ptah db`

| Command | Purpose |
| --- | --- |
| `ptah db read` | Read schema from a live database. |
| `ptah db drop-all` | Drop all schema objects in a live database. |

## Registries and SQL files: `ptah oci`, `ptah sql`

| Command | Purpose |
| --- | --- |
| `ptah oci referrers` | List direct referrer metadata attached to an OCI artifact. |
| `ptah sql lint` | Lint standalone SQL files. |

## Top-level verbs

| Command | Purpose |
| --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. |
| `ptah seed` | Apply environment-scoped SQL seed files. |
| `ptah viz` | Render desired schema diagrams as Mermaid, DOT, or SVG. |
| `ptah version` | Print Ptah build information. |
| `ptah license` | Print license, copyright, and Atlas-compatibility attribution. |
| `ptah completion <shell>` | Generate shell completion output for the native `ptah` command tree. |

## OCI transport behavior

Native `migrations up`, `status`, and `down` accept `oci://` through
`--migrations-dir`. `migrations lint` accepts an OCI `--dir` and can attach its
canonical report with `--attach`. Native `schema compare`, `drift`, and
`migrations plan` accept an OCI desired-schema artifact through
`--schema-file`; a plan with exactly one OCI schema source can attach its
canonical safety report. Use digest pins for reproducible runs and reserve
`--plain-http` for an explicitly trusted local registry. See [OCI registry
artifacts](../../operate/oci-registry/).

`ptah oci referrers <oci-reference>` lists direct attachment descriptors.
`--type` accepts `all`, `lint`, `plan`, or `deployment`; `--format` accepts
`text` or `json`. Unqualified subjects resolve to `:latest`, tags resolve to
their current manifest, and digest subjects remain immutable. Docker
credentials and HTTPS are the defaults; `--plain-http` is only for an
explicitly trusted local registry. The command lists metadata, not attachment
payload contents.

This does not implement the Atlas Cloud command paths. The Atlas-compatible
`migrate push` and `schema push` remain Atlas community-edition boundary
stubs in the `ptah-compat` binary, and Atlas-compatible apply commands do not
expose the native OCI transport flags.

## External desired-schema inputs

The native desired-schema consumers — `schema render`, `schema compare`,
`schema drift`, `migrations plan`, and `migrations generate` — accept external
commands that emit SQL, HCL, or YAML. Each command also reads
`ptah.yaml external_schema` when `--schema-cmd` is not set and
`--allow-external-schema` explicitly permits config-sourced execution. See
[Configuration](../configuration/) for the `external_schema` block.
