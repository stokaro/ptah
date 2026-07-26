# Native CLI Command Tree

Ptah has a native CLI surface in the `ptah` binary, plus Atlas-compatible
command surfaces outside the native tree:

- Native Ptah commands, owned by Ptah and documented here.
- Atlas-compatible commands under `ptah atlas <command> ...`.
- The separate `ptah-compat` binary, which exposes the Atlas-compatible tree at
  process root and can be copied or symlinked as `atlas`.

Do not add root-level Atlas spellings such as `ptah migrate apply` or
`ptah schema inspect` to the native `ptah` binary.

## Canonical Native Tree

The native tree uses Ptah-owned noun/verb groups. Ptah is pre-GA, so old
root-level command spellings are removed instead of preserved.

| Native command | Purpose |
| --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. |
| `ptah schema render` | Render desired schema SQL from Go, YAML, or HCL schema inputs. |
| `ptah schema compare` | Compare desired schema with a live database. |
| `ptah schema drift` | Check live database drift against desired schema. |
| `ptah schema export` | Export a schema to HCL, an OpenAPI 3.0 component schema, or a GraphQL SDL. |
| `ptah viz` | Render desired schema diagrams as Mermaid, DOT, or SVG. |
| `ptah db read` | Read schema from a live database. |
| `ptah db drop-all` | Drop all schema objects in a live database. |
| `ptah migrations plan` | Print migration SQL from desired/live schema differences. |
| `ptah migrations generate` | Generate migration files from desired/live schema differences. |
| `ptah migrations create` | Create empty migration files for manual SQL. |
| `ptah migrations import` | Convert another tool's migration directory to Ptah format. |
| `ptah migrations up` | Run pending migrations. |
| `ptah migrations down` | Roll back migrations. |
| `ptah migrations status` | Show migration status. |
| `ptah migrations baseline` | Record existing migrations as applied. |
| `ptah migrations checkpoint` | Squash history into a cumulative-schema checkpoint fresh databases bootstrap from. |
| `ptah migrations repair` | Repair migration revision metadata. |
| `ptah migrations hash` | Write or update migration directory integrity. |
| `ptah migrations validate` | Validate migration directory integrity and, optionally, SQL execution with `--dev-url`. |
| `ptah migrations edit` | Edit a migration's SQL (via `$EDITOR` or `--up-file`/`--down-file`) and rewrite the integrity file. |
| `ptah migrations rebase` | Move a migration to the end of history by re-timestamping it, and rewrite the integrity file. |
| `ptah migrations rm` | Delete a migration's up/down pair and rewrite the integrity file. |
| `ptah migrations lint` | Lint migration files. |
| `ptah sql lint` | Lint standalone SQL files. |
| `ptah seed` | Apply environment-scoped SQL seed files. |
| `ptah version` | Print Ptah build information. |

The schema-diff commands (`ptah schema render`, `ptah migrations generate`,
`ptah migrate`, `ptah compare`) emit `CREATE`/`ALTER`/`DROP SEQUENCE` for
standalone PostgreSQL sequences declared with `//migrator:schema:sequence`. See
[Sequences](./sequences.md).

The same commands emit `CREATE DOMAIN` / `CREATE TYPE … AS (…)` / `CREATE TYPE …
AS RANGE (…)` (and their drops) for PostgreSQL user-defined types declared with
`//migrator:schema:domain` / `:composite` / `:range`, and `read-db` introspects
them. See [User-defined types](./user_defined_types.md).

`ptah migrations up` evaluates any `-- +ptah check` pre-migration assertions in a
migration before applying its statements, aborting (non-zero) if a precondition
does not hold; `--skip-checks` is an emergency bypass. See
[Pre-migration checks](./pre-migration-checks.md).

`ptah migrations import` converts an existing migration directory from another
versioned-migration tool into Ptah's native format, preserving version order and
rewriting `ptah.sum`, so a team can adopt Ptah without hand-rewriting its
history. See [Importing migrations](./migrations-import.md).

`ptah migrations edit`, `rebase`, and `rm` safely maintain an existing migration
directory: `edit` changes a migration's SQL, `rebase` re-timestamps a migration
to the end of history so it applies after concurrently-merged work, and `rm`
deletes a migration. Each rewrites the integrity file (`ptah.sum` or `atlas.sum`)
atomically, so `ptah migrations validate` passes immediately afterward — no
separate `hash` step. All three take `--migrations-dir`, `--version`, and
`--dir-format`. To protect deployed history, they refuse to modify a migration
that is already applied in the database given by `--db-url` unless `--force` is
passed; without `--db-url` they warn that applied state could not be verified.
These are directory-maintenance commands Atlas keeps in its proprietary (Pro)
build; Ptah provides them natively and for free.

`ptah migrations lint` and `ptah sql lint` report findings by rule code, grouped
into families (`DS` data-safety, `PG`/`MY` dialect-specific, `TX` transaction,
and others). Alongside `DS`, the `CD` (constraint-deletion) family flags dropping
a constraint whose type is recoverable from the SQL — `CD101` foreign key,
`CD102` check, `CD103` primary key (all `SeverityError`). `DS105` remains the
fallback for the ANSI `DROP CONSTRAINT <name>` form, whose constraint type the
SQL does not encode, so a typed drop yields exactly one finding (its `CD` code,
never also `DS105`). Individual codes and whole families are disabled by code or
prefix (for example `CD`, or a single `CD101`) and re-severitied per code through
lint configuration, the same as every other family.

## Exit Codes

Canonical grouped commands inherit the exit-code contract of the implementation
they delegate to. See [CLI Exit Codes](exit_codes.md) for the command-by-command
matrix.
