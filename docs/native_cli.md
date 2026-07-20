# Native CLI Command Tree

Ptah has two CLI surfaces:

- Native Ptah commands, owned by Ptah and documented here.
- Atlas-compatible commands, reserved under `ptah atlas <command> ...`.

Do not add root-level Atlas spellings such as `ptah migrate apply` or
`ptah schema inspect`. Those paths belong to `ptah atlas migrate apply` and
`ptah atlas schema inspect`.

## Canonical Native Tree

The native tree uses Ptah-owned noun/verb groups. Ptah is pre-GA, so old
root-level command spellings are removed instead of preserved.

| Native command | Purpose |
| --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. |
| `ptah schema render` | Render desired schema SQL from Go, YAML, or Atlas HCL inputs. |
| `ptah schema compare` | Compare desired schema with a live database. |
| `ptah schema drift` | Check live database drift against desired schema. |
| `ptah schema export` | Export one schema source format to another. |
| `ptah db read` | Read schema from a live database. |
| `ptah db drop-all` | Drop all schema objects in a live database. |
| `ptah migrations plan` | Print migration SQL from desired/live schema differences. |
| `ptah migrations generate` | Generate migration files from desired/live schema differences. |
| `ptah migrations create` | Create empty migration files for manual SQL. |
| `ptah migrations up` | Run pending migrations. |
| `ptah migrations down` | Roll back migrations. |
| `ptah migrations status` | Show migration status. |
| `ptah migrations baseline` | Record existing migrations as applied. |
| `ptah migrations repair` | Repair migration revision metadata. |
| `ptah migrations hash` | Write or update migration directory integrity. |
| `ptah migrations validate` | Validate migration directory integrity. |
| `ptah migrations lint` | Lint migration files. |
| `ptah sql lint` | Lint standalone SQL files. |
| `ptah seed` | Apply environment-scoped SQL seed files. |
| `ptah version` | Print Ptah build information. |

## Exit Codes

Canonical grouped commands inherit the exit-code contract of the implementation
they delegate to. See [CLI Exit Codes](exit_codes.md) for the command-by-command
matrix.
