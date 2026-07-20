# Native CLI Command Tree

Ptah has two CLI surfaces:

- Native Ptah commands, owned by Ptah and documented here.
- Atlas-compatible commands, reserved under `ptah atlas <command> ...`.

Do not add root-level Atlas spellings such as `ptah migrate apply` or
`ptah schema inspect`. Those paths belong to `ptah atlas migrate apply` and
`ptah atlas schema inspect`.

## Canonical Native Tree

The native tree uses Ptah-owned noun/verb groups:

| Native command | Purpose | Legacy compatibility command |
| --- | --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. | Same command; already canonical. |
| `ptah schema render` | Render desired schema SQL from Go, YAML, or Atlas HCL inputs. | `ptah generate` |
| `ptah schema compare` | Compare desired schema with a live database. | `ptah compare` |
| `ptah schema drift` | Check live database drift against desired schema. | `ptah drift` |
| `ptah schema export` | Export one schema source format to another. | Same command; already canonical. |
| `ptah db read` | Read schema from a live database. | `ptah read-db` |
| `ptah db drop-all` | Drop all schema objects in a live database. | `ptah drop-all` |
| `ptah migrations plan` | Print migration SQL from desired/live schema differences. | `ptah migrate` |
| `ptah migrations generate` | Generate migration files from desired/live schema differences. | `ptah migrate generate` |
| `ptah migrations create` | Create empty migration files for manual SQL. | `ptah migrate new` |
| `ptah migrations up` | Run pending migrations. | `ptah migrate-up` |
| `ptah migrations down` | Roll back migrations. | `ptah migrate-down` |
| `ptah migrations status` | Show migration status. | `ptah migrate-status` |
| `ptah migrations baseline` | Record existing migrations as applied. | `ptah migrate-baseline` |
| `ptah migrations repair` | Repair migration revision metadata. | `ptah migrate-repair` |
| `ptah migrations hash` | Write or update migration directory integrity. | `ptah migrate-hash` |
| `ptah migrations validate` | Validate migration directory integrity. | `ptah migrate-validate` |
| `ptah migrations lint` | Lint migration files. | `ptah lint` |
| `ptah sql lint` | Lint standalone SQL files. | Same command; already canonical. |
| `ptah seed` | Apply environment-scoped SQL seed files. | Same command; already canonical. |
| `ptah version` | Print Ptah build information. | Same command; already canonical. |

## Compatibility And Deprecation

Ptah is pre-GA, so the grouped native commands are the preferred command tree
for new scripts. The historical kebab-case commands remain available as
compatibility spellings during the pre-GA period, and they delegate to the same
implementation and exit-code contract as their canonical grouped equivalents.

Before a stable release, maintainers may choose to hide or remove legacy
spellings after documenting the change in release notes. Until then, tests cover
both the grouped command resolution and representative legacy paths.

## Exit Codes

Canonical grouped commands inherit the exit-code contract of the implementation
they delegate to. See [CLI Exit Codes](exit_codes.md) for the command-by-command
matrix.
