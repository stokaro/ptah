# CLI Exit Codes

Ptah treats CLI exit codes as a public scripting contract.

| Code | Meaning |
| --- | --- |
| `0` | Success. The command completed and did not find a configured failing condition. |
| `1` | Expected negative result. A check found drift, lint findings, integrity drift, pending migrations, or a non-empty diff when that behavior is enabled. |
| `2` | Command or usage error. Examples: bad flags, unknown commands, invalid input, connection failure, parse failure, unsupported dialect, unwritable output, or an internal panic recovered by the root command. |
| `3+` | Reserved. Do not rely on these codes until Ptah documents a specific use. |

## Native Commands

The grouped command tree is the native Ptah surface. Ptah is pre-GA, so old
root-level command spellings are removed instead of preserved.

| Command | `0` | `1` | `2` |
| --- | --- | --- | --- |
| `ptah introspect` | Annotated Go model files generated. | Not used. | Usage error, invalid output path, connection failure, schema-read failure, render error, or write error. |
| `ptah schema render` | Schema rendered. | Not used. | Usage error, parse error, unsupported dialect, or render error. |
| `ptah schema export` | Schema exported. | Not used. | Usage error, invalid paths, parse error, render error, write error, or cleanup error. |
| `ptah viz` | Schema diagram rendered. | Not used. | Usage error, invalid paths, parse error, unsupported format/theme, missing Graphviz for SVG, SVG render error, or write error. |
| `ptah db read` | Schema read and printed. | Not used. | Usage error, connection failure, or schema-read failure. |
| `ptah db drop-all` | Objects dropped, dry-run output printed, or operation canceled by the user. | Not used. | Usage error, connection failure, input read error, or drop failure. |
| `ptah schema compare` | Diff printed, or no diff. | Non-empty diff when `--exit-code` is set. | Usage error, connection failure, parse failure, or diff generation failure. |
| `ptah schema drift` | No drift that meets `--severity`, or `--exit-code=false`. | Drift meets `--severity` while `--exit-code=true`. | Usage error, connection failure, parse failure, or report error. |
| `ptah migrations lint` | No findings above `--fail-on`, or `--fail-on=none`. | Findings meet `--fail-on`. | Usage error, invalid config, unreadable migration directory, or report error. |
| `ptah sql lint` | No SQL lint findings with `error` severity. | One or more SQL lint findings with `error` severity. | Usage error, unreadable SQL input, unsupported dialect, or report error. |
| `ptah migrations plan` | Migration SQL generated, or no schema changes. | Not used. | Usage error, connection failure, parse failure, safety check failure, or render error. |
| `ptah migrations generate` | Migration file generated, or no migration needed. | Not used. | Usage error, connection failure, parse failure, shadow verification failure, safety check failure, or write error. |
| `ptah migrations create` | Empty migration files created. | Not used. | Usage error, invalid directory, or write error. |
| `ptah migrations baseline` | Existing migrations recorded as applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, verification failure, or write error. |
| `ptah migrations up` | Pending migrations applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, integrity verification failure, lint/safety gate failure, pre-flight hook failure, lock failure, or migration execution failure. |
| `ptah migrations down` | Requested rollback applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, pre-flight hook failure, lock failure, or rollback failure. |
| `ptah migrations repair` | Migration revision repaired, or dry-run output printed. | Not used. | Usage error, connection failure, revision lookup failure, or repair failure. |
| `ptah migrations status` | Status printed, including pending migrations by default. | Pending migrations exist when `--exit-code` is set. | Usage error, connection failure, migration directory error, or status-read failure. |
| `ptah migrations hash` | Integrity file written. | Not used. | Usage error, invalid directory, invalid migration format, or write error. |
| `ptah migrations validate` | Integrity file matches the migration directory. | Migration content drift found. | Usage error, missing or unreadable integrity file, invalid directory, or invalid migration format. |
| `ptah seed` | Seed files applied or already applied. | Not used. | Usage error, protected environment rejection, connection failure, invalid seed files, or seed execution failure. |
| `ptah version` | Version information printed. | Not used. | Usage error. |

## Atlas-Compatible Command Surfaces

Commands under `ptah atlas <command> ...`, `ptah-compat <command> ...`, and a
copied or symlinked executable named `atlas` translate implemented
Atlas-compatible flags and then delegate to the matching native command. Their
exit codes therefore follow the native command contract:

| Atlas-compatible command | Native command |
| --- | --- |
| `ptah atlas migrate apply` | `ptah migrations up` |
| `ptah-compat migrate apply` / `atlas migrate apply` | `ptah migrations up` |
| `ptah atlas migrate down` | `ptah migrations down` |
| `ptah-compat migrate down` / `atlas migrate down` | `ptah migrations down` |
| `ptah atlas migrate status` | `ptah migrations status` |
| `ptah-compat migrate status` / `atlas migrate status` | `ptah migrations status` |
| `ptah atlas migrate hash` | `ptah migrations hash` |
| `ptah atlas migrate validate` | `ptah migrations validate` |
| `ptah atlas migrate lint` | `ptah migrations lint` |
| `ptah atlas schema inspect` | `ptah db read` |
| `ptah-compat schema inspect` / `atlas schema inspect` | `ptah db read` |
| `ptah atlas schema diff` | `ptah schema compare` |

Unsupported Atlas-compatible flags are rejected explicitly and exit `2`.
