# CLI Exit Codes

Ptah treats CLI exit codes as a public scripting contract.

| Code | Meaning |
| --- | --- |
| `0` | Success. The command completed and did not find a configured failing condition. |
| `1` | Expected negative result. A check found drift, lint findings, integrity drift, pending migrations, or a non-empty diff when that behavior is enabled. |
| `2` | Command or usage error. Examples: bad flags, unknown commands, invalid input, connection failure, parse failure, unsupported dialect, unwritable output, or an internal panic recovered by the root command. |
| `3+` | Reserved. Do not rely on these codes until Ptah documents a specific use. |

## Native Commands

| Command | `0` | `1` | `2` |
| --- | --- | --- | --- |
| `ptah generate` | Schema rendered. | Not used. | Usage error, parse error, unsupported dialect, or render error. |
| `ptah schema export` | Schema exported. | Not used. | Usage error, invalid paths, parse error, render error, write error, or cleanup error. |
| `ptah read-db` | Schema read and printed. | Not used. | Usage error, connection failure, or schema-read failure. |
| `ptah compare` | Diff printed, or no diff. | Non-empty diff when `--exit-code` is set. | Usage error, connection failure, parse failure, or diff generation failure. |
| `ptah drift` | No drift that meets `--severity`, or `--exit-code=false`. | Drift meets `--severity` while `--exit-code=true`. | Usage error, connection failure, parse failure, or report error. |
| `ptah lint` | No findings above `--fail-on`, or `--fail-on=none`. | Findings meet `--fail-on`. | Usage error, invalid config, unreadable migration directory, or report error. |
| `ptah migrate` | Migration SQL generated, or no schema changes. | Not used. | Usage error, connection failure, parse failure, safety check failure, or render error. |
| `ptah migrate generate` | Migration file generated, or no migration needed. | Not used. | Usage error, connection failure, parse failure, shadow verification failure, safety check failure, or write error. |
| `ptah migrate new` | Empty migration files created. | Not used. | Usage error, invalid directory, or write error. |
| `ptah migrate-baseline` | Existing migrations recorded as applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, verification failure, or write error. |
| `ptah migrate-up` | Pending migrations applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, integrity verification failure, lint/safety gate failure, lock failure, or migration execution failure. |
| `ptah migrate-down` | Requested rollback applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, lock failure, or rollback failure. |
| `ptah migrate-repair` | Migration revision repaired, or dry-run output printed. | Not used. | Usage error, connection failure, revision lookup failure, or repair failure. |
| `ptah migrate-status` | Status printed, including pending migrations by default. | Pending migrations exist when `--exit-code` is set. | Usage error, connection failure, migration directory error, or status-read failure. |
| `ptah migrate-hash` | Integrity file written. | Not used. | Usage error, invalid directory, invalid migration format, or write error. |
| `ptah migrate-validate` | Integrity file matches the migration directory. | Migration content drift found. | Usage error, missing or unreadable integrity file, invalid directory, or invalid migration format. |
| `ptah seed` | Seed files applied or already applied. | Not used. | Usage error, protected environment rejection, connection failure, invalid seed files, or seed execution failure. |
| `ptah drop-all` | Objects dropped, dry-run output printed, or operation canceled by the user. | Not used. | Usage error, connection failure, input read error, or drop failure. |
| `ptah version` | Version information printed. | Not used. | Usage error. |

## Atlas-Compatible Aliases

Commands under `ptah atlas <command> ...` translate implemented Atlas-compatible flags and then delegate to the matching native command. Their exit codes therefore follow the native command contract:

| Atlas-compatible command | Native command |
| --- | --- |
| `ptah atlas migrate apply` | `ptah migrate-up` |
| `ptah atlas migrate down` | `ptah migrate-down` |
| `ptah atlas migrate status` | `ptah migrate-status` |
| `ptah atlas migrate hash` | `ptah migrate-hash` |
| `ptah atlas migrate validate` | `ptah migrate-validate` |
| `ptah atlas migrate lint` | `ptah lint` |
| `ptah atlas schema inspect` | `ptah read-db` |
| `ptah atlas schema diff` | `ptah compare` |

Unsupported Atlas-compatible flags are rejected explicitly and exit `2`.
