---
title: Exit codes
description: How Ptah commands behave when used as automation gates.
---

Ptah uses exit codes as a public scripting contract:

| Code | Meaning |
| --- | --- |
| `0` | Success. The command completed and did not find a configured failing condition. |
| `1` | Expected negative result. A check found drift, lint findings, integrity drift, pending migrations, or a non-empty diff when that behavior is enabled. |
| `2` | Command or usage error. Examples: bad flags, unknown commands, invalid input, connection failure, parse failure, unsupported dialect, unwritable output, or an internal panic recovered by the root command. |
| `3+` | Reserved. Do not rely on these codes until Ptah documents a specific use. |

Common gates:

```bash
ptah migrations validate --dir ./migrations
ptah migrations status --db-url "$DATABASE_URL" --migrations-dir ./migrations --exit-code
ptah migrations lint --dir ./migrations --dialect postgres
```

Do not collapse all non-zero outcomes into the same remediation. A `1` usually
means the command successfully found a condition you asked it to check; a `2`
means the command itself did not complete correctly.

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
| `ptah migrations lint` | No findings above `--fail-on`, or `--fail-on=none`. | Findings meet `--fail-on`. | Usage error, invalid config, unreadable migration directory, dev-database connection failure, SQL replay failure, or report error. |
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
| `ptah migrations validate` | Integrity file matches the migration directory, and optional `--dev-url` SQL replay succeeds. | Migration content drift found. | Usage error, missing or unreadable integrity file, invalid directory, invalid migration format, dev-database connection failure, or SQL replay failure. |
| `ptah seed` | Seed files applied or already applied. | Not used. | Usage error, protected environment rejection, connection failure, invalid seed files, or seed execution failure. |
| `ptah version` | Version information printed. | Not used. | Usage error. |

## Atlas-Compatible Command Surfaces

Commands under `ptah atlas <command> ...` either translate implemented
Atlas-compatible flags and delegate to the matching native command, or execute
Ptah-owned Atlas-shaped behavior such as migration apply, the license notice, or
schema formatting. The separate `ptah-compat` binary exposes the same
Atlas-compatible command tree at process root for drop-in script migration, so it
shares this exit-code contract.

| Atlas-compatible command | Behavior |
| --- | --- |
| `ptah atlas version` | `ptah version` |
| `ptah atlas license` | Ptah license notice |
| `ptah atlas migrate apply` | Atlas-format apply path equivalent to `ptah migrations up` |
| `ptah atlas migrate down` | `ptah migrations down` |
| `ptah atlas migrate diff` | Local Atlas-style migration diff, `atlas.sum` update, or dry-run output printed |
| `ptah atlas migrate import` | Import local migrations into a separate directory and write `atlas.sum` |
| `ptah atlas migrate status` | Atlas-format migration status with Atlas revision-table metadata |
| `ptah atlas migrate hash` | `ptah migrations hash` |
| `ptah atlas migrate validate` | `ptah migrations validate` |
| `ptah atlas migrate lint` | `ptah migrations lint` |
| `ptah atlas schema inspect` | Atlas-shaped schema inspection |
| `ptah atlas schema apply` | Local Atlas-style schema apply |
| `ptah atlas schema diff` | Local Atlas-style schema-file diff |
| `ptah atlas schema fmt` | Format local `.hcl` files |

Unsupported Atlas-compatible flags are rejected explicitly and exit `2`.

This page is checked against the repository exit-code contract during docs CI.
