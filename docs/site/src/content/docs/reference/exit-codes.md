---
title: Exit codes
description: How Ptah commands behave when used as automation gates.
type: reference
audience:
  - "all-users"
readerQuestion: "Which exit code should automation expect from each Ptah result?"
goal: "Map each Ptah result to the exit code automation should expect."
sourceOfTruth:
  - "docs/exit_codes.md"
  - "docs/site/scripts/check-exit-codes.mjs"
generated: false
searchAliases:
  - "exit code 2"
overlaps: []
disposition: keep
owns:
  - gopkg-core-ptaherr
---

Native Ptah commands use exit codes as a public scripting contract:

| Code | Meaning |
| --- | --- |
| `0` | Success. The command completed and did not find a configured failing condition. |
| `1` | Expected negative result. A check found drift, lint findings, integrity drift, pending migrations, or a non-empty diff when that behavior is enabled. |
| `2` | Command or usage error. Examples: bad flags, unknown commands, invalid input, connection failure, parse failure, unsupported dialect, unwritable output, or an internal panic recovered by the root command. |
| `128+N` | Interrupted. A signal canceled the command, which exited with the shell convention of 128 plus the signal number: `130` for SIGINT (Ctrl-C), `143` for SIGTERM. |
| `3+` | Reserved apart from the signal statuses above. Do not rely on these codes until Ptah documents a specific use. |

This four-level contract applies to native Ptah commands. The Atlas-compatible
surfaces intentionally use Atlas CE's narrower process contract: `0` for
success and help, and `1` for command, usage, validation, and runtime failures.
An internal panic recovered by Ptah's process boundary still exits `2`. An
interrupt is a runtime failure there as well, so a canceled `ptah-compat`
command exits `1` rather than reporting a signal status: a surface that
promises two codes does not grow a third one because the operator pressed
Ctrl-C.

Common gates:

```bash
ptah migrations validate --dir ./migrations
ptah migrations status --db-url "$DATABASE_URL" --migrations-dir ./migrations --exit-code
ptah migrations lint --dir ./migrations --dialect postgres
```

For native Ptah commands, do not collapse all non-zero outcomes into the same
remediation. A `1` means the command successfully found a condition you asked
it to check; a `2` means the command itself did not complete correctly.
Atlas-compatible surfaces use `1` for both classes to match Atlas CE.

## Interrupts

SIGINT and SIGTERM cancel the running command instead of killing the process
where it stands. The difference is visible when a command holds something that
has to be given back -- a `docker://` dev database is the case that matters,
because a killed process leaves the container running with a copy of the schema
on a published port. Cancelation lets the release the command already defers
actually run.

The command therefore does not stop instantly, and says so: it writes
`interrupt received, releasing resources; interrupt again to stop immediately`
to stderr. A second interrupt reaches the default handler and ends the process
at once, which is the escape hatch for a command that is not watching its
context.

What it prints when it stops is `error: canceled`, whichever call noticed the
cancellation first. That matters because they all notice differently: a store
write answers `save run r-4: context canceled`, a connection pool answers
`driver: bad connection`, and a provider request answers whatever its transport
says — three sentences about one Ctrl-C, none of them about the interrupt.

A **deadline** is not a cancellation and keeps its own wording. A
`--provider-timeout` that expires is a fact about the endpoint, and reporting it
as canceled would take away the one word saying which. An interrupt that arrives
while a command is already failing for its own reason keeps that reason too.

## Diagnostic prefix

A *process-level diagnostic* is the single line a surface prints when a command
terminates with a failure: the line produced by Ptah's CLI error contract
(`cmdutil.Fail`, the Cobra flag-error and `RunE` wrappers, the post-execution
error normalizer, the compatibility tree's own printers, and the recovered
panic at the process boundary). Its prefix is punctuation owned by the surface,
not by the message, and the only input is which binary printed the line:

| Surface | Prefix | Exit code |
| --- | --- | --- |
| Native `ptah` | `error: ` | `2` |
| Compatibility `ptah-compat` | `Error: ` | `1` |

This holds for every diagnostic in that class, regardless of which package
produced the underlying error. In particular, a `ptah-compat` verb that
delegates to a native command still prints `Error: `, because the user invoked
the compatibility surface. If you grep stderr in a script, match the prefix of
the binary you actually run.

The rule covers the prefix only. The message text after it is decided per
diagnostic, and the two surfaces decide it differently. Where a cell of the
output-shape register pins a wording, `ptah-compat` matches the pinned community
binary byte for byte: `ptah-compat schema inspect` with no `--url` reports
`required flag(s) "url" not set`, and `ptah-compat migrate status` with none
reports `sql/sqlclient: missing driver. See: https://atlasgo.io/url`. Native
`ptah` keeps its own prose for the same mistake — `ptah migrations status`
reports `database URL is required` — because matching is a promise the
compatibility surface makes and the native tree does not.

Other stderr output is outside the class and keeps its own format. Report
bodies are the main case: the `Error:` field inside a `migrate status`
dirty-revision block, or inside the Atlas-format checksum report
`ptah-compat migrate validate` writes, is part of that report's format. So are
`warning: ` lines and progress logs, which do not terminate the command.

One report line currently reaches stderr with no prefix at all:
`ptah-compat migrate lint` writes a bare `checksum mismatch` before exiting `1`
when the directory does not match `atlas.sum` and no `--format` was given. That
line is the lint report's integrity finding rather than a process-level
diagnostic — with `--format` the same content is rendered into the report on
stdout — so the prefix rule does not reach it. Its stream is a known
divergence, not an endorsed format.

Internally the prefix is an inherited command-tree policy resolved at print
time by walking from the printing command up to the nearest ancestor that
declares one. Only the Atlas-compatible surface declares one, on its root
command next to its exit-code policy; the native tree declares nothing and
falls through to the default. Adding a command or a diagnostic therefore requires no prefix
decision. The nearest declaration wins, so a subtree *can* declare its own
prefix; Ptah's trees do not, but a `ptah-compat` verb that forwards to a native
command relies on it, because the forwarded command runs detached from the
compatibility tree and is handed the surface's prefix for the duration of the
call.

## Native commands

The grouped command tree is the native Ptah surface. Ptah is pre-GA, so old
root-level command spellings are removed instead of preserved.

| Command | `0` | `1` | `2` |
| --- | --- | --- | --- |
| `ptah introspect` | Annotated Go model files generated. | Not used. | Usage error, invalid output path, connection failure, schema-read failure, render error, or write error. |
| `ptah schema render` | Schema rendered. | Not used. | Usage error, parse error, unsupported dialect, or render error. |
| `ptah schema export` | Schema exported. | Not used. | Usage, path, parse, render, write, cleanup, empty-export, or Protobuf compatibility failure. See details below. |
| `ptah viz` | Schema diagram rendered. | Not used. | Usage error, invalid paths, parse error, unsupported format/theme, missing Graphviz for SVG, SVG render error, or write error. |
| `ptah db read` | Schema read and printed. | Not used. | Usage error, connection failure, or schema-read failure. |
| `ptah db capabilities` | Capability profile printed as text or JSON. | Not used. | Usage error (missing `--db-url`, an invalid `--format` value, an unparsable `--connect-timeout`, or an unknown flag), or connection failure. |
| `ptah db drop-all` | Objects dropped, dry-run output printed, or operation canceled by the user. | Not used. | Usage error, connection failure, input read error, or drop failure. |
| `ptah schema compare` | Diff printed, or no diff. | Non-empty diff when `--exit-code` is set. | Usage error, connection failure, parse failure, or diff generation failure. |
| `ptah schema drift` | No drift that meets `--severity`, or `--exit-code=false`. | Drift meets `--severity` while `--exit-code=true`. | Usage error, connection failure, parse failure, or report error. |
| `ptah schema diff` | Diff printed, or no diff. | Not used. | Usage error, source failure, invalid selector, an explicit include selection matching neither side, or diff generation failure. |
| `ptah schema plan` | Plan saved, or the database already matches the desired schema and no file is written. | Not used. | Usage error, connection failure, parse failure, safety check failure, or plan write failure, including an `--output` directory that does not exist. |
| `ptah schema apply` | Schema applied, dry-run output printed, or the operation canceled at the confirmation prompt. | Not used. | Usage error, connection failure, parse failure, dev-database rehearsal failure, lock failure, a `--plan` whose fingerprint no longer matches the database, a `--require-approval` plan carrying no verified approval, or apply failure. |
| `ptah schema approve` | Plan signed and the signature written beside it. | Not used. | Usage error, unreadable plan, or an `ssh-keygen` signing failure. |
| `ptah schema verify-approval` | Approval verifies, and the principal it belongs to is printed. | Not used. | Usage error, unreadable plan, a plan with no signature file, or an approval that does not verify against the allowed-signers list. |
| `ptah schema fmt` | Every `.hcl` file is canonically formatted, or the files that were not have been rewritten. | Not used. | Usage error, an unreadable path, a file that does not parse as HCL, a write error, or `--check` finding files that are not canonically formatted. |
| `ptah schema lineage` | Lineage printed, including when views landed under `undecided`. | Not used. | Usage error, source read failure, or parse failure. |
| `ptah schema stats` | Object counts printed as OpenMetrics. | Not used. | Usage error, including a missing `--db-url`; connection failure; or schema-read failure. |
| `ptah schema security` | No finding meets `--fail-on`, or `--fail-on=none`. | Findings meet `--fail-on`. | Usage error, connection failure, an unreadable `--role-usage` file, or report error. |
| `ptah schema serve` | Not used. The process serves until it is interrupted, and an interrupt exits `130`. | Not used. | Usage error, a missing database URL, an invalid project configuration in the working directory, or a listen address that cannot be bound. |
| `ptah schema validate` | No structural problem found in any target. | One or more structural problems found; each is printed on its own line. | Usage error (no `--dialect`, no `--root-dir` or `--schema-file`, or an unknown flag). |
| `ptah schema test` | Every schema test case passed. | One or more cases failed. | Usage error (including two desired-schema selectors at once, a database source whose dialect differs from `--db-url`, or a non-SQLite database source with no `--db-url`), invalid or unreadable cases, connection failure, interrupted run, desired-schema parse/apply failure, or report error. |
| `ptah migrations lint` | No findings above `--fail-on`, or `--fail-on=none`. | Findings meet `--fail-on`. | Usage error, invalid config, unreadable migration directory, dev-database connection failure, SQL replay failure, or report error. |
| `ptah migrations test` | Every migration test case passed. | One or more cases failed. | Usage error, invalid or unreadable cases, connection failure, interrupted run, migration/schema setup failure, or report error. |
| `ptah sql lint` | No SQL lint findings with `error` severity. | One or more SQL lint findings with `error` severity. | Usage error, unreadable SQL input, unsupported dialect, a `--version` value that names no server, or report error. |
| `ptah migrations plan` | Migration SQL generated, or no schema changes. | Not used. | Usage error, connection failure, parse failure, safety check failure, or render error. |
| `ptah migrations generate` | Migration file generated, or no migration needed. | Not used. | Usage error, connection failure, parse failure, shadow verification failure, safety check failure, or write error. |
| `ptah migrations create` | Empty migration files created. | Not used. | Usage error, invalid directory, or write error. |
| `ptah migrations baseline` | Existing migrations recorded as applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, verification failure, or write error. |
| `ptah migrations up` | Pending migrations applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, integrity verification failure, lint/safety gate failure, pre-migration check failure, pre-flight hook failure, lock failure, or migration execution failure. |
| `ptah migrations down` | Requested rollback applied, or dry-run output printed. | Not used. | Usage error, connection failure, migration directory error, pre-flight hook failure, lock failure, or rollback failure. |
| `ptah migrations repair` | Migration revision repaired, or dry-run output printed. | Not used. | Usage error, connection failure, revision lookup failure, or repair failure. |
| `ptah migrations status` | Status printed, including pending migrations by default. | Pending migrations exist when `--exit-code` is set. | Usage error, connection failure, migration directory error, or status-read failure. |
| `ptah migrations ls` | Migration files listed, or nothing printed when the directory holds none. | Not used. | Usage error, invalid directory, invalid migration format, or integrity verification failure under `--verify-sum`. |
| `ptah migrations show` | Migration SQL printed. | Not used. | Usage error, invalid directory or version, a version the directory does not hold, a version with no migration in the requested direction, or integrity verification failure under `--verify-sum`. |
| `ptah migrations hash` | Integrity file written. | Not used. | Usage error, invalid directory, invalid migration format, or write error. |
| `ptah migrations validate` | Integrity file matches the migration directory, and optional `--dev-url` SQL replay succeeds. | Migration content drift found. | Usage error, missing or unreadable integrity file, invalid directory, invalid migration format, dev-database connection failure, or SQL replay failure. |
| `ptah migrations edit` | Migration edited and the integrity file rewritten. | Not used. | Usage error, invalid directory or version, missing migration, refused because already applied without `--force`, database connection failure, editor failure, or write error. |
| `ptah migrations rebase` | Migration re-timestamped to the end of history and the integrity file rewritten. | Not used. | Usage error, invalid directory or version, missing migration, already last, refused because already applied without `--force`, database connection failure, or write error. |
| `ptah migrations rm` | Migration deleted and the integrity file rewritten. | Not used. | Usage error, invalid directory or version, missing migration, refused because already applied without `--force`, database connection failure, or write error. |
| `ptah inference describe` | What the specification says, printed as text or JSON. | Not used. | Usage error, no `--spec` or `--release`, an unreadable or invalid specification, an unreachable release, or a release whose specification is not the one it records. |
| `ptah inference probe` | Every provider check passed. | One or more provider checks did not pass. | Usage error, an invalid `--format` value, no `--spec` or `--release`, an unreadable specification, or a credential reference that cannot be resolved. |
| `ptah inference plan` | Plan printed, including a plan that is blocked. | Not used. | Usage error, connection failure, specification failure, schema-read failure, or a record that could not be left where `--publish-evidence` or `--evidence-file` named. |
| `ptah inference prepare` | Target column, run state and outbox created, or already there. | Not used. | Usage error, connection failure, specification failure, or a create failure. |
| `ptah inference backfill` | Source embedded into the generation, or the run resumed and finished. | Not used. | Usage error, connection failure, lease loss, provider failure, or target write failure. |
| `ptah inference catchup` | Source changes processed into the generation. | Not used. | Usage error, connection failure, a consistency mode that records no boundary, provider failure, or target write failure. |
| `ptah inference index` | Vector index built and valid, or the specification declares none. | Not used. | Usage error, connection failure, or an index build that failed or left an invalid index. |
| `ptah inference abandon` | Run permanently ended without deleting its generation or vectors. | Not used. | Usage error, no reason given, connection failure, a complete run, or an abandonment that would leave an active or maintained generation without another usable live feeder; an outbox feeder needs a durable resume position. |
| `ptah inference pause` | Run paused at its last checkpoint. | Not used. | Usage error, no reason given, connection failure, or a run that cannot be paused from its current state. |
| `ptah inference resume` | Paused run returned to running. | Not used. | Usage error, connection failure, or a run that is not paused. |
| `ptah inference verify` | Every deterministic layer passed. | One or more blocking findings. | Usage error, connection failure, or a read failure. |
| `ptah inference evaluate` | Retrieval measured and within every configured tolerance. | A configured retrieval tolerance was exceeded, or a required case was not answered. | Usage error, connection failure, an unreadable corpus, or a provider failure. |
| `ptah inference status` | Status printed, including a generation that is not ready. | The generation is not verified and cutover-ready when `--require-ready` is set. | Usage error, an invalid `--format` value, connection failure, or a read failure. |
| `ptah inference cutover` | Pointer moved to the generation. | Not used. | Usage error, connection failure, a refused cutover, a missing or mismatched approval, or a pointer move that failed. |
| `ptah inference rollback` | Pointer moved back to the previous generation. | Not used. | Usage error, connection failure, a previous generation that is not eligible, or a pointer move that failed. |
| `ptah inference retire` | Generation destroyed. | Not used. | Usage error, connection failure, a refused retirement, a missing or mismatched approval, or a drop failure. |
| `ptah seed` | Seed files applied or already applied. | Not used. | Usage error, protected environment rejection, connection failure, invalid seed files, or seed execution failure. |
| `ptah version` | Version information printed. | Not used. | Usage error. |

### `ptah schema export`

Exit code `2` covers invalid paths; parse, render, write, and cleanup failures;
Go input with no annotations or no exportable HCL objects; and Protobuf
compatibility refusals. Protobuf export refuses previous output that is foreign,
modified, malformed, package-mismatched, or written by an unsupported export
version. It also refuses unresolved type-removal, incompatible-change, and
name-reuse policy violations.

## Atlas-compatible command surfaces

The Atlas-compatible commands live in the separate `ptah-compat` binary, the
drop-in Atlas replacement; invocations below use the `ptah-compat` binary
name. They either translate implemented Atlas-compatible flags and
delegate to the matching native command, or execute Ptah-owned Atlas-shaped
behavior such as migration apply, the license notice, or schema formatting.

| Atlas-compatible command | Behavior |
| --- | --- |
| `ptah-compat version` | `ptah version` |
| `ptah-compat license` | Ptah license notice |
| `ptah-compat migrate apply` | Atlas-format apply path equivalent to `ptah migrations up` |
| `ptah-compat migrate down` | Non-interactive rollback through the same engine as `ptah migrations down`; with `--format`, an Atlas Go-template down report (no confirmation prompt, same success/failure codes) |
| `ptah-compat migrate diff` | Atlas-style migration diff from a supported desired schema source, `atlas.sum` update, or dry-run output printed |
| `ptah-compat migrate import` | Import local migrations into a separate directory and write `atlas.sum` |
| `ptah-compat migrate status` | Atlas-format migration status with Atlas revision-table metadata |
| `ptah-compat migrate hash` | `ptah migrations hash` |
| `ptah-compat migrate validate` | Atlas-format integrity validation with Atlas checksum diagnostics |
| `ptah-compat migrate lint` | `ptah migrations lint` |
| `ptah-compat migrate checkpoint` | `ptah migrations checkpoint` |
| `ptah-compat migrate test` | `ptah migrations test` |
| `ptah-compat migrate edit` | `ptah migrations edit` |
| `ptah-compat migrate rebase` | `ptah migrations rebase` |
| `ptah-compat migrate rm` | `ptah migrations rm` |
| `ptah-compat migrate ls` | `ptah migrations ls`, with the Atlas checksum gate always on |
| `ptah-compat migrate show` | `ptah migrations show`, with the Atlas checksum gate always on |
| `ptah-compat schema inspect` | Atlas-shaped schema inspection |
| `ptah-compat schema apply` | Local Atlas-style schema apply |
| `ptah-compat schema diff` | Local Atlas-style schema-file diff |
| `ptah-compat schema fmt` | Format local `.hcl` files |
| `ptah-compat schema test` | `ptah schema test` |
| `ptah-compat schema validate` | `ptah schema validate` |
| `ptah-compat schema plan` | Local Atlas-style plan computation saved to a fingerprinted plan file; `new`, `validate` and `lint` are implemented, the registry sub-verbs plus `test` stay boundary stubs. `lint` exits 0 with findings reported unless `PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR=1` makes an error-severity finding exit 1 |
| `ptah-compat migrate push` | Registered but not implemented boundary command |
| `ptah-compat schema push` | Registered but not implemented boundary command |

`ptah-compat` reports its version through the `version` command only. It
deliberately carries no `--version`/`-v` flag — the command set it mirrors
carries neither, and both spellings are rejected as unknown flags with exit 1.
The native `ptah` binary does accept all three spellings and prints the same
block for each.

These registered boundary commands use Ptah-owned diagnostics: `--help` reports
that the command is not implemented and exits `0`, while direct execution
reports the same status and exits `1`. All other reported failures on
the `ptah-compat` binary, including unsupported flags, malformed input,
missing files, configuration errors, and database failures, also exit `1`.
This normalization applies only to the compatibility tree; equivalent native
Ptah command failures keep exit code `2`.

An unknown root command exits `1` and writes Atlas's `unknown command`
diagnostic plus `atlas --help` guidance to stderr. Atlas CE treats an extra
token under the `migrate` or `schema` command group differently: it prints that
group's help to stdout and exits `0`. Both compatibility surfaces preserve this
distinction. The same group behavior applies to `completion`; an extra token
after a concrete shell command exits `1` with Atlas's leaf-command diagnostic.

Successful `migrate validate` runs are silent, including successful
`--dev-url` SQL replay. Checksum mismatches exit `1`, write Atlas's recovery
guidance to stdout, and write `Error: checksum mismatch` to stderr. A missing
`atlas.sum` uses the same stdout guidance and writes
`Error: checksum file not found` to stderr. Entry-level drift also identifies
the first mismatched `atlas.sum` line, file, and reason. The native
`ptah migrations validate` command keeps its own success and error diagnostics:
malformed or missing sum files are usage failures with exit `2`, while valid
integrity drift exits `1` with Ptah's native drift report.

`migrate apply`, `migrate status`, and `migrate set` refuse the same two
directory states with the same exit code and the same output, before they open
the target database: a mismatched `atlas.sum` and a missing one
([#974](https://github.com/stokaro/ptah/issues/974) extended the gate from
`apply` alone to all three).

- On `migrate set` the refusal also precedes the positional-version check, so a
  wrong argument count on an unverified directory still reports the checksum
  error.
- The missing-file refusal requires at least one `.sql` file anywhere in the
  directory tree; an empty or `.gitkeep`-only directory exits `0` with
  `No migration files to execute`.
- Directories read through `?format=` are gated the same way on `migrate apply`,
  over the file set Atlas covers for that layout, so a golang-migrate down file
  and a Flyway undo file are outside the check and a layout that carries no
  `atlas.sum` and whose covered set is empty is not refused. A hashed directory
  whose covered set is empty is still verified, and a drifted one exits `1`.
- `migrate lint` is deliberately not gated, on either tool.

Native `ptah migrations up` verifies a hashed directory the same way (exit `2`
with Ptah's drift report) but applies a never-hashed directory unless
`--verify-sum` is passed.

Docs CI checks these tables against the repository exit-code contract.
