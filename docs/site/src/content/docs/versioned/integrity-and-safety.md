---
title: Integrity and safety
description: Hash and validate the migration directory, replay it on a dev database, lint for unsafe SQL, and gate destructive changes.
---

You are about to let migrations touch a database other people depend on. This
page shows the gates Ptah puts between a migration directory and that
database: the integrity file, replay validation, the lint rules, the
destructive-change gate, and pre-migration assertion checks — and what each
one's failure looks like.

Prerequisites: a migration directory (see
[Generate migrations](../generate/)). The examples use fixed-version manual
migrations and a local SQLite file.

## The integrity file

`ptah migrations hash` writes `ptah.sum`: a hash of every migration file,
committed alongside them.

```bash
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
```

Expected output includes:

```text
Wrote ./migrations/ptah.sum
4 migration file(s) hashed
OK: migrations directory matches ptah.sum
```

The file lists a directory-level hash and one line per migration file:

```text
h1:tIMjgi/Ua3SltHhtGO6BksGhtMonFkeLmHl0+fM8p/o=
0000000001_init.down.sql h1:iwBWserNbFegc3M+AZUbJlv00afJyT9smM0ezgVpk4o=
0000000001_init.up.sql h1:5Lxcm8LHBcS4SgvVBg/frPdzEflRbvdjWh5UOSDATiM=
```

Atlas-format directories use `atlas.sum` the same way.

## Detect out-of-band edits

Any change to a hashed file — a hotfix applied in place, a merge gone wrong —
makes validation fail and name the file (exit `1`):

```bash
ptah migrations validate --dir ./migrations
```

```text
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

The same drift blocks `ptah migrations up` on any hashed directory — the
apply verifies `ptah.sum` or `atlas.sum` before executing anything, so a
tampered migration never runs (exit `2`):

```text
error: migration sum verification failed:
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

`ptah-compat migrate apply`, `migrate status`, and `migrate set` enforce the
same gate on `atlas.sum` directories with Atlas's own checksum output, matching
official Atlas behavior. Reporting is not exempt: on a hashed directory whose
only migration was deleted, an ungated `migrate status` announced
"Database is up to date"
([#974](https://github.com/stokaro/ptah/issues/974)).

Recovery is a decision, not a command: if the change is intentional, review
it and re-run `ptah migrations hash`; if it is not, restore the file from
version control. Use `git diff` on the migration directory to tell the two
apart. Run `validate` in CI and hash every shared directory so drift is
caught at review time, not at deploy time.

## A directory that was never hashed

Drift is one question; "there is no sum file at all" is a different one, and
the two surfaces answer it differently on purpose.

**Native `ptah migrations up` applies an unhashed directory.** Hashing is
opt-in on this surface: a directory with no `ptah.sum` or `atlas.sum` has
never claimed integrity, so demanding one would break every project that has
not adopted `ptah migrations hash` yet. Turn the missing file into an error
where that matters — in CI, and on deploys of directories you do hash — with
`--verify-sum` (exit `2`):

```bash
ptah migrations up \
  --db-url "sqlite://app.db" \
  --migrations-dir ./migrations \
  --verify-sum
```

```text
error: migration sum verification failed: ptah.sum not found; run `ptah migrations hash` to create it
```

That flag is the only thing on the native surface that rejects a never-hashed
directory; a hashed one is always verified with or without it.

**`ptah-compat migrate apply`, `migrate status` and `migrate set` refuse an
unhashed directory.** Atlas treats a missing `atlas.sum` as a checksum error, so
the compatibility surface does too — measured against the pinned community
binary, which exits `1` and never creates the target database:

```text
You have a checksum error in your migration directory.
Please check your migration files and run 'atlas migrate hash' to re-hash the contents

Error: checksum file not found
```

Nothing executes and the target is never opened, exactly as with a checksum
mismatch. Run `ptah-compat migrate hash` once and commit the `atlas.sum` it
writes. A directory that holds no `.sql` file anywhere in its tree — a freshly
created or `.gitkeep`-only migrations directory — is not a checksum error: it
reports `No migration files to execute` and exits `0`, matching Atlas.

That scan reads the top level only, because an Atlas migration directory
executes exactly what its `atlas.sum` covers: top-level files whose name ends in
`.sql`, spelled in lower case. A `.sql` file in a subdirectory, or a top-level
`.SQL`, is not a migration and is not run
([#976](https://github.com/stokaro/ptah/issues/976)).

That rule replaced a recursive one. Ptah used to discover and execute nested
files while hashing only the top level, so a migration in a subdirectory ran
with no checksum reaching it: `migrate validate` reported the directory clean,
and editing that file afterwards changed what ran without changing any hash.
Converging the executed set onto the covered set is what makes `atlas.sum` mean
something; refusing such directories, which is what the previous release did for
the unhashed half only, never closed the hashed half at all.

Ptah names what it declined. A file it found but did not treat as a migration is
reported on stderr:

```text
warning: sub/2_b.sql is not covered by atlas.sum and will not run; Atlas migrations are top-level files named *.sql
```

Atlas prints nothing here, and that silence is the reason Ptah does not copy it:
a directory whose only migration sits one level down hashes zero files,
validates clean, and applies nothing at exit `0`, so a migration you committed
never runs and no output says so. Exit codes and stdout stay identical to Atlas;
only this stderr line is added. Move the file to the top level to have it run.

:::caution[Breaking change]
A project that relies on Atlas-format migrations in subdirectories will stop
executing them. The warning above names every affected file on each run.
:::

Native `ptah migrations` commands apply the same selection under
`--dir-format atlas`, which writes an `atlas.sum` and so is bound by the same
coverage. Auto mode without an `atlas.sum` keeps reading subdirectories and
stays self-consistent, because `ptah.sum` is computed from the same discovery
and covers every file it finds. The declined-file warning is currently printed
by the `ptah-compat` verbs only.

**`migrate lint` is deliberately not gated**, on either tool: inspecting a
directory that has drifted is the point of linting it. `migrate new` and
`migrate diff` are gated on Atlas but not yet here — they write an `atlas.sum`
over a directory whose previous contents were never verified, which turns drift
into apparent cleanliness. Verify with `ptah-compat migrate validate` before
generating into a directory you did not hash yourself.

**Directories read through `?format=` are gated too.** A goose, flyway,
liquibase, dbmate or golang-migrate directory is converted in memory and the
converted filesystem carries no integrity file — but the directory it was read
*from* carries `atlas.sum` beside its own migrations, and that is what is
verified, before the source layout is parsed and before the database is opened.
Run `ptah-compat migrate hash --dir 'file://migrations?format=goose'` once and
commit the file. The same applies when the layout comes from `atlas.hcl`
(`migration { format = goose }`) rather than from the URL.

Each layout is verified over the file set Atlas covers for it, which is not
always every `.sql` file:

| Layout | Covered by `atlas.sum` |
| --- | --- |
| `atlas`, `goose`, `dbmate`, `liquibase` | every top-level `*.sql` |
| `golang-migrate` | every top-level `*.up.sql` — the down file is never covered |
| `flyway` | `V`/`B`/`R` files anywhere in the tree; `U` undo files and everything a baseline squashes are dropped |

So editing a golang-migrate down file or a Flyway undo file is invisible to the
check, exactly as it is in Atlas. A directory that carries no `atlas.sum` and
whose covered set is empty — a golang-migrate directory holding only a down
file, say — is not a checksum error and is not refused.

:::caution[A **directory** whose name matches is covered too, and cannot be hashed]
Membership in the covered set is decided by the name, so a *directory* called
`weird.sql` is a member for `atlas`, `goose`, `dbmate` and `liquibase`, and one
called `weird.up.sql` is a member for `golang-migrate`. Reading it fails, so
there is no sum to write and no sum to verify: `hash`, `validate`, `apply`,
`status`, `set` and `lint` all refuse the directory and exit `1`, matching
Atlas. Rename the directory or move it out of the migrations directory.

```text
$ ptah-compat migrate hash --dir 'file://migrations?format=goose'
Error: read file "weird.sql": is a directory, not a migration file; rename it or move it out of the migration directory
```

**This is a behavior change.** Releases up to v0.1.2 skipped such a directory
and wrote an `atlas.sum` over the remaining files — a file Atlas itself then
refused to read, so the directory looked hashed here and was rejected there
([#991](https://github.com/stokaro/ptah/issues/991)). A project with a
legitimately named `*.sql` directory that hashed cleanly before will now be
refused.

`flyway` is exempt on both tools, and not by special-casing: Atlas walks a
Flyway tree instead of globbing it, so a directory is a node it descends into
and never reads. A migration nested inside one is covered as usual.
:::

For every layout, "not covered by `atlas.sum`" also means "not executed": the
set `migrate apply` runs is the set the checksum it verified covers. Flyway was
the exception until [#982](https://github.com/stokaro/ptah/issues/982) — its
importer selected a wider set than Atlas hashes, so a superseded baseline or a
lowercase-prefixed file could run SQL no checksum protected. The importer and
the hasher now share one selection rule, so that class of gap cannot reopen
without a failing test.

:::note[Flyway baselines: Ptah runs one where Atlas sometimes does not]
Atlas CE decides which migrations are pending by comparing the version token as
a **string** against the highest version already recorded. A baseline whose
token does not sort above that mark is silently treated as applied and never
runs — measured on Atlas CE v1.2.0:

- `V2__x.sql` applied, then `B10__base.sql` added: `"10" < "2"` as strings, so
  Atlas reports `No migration files to execute` and the baseline never runs.
- `V1`, `V2`, `V3` applied, then `B2__base.sql` added (superseding `V1` and
  `V2`): same result.

Ptah decides pending-ness from the recorded set instead, so it **runs** the
baseline in both cases. Where a baseline is added deliberately, that is what was
intended; it is a divergence from Atlas either way, and which behavior
`ptah-compat` should keep is
[#1003](https://github.com/stokaro/ptah/issues/1003).

Both tools run a baseline added above the high-water mark, and both execute its
SQL rather than merely recording it — Atlas fails loudly if that SQL cannot
survive a second run.
:::

:::danger[Upgrading a Flyway directory applied by Ptah v0.1.0–v0.1.2]
Converging the importer changed the Atlas version each Flyway file converts
to, and that version is the key `atlas_schema_revisions` stores. A database
migrated through `?format=flyway` by any of those releases therefore records
migrations under versions this release matches to no file, so **every migration
in the directory reads as pending**.

`ptah-compat migrate apply` refuses such a database before executing anything
and prints the `UPDATE` statements that migrate the recorded versions forward:

```text
error: this database was migrated by a Ptah build older than the one that fixed
stokaro/ptah#982 ... 2 already-applied migration(s) would run a second time and
nothing has been applied

recorded version -> version this build uses:
  10000                -> 4611686018427469511  V1__init.sql
  20000                -> 4611686018427510315  V2__seed.sql

to adopt the new encoding, migrate the recorded versions forward and re-run:
  UPDATE atlas_schema_revisions SET version = '4611686018427469511' WHERE version = '10000';
  UPDATE atlas_schema_revisions SET version = '4611686018427510315' WHERE version = '20000';
```

Run them against the schema `--revisions-schema` selects, then re-run the
apply. Rewriting the version column is enough on its own: the recorded hash
covers the converted SQL body, which this change does not touch.

The statements have to be run by hand because `migrate set` and `migrate
status` do not yet accept `?format=`
([#1002](https://github.com/stokaro/ptah/issues/1002)), and `--baseline`
refuses once the revision table is non-empty.

Without the refusal this is not reliably loud: re-running a `CREATE TABLE`
fails and leaves a dirty revision, but re-running a backfill or a seed
succeeds, exits 0, and duplicates the rows.
:::

## Replay on a dev database

Hashes prove the files are unchanged, not that the SQL executes. Add
`--dev-url` to also clean a disposable
**[dev database](../../concepts/database-urls-and-dev-databases/)** and replay
the whole directory on it:

```bash
ptah migrations validate \
  --dir ./migrations \
  --dev-url "sqlite://replay.db"
```

Expected output includes:

```text
OK: migrations directory matches ptah.sum
OK: migration SQL validated on dev database
```

A migration that no longer executes — here one that alters a table dropped by
an earlier edit — fails the replay (exit `2`):

```text
error: error validating migration SQL on dev database: replay migration 3 on dev database: failed to execute migration SQL: sqlite: SQL execution failed: SQL logic error: no such table: missing (1)
SQL: ALTER TABLE missing ADD COLUMN nickname TEXT
```

The dev database is dropped clean on every run — point it at a scratch
database of the target engine, never at a real environment.

## Lint for production-unsafe SQL

`ptah migrations lint` analyzes migration files for patterns that are legal
SQL but dangerous in production — dropped tables and columns, lock-heavy
DDL, and dialect-specific hazards:

```bash
ptah migrations lint --dir ./migrations --dialect sqlite
```

A finding names the file, line, rule, and remediation (exit `1`):

```text
migrations/0000000003_drop_users.up.sql:1 [error] DS101: DROP TABLE permanently deletes the table and every row in it; take a verified backup first and consider a rename-and-retire window instead (table dropped)

1 finding(s).
```

A clean run prints `No lint findings.` and exits `0`.

Useful controls, all designed for CI:

- `--latest N` lints only the newest N versions — the changeset of a pull
  request rather than all of history. `--git-base <branch>` selects the
  changeset from Git instead.
- `--fail-on error` (default) fails only on error-severity findings;
  `--fail-on any` fails on warnings too; `--fail-on none` always exits `0`.
- `--format json`, `--format sarif`, and `--format github-actions` feed code
  scanners and PR annotations. The SARIF output is a SARIF 2.1.0 document
  that GitHub code scanning ingests — [CI](../../testing/ci/) shows the
  upload step.
- `--dialect` gates dialect-specific rules; `--dev-url` infers the dialect
  and additionally replays the directory on the dev database.
- `--disable DS101` (or a family such as `MY`) skips rules ad hoc; a
  committed `.ptah-lint.yaml` does it persistently and adds per-rule
  severity and path scoping — see below.

For OCI-distributed directories, `lint --dir oci://...` lints the published
artifact, and `--attach` stores the canonical report next to it — see
[OCI registry artifacts](../../operate/oci-registry/).

### Configure rules with `.ptah-lint.yaml`

Commit a `.ptah-lint.yaml` next to the migration files to make lint policy
part of the reviewed migration directory. `ptah migrations lint --config`
selects an explicit lint policy; without that flag, lint loads
`<dir>/.ptah-lint.yaml` when present:

```yaml
dialect: postgres
disabled-rules:
  - MF103
  - MY
rules:
  DS103:
    severity: warning
  DS102:
    severity: error
    exclude:
      - legacy/**
```

- `dialect` sets the default lint dialect; `--dialect` overrides it.
- `disabled-rules` lists rule codes (`DS101`) or family prefixes (`MY`) to
  skip entirely; entries merge with `--disable` flags. Selectors and custom
  rule codes use uppercase ASCII letters and digits and start with a letter.
- `rules` keys name an exact code or a family prefix; the most specific key
  wins, so a `DS102` entry beats a `DS` entry.
- `severity` accepts `warning` or `error` and replaces the rule's default
  severity on its findings — the level that `--fail-on` and the apply-time
  destructive gate below evaluate. Any other value fails config parsing
  (exit `2`).
- `exclude` lists slash-separated path globs (`**` crosses directory
  levels) where the rule is skipped. Prefer paths relative to the migration
  directory, such as `legacy/**`; these match regardless of how `--dir` was
  spelled. A directory-prefixed pattern such as `migrations/legacy/**`
  matches only when the command path has that prefix, for example
  `--dir migrations`, and need not match an absolute `--dir` path.

Configuration decoding is strict. Unknown keys, misspelled keys such as
`severty`, lowercase or whitespace-padded selectors, selectors that match no registered rule,
unsupported severities, and multiple YAML documents fail before linting or
migration execution instead of silently weakening policy.

Precedence: a rule listed in `disabled-rules` never runs, regardless of its
`rules` entry; then `exclude` skips the matching files; then `severity`
relabels the findings that remain. Per-rule entries apply to custom
analyzer codes the same way as to built-in ones — see
[Reusable components](../../extend/components/) for registering custom
rules from Go.

### Suppress a single statement inline

When one reviewed statement is acceptable but the rule should stay active
everywhere else, put a `ptah:nolint` comment directly above it:

```sql
-- ptah:nolint DS102
ALTER TABLE users DROP COLUMN archived_note;
```

The directive suppresses only the named rules, and only for the next
statement. List several codes separated by spaces or commas, name a family
(`DS`), or write a bare `-- ptah:nolint` to silence every rule for that one
statement. `-- atlas:nolint DS102` is accepted as an alias with the same
statement-scoped behavior, so a directory shared with Atlas tooling keeps
one set of directives. Atlas analyzer-name selectors (such as
`destructive`) and whole-file `atlas:nolint` headers take effect only on
the Atlas-compatible surface — see
[Atlas migrate commands](../../atlas/migrate-commands/).

## The destructive-change gate

Destructive statements require explicit policy at two points:

- **At generation**, `plan`/`generate --check-destructive` refuse to write
  destructive SQL without `--allow-destructive` — see
  [Generate migrations](../generate/).
- **At apply**, `ptah migrations up` refuses pending migrations that contain
  destructive statements (exit `2`):

```text
error: error running migrations: pending migrations contain destructive statements; rerun with --allow-destructive after review:
- 0000000003_drop_users.up.sql:1 DS101 error: DROP TABLE permanently deletes the table and every row in it; take a verified backup first and consider a rename-and-retire window instead
```

Use `--allow-destructive` only after the plan has been reviewed and the
rollback path is understood.

The apply-time gate always loads the conventional
`<migrations-dir>/.ptah-lint.yaml` and blocks on error-severity `DS`
data-safety findings. The `--config` flag on `ptah migrations up` selects
`ptah.yaml`; it does not select a lint policy. This distinction prevents a
project configuration path from silently replacing the policy shipped with
the migration directory.

The gate otherwise uses the same rule configuration and path matching as
`ptah migrations lint`. That makes the escape hatch proportional: a rule downgraded with
`severity: warning`, listed under `disabled-rules`, or excluded for a path
stops blocking exactly that reviewed pattern — a widening
`ALTER COLUMN ... TYPE` under `rules: {DS103: {severity: warning}}` applies
without `--allow-destructive` — while a `DROP TABLE` in another pending
file of the same batch still aborts the apply. `--allow-destructive`
remains the all-or-nothing per-run override rather than the only way past
a single warning-grade finding.

Lint-policy severities are `warning` and `error`. Generated safety reports use
the operational vocabulary `safe`, `warning`, and `destructive`; an `error`
lint assessment and a `destructive` safety assessment are both blocking.

Know the limits of this gate: the policy file is not tamper-evident.

`ptah.sum` hashes only the migration `*.sql` files, so `ptah migrations
validate` and `up --verify-sum` still pass when `.ptah-lint.yaml` is added or
edited out-of-band, and the apply prints no notice when the config suppresses
a destructive finding — a one-line `disabled-rules: [DS]` dropped next to the
migrations at deploy time disables this gate silently.

Loosening the gate is a visible committed change only if your process makes it
one: commit `.ptah-lint.yaml`, treat any edit to it as an edit to the gate in
review, and restrict writes to the deployed migration directory, because the
integrity file does not protect the policy the way it protects the SQL.

Local migration commands capture the migration directory before database
connection, checksum verification, provider registration, and destructive
linting. `up`, `down`, `status`, `lint`, and `set` therefore use the same
immutable SQL and metadata bytes throughout one invocation. Each command
compares two captures and aborts only when the observed captures differ.

This best-effort check cannot defeat coordinated writers or ABA changes that
restore the original bytes before the next observation. Hostile writers
require trusted immutable input, manifest or process controls, or
filesystem-level snapshots. Relative CLI directories are rooted at the working
directory and symlink escapes are rejected, while explicit absolute paths
remain supported.

A contained relative `atlas.hcl` migration directory remains bound to the
project directory handle opened for config evaluation until capture completes;
replacing the project pathname does not retarget it. Parent-relative project
directories are external compatibility paths and do not inherit that root.

## Pre-migration checks

Guard a migration on a data-state precondition with a `-- +ptah check`
directive, which runs before the migration's statements and aborts if the
assertion fails:

```sql
-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort
DROP TABLE users;
```

Each check is a separate read against committed state that runs before the
migration's statements, so a failing assertion leaves nothing applied and
exits non-zero. Checks are rejected under `--tx-mode all` on a real apply (a
pooled read cannot see the batch's uncommitted state), and
`ptah migrations up --skip-checks` is an emergency bypass. On the
Atlas-compatible surface that bypass is spelled `PTAH_SKIP_CHECKS=1`, because
Atlas registers no `--skip-checks` on `migrate apply` and `ptah-compat` adds no
flags Atlas does not have. This is the open, local half of Atlas Pro's
pre-migration checks; the Cloud approval-policy half is intentionally out of
scope.

### Checks in a dry run

A check is a read, and a dry run intercepts only writes. Evaluating every
check in a preview would therefore ask each migration's guard about state that
only exists once its predecessors apply — state the dry run has, by
construction, refused to produce. The answer would be a fact about the preview,
not about the migrations.

So a dry run evaluates a migration's assertions only where the state it
observes is the state a real apply would evaluate them against: **the first
migration executed in the run**. That is a position in the run, not a version
and not a place in the directory — a migration sitting second in its directory
is first in the run once its predecessor is applied, and its checks are
evaluated normally from then on.

Every check is still parsed and statically validated wherever it sits. A
malformed `-- +ptah check` directive, or an assertion that is not a single
read-only `SELECT`, is decided by its text alone and is reported in a dry run
exactly as on a real apply. Only the database evaluation is deferred, and the
run names what it deferred on stderr:

```text
Deferred pre-migration checks for 1 migration (20260101000002): a dry run does
not create the state they assert on, so they are evaluated on apply.
```

A dry run under `--tx-mode all` follows the same rule rather than refusing the
directory: it opens no batch transaction, so the uncommitted-state problem that
motivates the refusal on a real apply does not arise.

## Exit codes are the contract

Every gate above communicates through the CLI exit code — `0` clean, `1` for
an expected negative result (drift found, findings found), `2` for errors
and refused operations — so CI wiring is a matter of checking `$?`. The full
per-command table is in [Exit codes](../../reference/exit-codes/).

## Next steps

- Wiring these gates into pull requests? [CI](../../testing/ci/).
- A gate fired and the migration needs rework?
  [Maintain migration history](../maintain-history/).
- Asserting behavior rather than safety?
  [Test migrations and schemas](../../testing/migrations-and-schema/).
